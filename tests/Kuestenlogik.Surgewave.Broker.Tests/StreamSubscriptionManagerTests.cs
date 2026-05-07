using Kuestenlogik.Surgewave.Broker.Native.Streaming;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Unit tests for StreamSubscriptionManager: subscription lifecycle, limits, and concurrency.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class StreamSubscriptionManagerTests : IAsyncLifetime, IDisposable
{
    private readonly string _testDirectory;
    private readonly LogManager _logManager;
    private readonly RecordBatchSerializer _serializer;

    public StreamSubscriptionManagerTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"surgewave-stream-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDirectory);

        _logManager = new LogManager(_testDirectory, new MemoryLogSegmentFactory());
        _serializer = new RecordBatchSerializer(NullLogger<RecordBatchSerializer>.Instance);
    }

    public async ValueTask InitializeAsync()
    {
        await _logManager.CreateTopicAsync("test-topic", partitionCount: 3, replicationFactor: 1);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public void Dispose()
    {
        _logManager.Dispose();
        if (Directory.Exists(_testDirectory))
        {
            try { Directory.Delete(_testDirectory, recursive: true); }
            catch { /* best-effort cleanup */ }
        }
    }

    private StreamSubscriptionManager CreateManager(int maxSubscriptions = 100)
        => new StreamSubscriptionManager(_logManager, _serializer, NullLogger.Instance, maxSubscriptions);

    private static Task NoOpSendDelegate(string subId, int partition, long hwm, int count, ReadOnlyMemory<byte> data, CancellationToken ct)
        => Task.CompletedTask;

    // ─── Subscribe / Unsubscribe lifecycle ────────────────────────────────────

    [Fact]
    public async Task Subscribe_NewSubscription_ReturnsTrue()
    {
        await using var manager = CreateManager();

        var result = manager.Subscribe("sub-1", "test-topic",
            partitions: [0],
            initialOffsets: [0],
            maxBytesPerPush: 65536,
            sendDelegate: NoOpSendDelegate);

        Assert.True(result);
    }

    [Fact]
    public async Task Subscribe_NewSubscription_IncrementsActiveCount()
    {
        await using var manager = CreateManager();

        manager.Subscribe("sub-1", "test-topic", [0], [0], 65536, NoOpSendDelegate);

        Assert.Equal(1, manager.ActiveCount);
    }

    [Fact]
    public async Task Unsubscribe_ExistingSubscription_ReturnsTrueAndDecrementsCount()
    {
        await using var manager = CreateManager();
        manager.Subscribe("sub-1", "test-topic", [0], [0], 65536, NoOpSendDelegate);

        var removed = await manager.UnsubscribeAsync("sub-1");

        Assert.True(removed);
        Assert.Equal(0, manager.ActiveCount);
    }

    [Fact]
    public async Task Unsubscribe_NonExistentSubscription_ReturnsFalse()
    {
        await using var manager = CreateManager();

        var removed = await manager.UnsubscribeAsync("does-not-exist");

        Assert.False(removed);
    }

    [Fact]
    public async Task Subscribe_DuplicateId_ReturnsFalse()
    {
        await using var manager = CreateManager();
        manager.Subscribe("sub-1", "test-topic", [0], [0], 65536, NoOpSendDelegate);

        var result = manager.Subscribe("sub-1", "test-topic", [0], [0], 65536, NoOpSendDelegate);

        Assert.False(result);
        Assert.Equal(1, manager.ActiveCount); // only one subscription exists
    }

    // ─── Max subscriptions enforcement ───────────────────────────────────────

    [Fact]
    public async Task Subscribe_AtLimit_ReturnsFalse()
    {
        await using var manager = CreateManager(maxSubscriptions: 2);

        manager.Subscribe("sub-1", "test-topic", [0], [0], 65536, NoOpSendDelegate);
        manager.Subscribe("sub-2", "test-topic", [1], [0], 65536, NoOpSendDelegate);

        var result = manager.Subscribe("sub-3", "test-topic", [2], [0], 65536, NoOpSendDelegate);

        Assert.False(result);
        Assert.Equal(2, manager.ActiveCount);
    }

    [Fact]
    public async Task Subscribe_AfterUnsubscribeFreesSlot_Succeeds()
    {
        await using var manager = CreateManager(maxSubscriptions: 1);
        manager.Subscribe("sub-1", "test-topic", [0], [0], 65536, NoOpSendDelegate);
        await manager.UnsubscribeAsync("sub-1");

        var result = manager.Subscribe("sub-2", "test-topic", [0], [0], 65536, NoOpSendDelegate);

        Assert.True(result);
        Assert.Equal(1, manager.ActiveCount);
    }

    // ─── UnsubscribeAll cleanup ───────────────────────────────────────────────

    [Fact]
    public async Task UnsubscribeAll_RemovesAllSubscriptions()
    {
        await using var manager = CreateManager();
        manager.Subscribe("sub-1", "test-topic", [0], [0], 65536, NoOpSendDelegate);
        manager.Subscribe("sub-2", "test-topic", [1], [0], 65536, NoOpSendDelegate);
        manager.Subscribe("sub-3", "test-topic", [2], [0], 65536, NoOpSendDelegate);

        await manager.UnsubscribeAllAsync();

        Assert.Equal(0, manager.ActiveCount);
    }

    [Fact]
    public async Task UnsubscribeAll_EmptyManager_DoesNotThrow()
    {
        await using var manager = CreateManager();

        await manager.UnsubscribeAllAsync(); // Should not throw

        Assert.Equal(0, manager.ActiveCount);
    }

    [Fact]
    public async Task UnsubscribeAll_SubsequentSubscribe_Succeeds()
    {
        await using var manager = CreateManager();
        manager.Subscribe("sub-1", "test-topic", [0], [0], 65536, NoOpSendDelegate);
        await manager.UnsubscribeAllAsync();

        var result = manager.Subscribe("sub-new", "test-topic", [0], [0], 65536, NoOpSendDelegate);

        Assert.True(result);
        Assert.Equal(1, manager.ActiveCount);
    }

    // ─── AddCredit (StreamAck flow control) ──────────────────────────────────

    [Fact]
    public async Task AddCredit_ExistingSubscription_ReturnsTrue()
    {
        await using var manager = CreateManager();
        manager.Subscribe("sub-1", "test-topic", [0], [0], 65536, NoOpSendDelegate);

        var result = manager.AddCredit("sub-1", 1024 * 1024);

        Assert.True(result);
    }

    [Fact]
    public async Task AddCredit_NonExistentSubscription_ReturnsFalse()
    {
        await using var manager = CreateManager();

        var result = manager.AddCredit("no-such-sub", 1024);

        Assert.False(result);
    }

    // ─── Concurrent subscribe from multiple threads ───────────────────────────

    [Fact]
    public async Task ConcurrentSubscribe_UniqueIds_AllSucceed()
    {
        await using var manager = CreateManager(maxSubscriptions: 50);
        const int threadCount = 20;

        var results = await Task.WhenAll(
            Enumerable.Range(0, threadCount).Select(i =>
                Task.Run(() =>
                {
                    var subId = $"concurrent-sub-{i}";
                    return manager.Subscribe(subId, "test-topic", [0], [0], 65536, NoOpSendDelegate);
                })
            )
        );

        Assert.Equal(threadCount, results.Count(r => r));
        Assert.Equal(threadCount, manager.ActiveCount);
    }

    [Fact]
    public async Task ConcurrentUnsubscribe_OnlyOneSucceeds()
    {
        await using var manager = CreateManager();
        manager.Subscribe("shared-sub", "test-topic", [0], [0], 65536, NoOpSendDelegate);

        var results = await Task.WhenAll(
            Enumerable.Range(0, 5).Select(_ =>
                manager.UnsubscribeAsync("shared-sub")
            )
        );

        Assert.Equal(1, results.Count(r => r)); // exactly one task removed it
        Assert.Equal(0, manager.ActiveCount);
    }

    // ─── Multi-partition subscription ────────────────────────────────────────

    [Fact]
    public async Task Subscribe_MultiplePartitions_ActiveCountIsOne()
    {
        await using var manager = CreateManager();

        manager.Subscribe("multi-sub", "test-topic",
            partitions: [0, 1, 2],
            initialOffsets: [0, 0, 0],
            maxBytesPerPush: 65536,
            sendDelegate: NoOpSendDelegate);

        // Still counted as a single subscription regardless of partition count
        Assert.Equal(1, manager.ActiveCount);
    }

    // ─── DisposeAsync cleanup ─────────────────────────────────────────────────

    [Fact]
    public async Task DisposeAsync_StopsAllSubscriptions()
    {
        var manager = CreateManager();
        manager.Subscribe("sub-1", "test-topic", [0], [0], 65536, NoOpSendDelegate);
        manager.Subscribe("sub-2", "test-topic", [1], [0], 65536, NoOpSendDelegate);

        await manager.DisposeAsync();

        Assert.Equal(0, manager.ActiveCount);
    }
}
