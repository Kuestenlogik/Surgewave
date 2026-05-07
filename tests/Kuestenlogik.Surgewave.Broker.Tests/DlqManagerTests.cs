using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

[Trait("Category", TestCategories.Unit)]
public class DlqManagerTests : IAsyncLifetime, IDisposable
{
    private readonly DlqManagerConfig _config;
    private readonly LogManager _logManager;
    private readonly DlqManager _dlqManager;
    private readonly string _testDirectory;

    public DlqManagerTests()
    {
        _config = new DlqManagerConfig
        {
            Enabled = true,
            MaxRetries = 3,
            RetryBackoffMs = 100,
            TopicSuffix = ".DLQ",
            CleanupIntervalMs = 600_000, // Long interval so cleanup doesn't interfere
            EntryMaxAgeMs = 300_000
        };

        _testDirectory = Path.Combine(Path.GetTempPath(), $"surgewave-dlq-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDirectory);

        _logManager = new LogManager(_testDirectory, new MemoryLogSegmentFactory());
        _dlqManager = new DlqManager(_config, _logManager, null, null, NullLogger<DlqManager>.Instance);
    }

    public async ValueTask InitializeAsync()
    {
        // Create the test topic so DlqManager can read from it
        await _logManager.CreateTopicAsync("test-topic", partitionCount: 1, replicationFactor: 1);

        // Append a dummy record batch so offset 0 exists
        var dummyBatch = CreateMinimalRecordBatch();
        await _logManager.AppendBatchAsync(
            new TopicPartition { Topic = "test-topic", Partition = 0 },
            dummyBatch);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    [Fact]
    public async Task Nack_IncrementsRetryCount()
    {
        await _dlqManager.HandleNackAsync("test-topic", 0, 0);

        var retryCount = _dlqManager.GetRetryCount("test-topic", 0, 0);
        Assert.Equal(1, retryCount);

        await _dlqManager.HandleNackAsync("test-topic", 0, 0);

        retryCount = _dlqManager.GetRetryCount("test-topic", 0, 0);
        Assert.Equal(2, retryCount);
    }

    [Fact]
    public async Task Nack_ExceedsMaxRetries_RoutesToDlq()
    {
        // Nack 3 times (maxRetries = 3)
        for (int i = 0; i < 3; i++)
        {
            var result = await _dlqManager.HandleNackAsync("test-topic", 0, 0);
            Assert.False(result); // Not routed to DLQ yet
        }

        // 4th nack should route to DLQ
        var routedToDlq = await _dlqManager.HandleNackAsync("test-topic", 0, 0);
        Assert.True(routedToDlq);

        // Verify DLQ topic was auto-created
        var dlqTopicName = _config.GetDlqTopicName("test-topic");
        var dlqMetadata = _logManager.GetTopicMetadata(dlqTopicName);
        Assert.NotNull(dlqMetadata);
    }

    [Fact]
    public async Task Nack_WithBackoff_DelaysRedelivery()
    {
        // Create DlqManager with a DelayIndex to test backoff scheduling
        var delayConfig = new DeliveryDelayConfig
        {
            Enabled = true,
            MaxDelayMs = 60_000,
            IndexCleanupIntervalMs = 600_000
        };
        var delayIndex = new DelayIndex(delayConfig, null, NullLogger<DelayIndex>.Instance);

        using var dlqWithDelay = new DlqManager(_config, _logManager, delayIndex, null, NullLogger<DlqManager>.Instance);

        var beforeNack = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await dlqWithDelay.HandleNackAsync("test-topic", 0, 0);

        // Verify that the delay index has a record for this offset
        var tp = new TopicPartition { Topic = "test-topic", Partition = 0 };
        Assert.True(delayIndex.HasDelayedRecords(tp));

        // The message should be delayed (backoff = 100ms * 1 = 100ms)
        Assert.True(delayIndex.IsDelayed(tp, offset: 0, currentTimeMs: beforeNack));

        delayIndex.Dispose();
    }

    [Fact]
    public async Task DlqTopic_AutoCreated()
    {
        // Verify DLQ topic doesn't exist yet
        var dlqTopicName = _config.GetDlqTopicName("test-topic");
        Assert.Null(_logManager.GetTopicMetadata(dlqTopicName));

        // Nack enough times to trigger DLQ routing
        for (int i = 0; i <= _config.MaxRetries; i++)
        {
            await _dlqManager.HandleNackAsync("test-topic", 0, 0);
        }

        // DLQ topic should now exist
        var dlqMetadata = _logManager.GetTopicMetadata(dlqTopicName);
        Assert.NotNull(dlqMetadata);
        Assert.Equal(dlqTopicName, dlqMetadata.Name);
    }

    [Fact]
    public async Task RetryCountHeader_AddedOnRedelivery()
    {
        // Nack once and verify retry count tracking
        await _dlqManager.HandleNackAsync("test-topic", 0, 0);

        var retryCount = _dlqManager.GetRetryCount("test-topic", 0, 0);
        Assert.Equal(1, retryCount);

        // The retry count header key constant should be accessible
        Assert.Equal("surgewave-retry-count", DlqManager.RetryCountHeaderKey);

        // After routing to DLQ, the entry should be cleaned up
        for (int i = 1; i <= _config.MaxRetries; i++)
        {
            await _dlqManager.HandleNackAsync("test-topic", 0, 0);
        }

        // After DLQ routing, retry count should be 0 (entry removed)
        retryCount = _dlqManager.GetRetryCount("test-topic", 0, 0);
        Assert.Equal(0, retryCount);
    }

    /// <summary>
    /// Creates a minimal valid record batch for testing.
    /// </summary>
    private static byte[] CreateMinimalRecordBatch()
    {
        var batch = new byte[Core.KafkaConstants.RecordBatch.HeaderSize + 10];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), 0); // baseOffset
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12); // batchLength
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(
            batch.AsSpan(Core.KafkaConstants.RecordBatch.BaseTimestampOffset, 8),
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()); // baseTimestamp
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1); // recordCount = 1
        return batch;
    }

    public void Dispose()
    {
        _dlqManager.Dispose();
        _logManager.Dispose();

        // Clean up test data directory
        try
        {
            if (Directory.Exists(_testDirectory))
                Directory.Delete(_testDirectory, recursive: true);
        }
        catch
        {
            // Ignore cleanup errors in tests
        }
    }
}
