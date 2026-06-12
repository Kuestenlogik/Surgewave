using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Storage.Disaggregated;
using Kuestenlogik.Surgewave.Storage.Disaggregated.Routing;
using Kuestenlogik.Surgewave.Storage.Disaggregated.Stateless;
using Kuestenlogik.Surgewave.Storage.Tiering;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Tests.Routing;

public sealed class RoutingPartitionAppenderTests
{
    private static readonly TopicPartition Replicated = new() { Topic = "orders", Partition = 0 };
    private static readonly TopicPartition Wal = new() { Topic = "events", Partition = 0 };
    private static readonly TopicPartition Stateless = new() { Topic = "logs", Partition = 0 };

    [Fact]
    public async Task Replicated_mode_falls_through_to_default_appender()
    {
        var defaultAppender = new RecordingAppender();
        var sut = new RoutingPartitionAppender(
            defaultAppender,
            storageModeLookup: _ => StorageMode.Replicated);

        var offset = await sut.AppendBatchAsync(Replicated, new byte[] { 1, 2, 3 }, recordCount: 1);

        Assert.Equal(0, offset);
        Assert.Equal(1, defaultAppender.Calls);
    }

    [Fact]
    public async Task DisaggregatedWal_also_falls_through_to_default_appender()
    {
        // disaggregated-wal keeps the hot path identical to replicated — the
        // WAL flusher offloads asynchronously, the producer ack still comes
        // from the local append.
        var defaultAppender = new RecordingAppender();
        var sut = new RoutingPartitionAppender(
            defaultAppender,
            storageModeLookup: _ => StorageMode.DisaggregatedWal);

        await sut.AppendBatchAsync(Wal, new byte[] { 1, 2, 3 }, recordCount: 1);

        Assert.Equal(1, defaultAppender.Calls);
    }

    [Fact]
    public async Task DisaggregatedStateless_routes_to_StatelessAgent_and_default_is_untouched()
    {
        var defaultAppender = new RecordingAppender();
        var manifests = new InMemoryPartitionManifestStore();
        await using var agent = new StatelessAgent(manifests, new RecordingRemote());
        var sut = new RoutingPartitionAppender(
            defaultAppender,
            storageModeLookup: _ => StorageMode.DisaggregatedStateless,
            statelessAgent: agent);

        var produce = sut.AppendBatchAsync(Stateless, new byte[] { 9, 9, 9 }, recordCount: 1);
        await agent.FlushPartitionAsync(Stateless);
        var offset = await produce;

        Assert.Equal(0, offset);
        Assert.Equal(0, defaultAppender.Calls);
    }

    [Fact]
    public async Task DisaggregatedStateless_without_agent_throws_configuration_error()
    {
        var sut = new RoutingPartitionAppender(
            new RecordingAppender(),
            storageModeLookup: _ => StorageMode.DisaggregatedStateless,
            statelessAgent: null);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            sut.AppendBatchAsync(Stateless, new byte[] { 1 }, recordCount: 1));

        Assert.Contains("disaggregated-stateless", ex.Message);
        Assert.Contains("StatelessAgent", ex.Message);
    }

    [Fact]
    public async Task Unknown_topic_metadata_falls_back_to_default_appender()
    {
        // Topic was just deleted, or metadata-cache miss during a race —
        // we still want the Produce to succeed against the local log instead
        // of failing with NullReferenceException.
        var defaultAppender = new RecordingAppender();
        var sut = new RoutingPartitionAppender(
            defaultAppender,
            storageModeLookup: _ => null);

        await sut.AppendBatchAsync(Replicated, new byte[] { 1 }, recordCount: 1);

        Assert.Equal(1, defaultAppender.Calls);
    }

    [Fact]
    public async Task DelegatingPartitionAppender_invokes_underlying_func()
    {
        // Sanity: the adapter that turns a Func into IPartitionAppender
        // doesn't swallow or transform anything.
        var called = false;
        var adapter = new DelegatingPartitionAppender((p, m, n, ct) =>
        {
            called = true;
            Assert.Equal(Replicated, p);
            Assert.Equal(3, m.Length);
            Assert.Equal(1, n);
            return Task.FromResult(42L);
        });

        var offset = await adapter.AppendBatchAsync(Replicated, new byte[] { 1, 2, 3 }, recordCount: 1);

        Assert.True(called);
        Assert.Equal(42L, offset);
    }

    // ── fakes ────────────────────────────────────────────────────────────

    private sealed class RecordingAppender : IPartitionAppender
    {
        public int Calls { get; private set; }

        public Task<long> AppendBatchAsync(
            TopicPartition partition, ReadOnlyMemory<byte> recordBatch, int recordCount,
            CancellationToken cancellationToken = default)
        {
            Calls++;
            return Task.FromResult((long)(Calls - 1));
        }
    }

    private sealed class RecordingRemote : IRemoteStorageProvider
    {
        public Task UploadSegmentAsync(string topic, int partition, long baseOffset,
            ReadOnlyMemory<byte> logData, ReadOnlyMemory<byte> indexData, ReadOnlyMemory<byte> timeIndexData,
            CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<(byte[] LogData, byte[] IndexData, byte[] TimeIndexData)> DownloadSegmentAsync(
            string topic, int partition, long baseOffset, CancellationToken cancellationToken = default)
            => Task.FromResult<(byte[], byte[], byte[])>(([], [], []));
        public Task DeleteSegmentAsync(string topic, int partition, long baseOffset,
            CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<IReadOnlyList<RemoteSegmentInfo>> ListSegmentsAsync(string topic, int partition,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<RemoteSegmentInfo>>([]);
        public Task<bool> SegmentExistsAsync(string topic, int partition, long baseOffset,
            CancellationToken cancellationToken = default) => Task.FromResult(false);
        public Task<RemoteSegmentInfo?> GetSegmentInfoAsync(string topic, int partition, long baseOffset,
            CancellationToken cancellationToken = default) => Task.FromResult<RemoteSegmentInfo?>(null);
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
