using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Storage.Disaggregated;
using Kuestenlogik.Surgewave.Storage.Disaggregated.Stateless;
using Kuestenlogik.Surgewave.Storage.Tiering;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Tests.Stateless;

public sealed class StatelessAgentTests
{
    private static readonly TopicPartition P0 = new() { Topic = "orders", Partition = 0 };
    private static readonly TopicPartition P1 = new() { Topic = "orders", Partition = 1 };

    [Fact]
    public async Task ProduceAsync_then_FlushPartitionAsync_assigns_baseOffset_zero_and_writes_manifest()
    {
        await using var sut = NewAgent(out var remote, out var manifests);
        var produce = sut.ProduceAsync(P0, payload(10), recordCount: 1);

        await sut.FlushPartitionAsync(P0);
        var offset = await produce;

        Assert.Equal(0, offset);
        Assert.Single(remote.Uploaded);
        var manifest = await manifests.GetAsync(P0);
        Assert.Single(manifest.Objects);
        Assert.Equal(0, manifest.Objects[0].FirstOffset);
        Assert.Equal(0, manifest.Objects[0].LastOffset);
    }

    [Fact]
    public async Task Size_threshold_triggers_immediate_flush()
    {
        await using var sut = NewAgent(out _, out var manifests, new StatelessAgentOptions
        {
            MaxBufferBytes = 16,           // tiny so a single 32-byte batch trips
            MaxBufferAge = TimeSpan.FromHours(1), // age can't be the trigger
        });

        var offset = await sut.ProduceAsync(P0, payload(32), recordCount: 4);

        Assert.Equal(0, offset);
        var manifest = await manifests.GetAsync(P0);
        Assert.Single(manifest.Objects);
        Assert.Equal(3, manifest.Objects[0].LastOffset);
    }

    [Fact]
    public async Task Multiple_pending_produces_get_consecutive_offsets_after_flush()
    {
        await using var sut = NewAgent(out _, out var manifests);
        var p1 = sut.ProduceAsync(P0, payload(10), recordCount: 3); // gets offsets 0..2
        var p2 = sut.ProduceAsync(P0, payload(10), recordCount: 2); // gets offsets 3..4
        var p3 = sut.ProduceAsync(P0, payload(10), recordCount: 1); // gets offset 5

        await sut.FlushPartitionAsync(P0);

        Assert.Equal(0, await p1);
        Assert.Equal(3, await p2);
        Assert.Equal(5, await p3);
        var manifest = await manifests.GetAsync(P0);
        Assert.Equal(0, manifest.Objects[0].FirstOffset);
        Assert.Equal(5, manifest.Objects[0].LastOffset);
    }

    [Fact]
    public async Task Different_partitions_get_independent_offset_counters()
    {
        await using var sut = NewAgent(out _, out _);
        var p0 = sut.ProduceAsync(P0, payload(10), recordCount: 1);
        var p1 = sut.ProduceAsync(P1, payload(10), recordCount: 1);

        await sut.FlushPartitionAsync(P0);
        await sut.FlushPartitionAsync(P1);

        Assert.Equal(0, await p0);
        Assert.Equal(0, await p1);
    }

    [Fact]
    public async Task Failed_S3_upload_faults_all_pending_tickets()
    {
        var manifests = new InMemoryPartitionManifestStore();
        await using var sut = new StatelessAgent(manifests, new FailingRemote());
        var p1 = sut.ProduceAsync(P0, payload(10), recordCount: 1);
        var p2 = sut.ProduceAsync(P0, payload(10), recordCount: 1);

        await Assert.ThrowsAsync<IOException>(() => sut.FlushPartitionAsync(P0));

        await Assert.ThrowsAsync<IOException>(async () => await p1);
        await Assert.ThrowsAsync<IOException>(async () => await p2);
        var manifest = await manifests.GetAsync(P0);
        Assert.Empty(manifest.Objects);
    }

    [Fact]
    public async Task Age_loop_flushes_buffer_when_oldest_record_exceeds_MaxBufferAge()
    {
        await using var sut = NewAgent(out _, out var manifests, new StatelessAgentOptions
        {
            MaxBufferBytes = long.MaxValue,
            MaxBufferAge = TimeSpan.FromMilliseconds(50),
            AgePollInterval = TimeSpan.FromMilliseconds(20),
        });
        await sut.StartAsync();

        var produce = sut.ProduceAsync(P0, payload(10), recordCount: 1);
        var offset = await produce.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal(0, offset);
        var manifest = await manifests.GetAsync(P0);
        Assert.Single(manifest.Objects);
    }

    [Fact]
    public async Task StopAsync_drains_remaining_buffers()
    {
        await using var sut = NewAgent(out _, out var manifests, new StatelessAgentOptions
        {
            MaxBufferBytes = long.MaxValue,                 // never size-trips
            MaxBufferAge = TimeSpan.FromHours(1),           // never age-trips
            AgePollInterval = TimeSpan.FromMilliseconds(50),
        });
        await sut.StartAsync();
        var produce = sut.ProduceAsync(P0, payload(10), recordCount: 1);

        await sut.StopAsync();

        Assert.Equal(0, await produce);
        var manifest = await manifests.GetAsync(P0);
        Assert.Single(manifest.Objects);
    }

    [Fact]
    public async Task ProduceAsync_propagates_caller_cancellation_before_flush()
    {
        await using var sut = NewAgent(out _, out _);
        using var cts = new CancellationTokenSource();
        var produce = sut.ProduceAsync(P0, payload(10), recordCount: 1, cts.Token);

        cts.Cancel();

        await Assert.ThrowsAsync<TaskCanceledException>(async () => await produce);
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static byte[] payload(int n) => new byte[n];

    private static StatelessAgent NewAgent(
        out RecordingRemote remote,
        out InMemoryPartitionManifestStore manifests,
        StatelessAgentOptions? options = null)
    {
        remote = new RecordingRemote();
        manifests = new InMemoryPartitionManifestStore();
        return new StatelessAgent(manifests, remote, options);
    }

    private sealed class RecordingRemote : IRemoteStorageProvider
    {
        public List<(string Topic, int Partition, long BaseOffset, int Bytes)> Uploaded { get; } = [];

        public Task UploadSegmentAsync(string topic, int partition, long baseOffset,
            ReadOnlyMemory<byte> logData, ReadOnlyMemory<byte> indexData, ReadOnlyMemory<byte> timeIndexData,
            CancellationToken cancellationToken = default)
        {
            Uploaded.Add((topic, partition, baseOffset, logData.Length));
            return Task.CompletedTask;
        }

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

    private sealed class FailingRemote : IRemoteStorageProvider
    {
        public Task UploadSegmentAsync(string topic, int partition, long baseOffset,
            ReadOnlyMemory<byte> logData, ReadOnlyMemory<byte> indexData, ReadOnlyMemory<byte> timeIndexData,
            CancellationToken cancellationToken = default)
            => throw new IOException("simulated S3 timeout");
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
