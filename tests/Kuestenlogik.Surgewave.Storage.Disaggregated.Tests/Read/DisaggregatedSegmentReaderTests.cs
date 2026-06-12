using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Storage.Disaggregated;
using Kuestenlogik.Surgewave.Storage.Disaggregated.Read;
using Kuestenlogik.Surgewave.Storage.Tiering;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Tests.Read;

public sealed class DisaggregatedSegmentReaderTests
{
    private static readonly TopicPartition Partition = new() { Topic = "orders", Partition = 0 };

    [Fact]
    public async Task TryReadAsync_with_empty_manifest_misses()
    {
        var manifests = new InMemoryPartitionManifestStore();
        var remote = new RecordingRemote();
        var sut = new DisaggregatedSegmentReader(manifests, remote);

        var result = await sut.TryReadAsync(Partition, startOffset: 0, maxBytes: 1024);

        Assert.False(result.HitManifest);
        Assert.Empty(result.LogBytes.ToArray());
        Assert.Empty(remote.Downloaded);
    }

    [Fact]
    public async Task TryReadAsync_hits_matching_stream_object_and_downloads_bytes()
    {
        var manifests = new InMemoryPartitionManifestStore();
        await manifests.AppendObjectAsync(Partition, new StreamObjectRef("k0", 0, 99, 1024, DateTime.UtcNow));
        var remote = new RecordingRemote();
        remote.Payload[(Partition.Topic, Partition.Partition, 0L)] = ("hello"u8.ToArray(), [], []);
        var sut = new DisaggregatedSegmentReader(manifests, remote);

        var result = await sut.TryReadAsync(Partition, startOffset: 50, maxBytes: 1024);

        Assert.True(result.HitManifest);
        Assert.Equal("hello"u8.ToArray(), result.LogBytes.ToArray());
        Assert.Equal(100L, result.NextOffset);
        Assert.Single(remote.Downloaded);
        Assert.Equal(0L, remote.Downloaded[0].BaseOffset);
    }

    [Fact]
    public async Task TryReadAsync_with_offset_past_tail_misses_and_skips_remote()
    {
        var manifests = new InMemoryPartitionManifestStore();
        await manifests.AppendObjectAsync(Partition, new StreamObjectRef("k0", 0, 99, 1024, DateTime.UtcNow));
        var remote = new RecordingRemote();
        var sut = new DisaggregatedSegmentReader(manifests, remote);

        var result = await sut.TryReadAsync(Partition, startOffset: 500, maxBytes: 1024);

        Assert.False(result.HitManifest);
        Assert.Empty(remote.Downloaded);
    }

    [Fact]
    public async Task TryReadAsync_selects_correct_object_via_binary_search()
    {
        var manifests = new InMemoryPartitionManifestStore();
        await manifests.AppendObjectAsync(Partition, new StreamObjectRef("k0", 0, 99, 1024, DateTime.UtcNow));
        await manifests.AppendObjectAsync(Partition, new StreamObjectRef("k1", 100, 199, 1024, DateTime.UtcNow));
        await manifests.AppendObjectAsync(Partition, new StreamObjectRef("k2", 200, 299, 1024, DateTime.UtcNow));
        var remote = new RecordingRemote();
        remote.Payload[(Partition.Topic, Partition.Partition, 100L)] = ("middle"u8.ToArray(), [], []);
        var sut = new DisaggregatedSegmentReader(manifests, remote);

        var result = await sut.TryReadAsync(Partition, startOffset: 150, maxBytes: 1024);

        Assert.True(result.HitManifest);
        Assert.Equal("middle"u8.ToArray(), result.LogBytes.ToArray());
        Assert.Equal(200L, result.NextOffset);
        Assert.Equal(100L, remote.Downloaded[0].BaseOffset);
    }

    // ── fake remote that records download calls + lets tests pre-seed bytes ──

    private sealed class RecordingRemote : IRemoteStorageProvider
    {
        public List<(string Topic, int Partition, long BaseOffset)> Downloaded { get; } = [];
        public Dictionary<(string Topic, int Partition, long BaseOffset), (byte[] Log, byte[] Index, byte[] TimeIndex)> Payload { get; } = [];

        public Task UploadSegmentAsync(string topic, int partition, long baseOffset,
            ReadOnlyMemory<byte> logData, ReadOnlyMemory<byte> indexData, ReadOnlyMemory<byte> timeIndexData,
            CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task<(byte[] LogData, byte[] IndexData, byte[] TimeIndexData)> DownloadSegmentAsync(
            string topic, int partition, long baseOffset, CancellationToken cancellationToken = default)
        {
            Downloaded.Add((topic, partition, baseOffset));
            if (Payload.TryGetValue((topic, partition, baseOffset), out var payload))
                return Task.FromResult(payload);
            return Task.FromResult<(byte[], byte[], byte[])>(([], [], []));
        }

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
