using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Storage.Disaggregated;
using Kuestenlogik.Surgewave.Storage.Disaggregated.Wal;
using Kuestenlogik.Surgewave.Storage.Tiering;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Tests.Wal;

public sealed class WalFlusherTests
{
    private static readonly TopicPartition Partition = new() { Topic = "orders", Partition = 0 };

    [Fact]
    public async Task RunOnceAsync_with_no_sealed_segments_does_nothing()
    {
        var sut = NewFlusher(source: new FakeSource(), out var remote, out var manifests);
        var flushed = await sut.RunOnceAsync(Partition);

        Assert.Equal(0, flushed);
        Assert.Empty(remote.Uploaded);
        var manifest = await manifests.GetAsync(Partition);
        Assert.Empty(manifest.Objects);
    }

    [Fact]
    public async Task RunOnceAsync_flushes_single_sealed_segment_and_appends_manifest()
    {
        var source = new FakeSource();
        source.Add(Segment(baseOffset: 0, lastOffset: 99, sizeBytes: 1024));
        var sut = NewFlusher(source, out var remote, out var manifests);

        var flushed = await sut.RunOnceAsync(Partition);

        Assert.Equal(1, flushed);
        Assert.Single(remote.Uploaded);
        Assert.Equal(0, remote.Uploaded[0].BaseOffset);

        var manifest = await manifests.GetAsync(Partition);
        Assert.Single(manifest.Objects);
        Assert.Equal(0, manifest.Objects[0].FirstOffset);
        Assert.Equal(99, manifest.Objects[0].LastOffset);
        Assert.Equal("topics/orders/0/stream-00000000000000000000.so", manifest.Objects[0].ObjectKey);
    }

    [Fact]
    public async Task RunOnceAsync_flushes_multiple_segments_in_offset_order()
    {
        var source = new FakeSource();
        source.Add(Segment(200, 299, 1024));
        source.Add(Segment(0, 99, 1024));
        source.Add(Segment(100, 199, 1024));
        var sut = NewFlusher(source, out var remote, out var manifests);

        var flushed = await sut.RunOnceAsync(Partition);

        Assert.Equal(3, flushed);
        Assert.Equal([0, 100, 200], remote.Uploaded.Select(u => u.BaseOffset).ToList());

        var manifest = await manifests.GetAsync(Partition);
        Assert.Equal([0, 100, 200], manifest.Objects.Select(o => o.FirstOffset).ToList());
    }

    [Fact]
    public async Task RunOnceAsync_skips_segments_already_in_manifest()
    {
        var source = new FakeSource();
        source.Add(Segment(0, 99, 1024));
        source.Add(Segment(100, 199, 1024));
        var sut = NewFlusher(source, out var remote, out var manifests);

        // First flush picks both up
        await sut.RunOnceAsync(Partition);
        remote.Uploaded.Clear();

        // Second flush sees nothing new
        var flushed = await sut.RunOnceAsync(Partition);
        Assert.Equal(0, flushed);
        Assert.Empty(remote.Uploaded);
    }

    [Fact]
    public async Task RunOnceAsync_honours_MaxSegmentsPerScan()
    {
        var source = new FakeSource();
        for (var i = 0; i < 5; i++)
        {
            source.Add(Segment(i * 100, i * 100 + 99, 1024));
        }
        var sut = NewFlusher(source, out var remote, out var manifests,
            options: new WalFlusherOptions { MaxSegmentsPerScan = 2 });

        var flushed = await sut.RunOnceAsync(Partition);
        Assert.Equal(2, flushed);
        Assert.Equal([0, 100], remote.Uploaded.Select(u => u.BaseOffset).ToList());
    }

    [Fact]
    public async Task RunOnceAsync_failed_upload_leaves_manifest_untouched()
    {
        var source = new FakeSource();
        source.Add(Segment(0, 99, 1024));
        var failingRemote = new FailingRemote();
        var manifests = new InMemoryPartitionManifestStore();
        var sut = new WalFlusher(source, manifests, failingRemote);

        await Assert.ThrowsAsync<IOException>(() => sut.RunOnceAsync(Partition));

        var manifest = await manifests.GetAsync(Partition);
        Assert.Empty(manifest.Objects);
    }

    [Fact]
    public void StreamObjectKeyConvention_uses_D20_padding()
    {
        var key = StreamObjectKeyConvention.Build(Partition, baseOffset: 42);
        Assert.Equal("topics/orders/0/stream-00000000000000000042.so", key);
    }

    [Fact]
    public async Task TrimAfterFlush_calls_TrimAsync_when_option_enabled()
    {
        var source = new FakeSource();
        var trimmed = false;
        source.Add(Segment(0, 99, 1024) with { TrimAsync = _ => { trimmed = true; return Task.CompletedTask; } });
        var sut = NewFlusher(source, out _, out _,
            options: new WalFlusherOptions { TrimAfterFlush = true });

        await sut.RunOnceAsync(Partition);

        Assert.True(trimmed);
    }

    [Fact]
    public async Task TrimAfterFlush_off_skips_TrimAsync_even_when_delegate_set()
    {
        var source = new FakeSource();
        var trimmed = false;
        source.Add(Segment(0, 99, 1024) with { TrimAsync = _ => { trimmed = true; return Task.CompletedTask; } });
        var sut = NewFlusher(source, out _, out _,
            options: new WalFlusherOptions { TrimAfterFlush = false });

        await sut.RunOnceAsync(Partition);

        Assert.False(trimmed);
    }

    [Fact]
    public async Task Failed_upload_skips_trim()
    {
        var source = new FakeSource();
        var trimmed = false;
        source.Add(Segment(0, 99, 1024) with { TrimAsync = _ => { trimmed = true; return Task.CompletedTask; } });
        var sut = new WalFlusher(source, new InMemoryPartitionManifestStore(), new FailingRemote(),
            options: new WalFlusherOptions { TrimAfterFlush = true });

        await Assert.ThrowsAsync<IOException>(() => sut.RunOnceAsync(Partition));

        Assert.False(trimmed);
    }

    [Fact]
    public async Task Trim_exception_is_swallowed_after_successful_manifest_commit()
    {
        // A trim error after the manifest is already committed must not
        // surface — the manifest is the source of truth, the leftover
        // local file is a future janitor's problem.
        var source = new FakeSource();
        source.Add(Segment(0, 99, 1024) with { TrimAsync = _ => throw new IOException("disk full") });
        var sut = NewFlusher(source, out _, out var manifests,
            options: new WalFlusherOptions { TrimAfterFlush = true });

        var n = await sut.RunOnceAsync(Partition);

        Assert.Equal(1, n);
        var manifest = await manifests.GetAsync(Partition);
        Assert.Single(manifest.Objects);
    }

    // ── fakes ────────────────────────────────────────────────────────────

    private static WalFlusher NewFlusher(
        FakeSource source,
        out FakeRemote remote,
        out InMemoryPartitionManifestStore manifests,
        WalFlusherOptions? options = null)
    {
        remote = new FakeRemote();
        manifests = new InMemoryPartitionManifestStore();
        return new WalFlusher(source, manifests, remote, options);
    }

    private static WalSealedSegment Segment(long baseOffset, long lastOffset, long sizeBytes) =>
        new(
            Partition: Partition,
            BaseOffset: baseOffset,
            LastOffset: lastOffset,
            SizeBytes: sizeBytes,
            CreatedAt: new DateTime(2026, 6, 12, 0, 0, 0, DateTimeKind.Utc),
            ReadLogBytesAsync: _ => Task.FromResult(new byte[(int)sizeBytes]),
            ReadIndexBytesAsync: _ => Task.FromResult(Array.Empty<byte>()),
            ReadTimeIndexBytesAsync: _ => Task.FromResult(Array.Empty<byte>()));

    private sealed class FakeSource : IWalSegmentSource
    {
        private readonly List<WalSealedSegment> _segments = [];
        public void Add(WalSealedSegment s) => _segments.Add(s);
        public ValueTask<IReadOnlyList<WalSealedSegment>> ListSealedAsync(
            TopicPartition partition, CancellationToken cancellationToken = default)
            => ValueTask.FromResult<IReadOnlyList<WalSealedSegment>>(_segments);
    }

    private sealed class FakeRemote : IRemoteStorageProvider
    {
        public List<(string Topic, int Partition, long BaseOffset)> Uploaded { get; } = [];

        public Task UploadSegmentAsync(string topic, int partition, long baseOffset,
            ReadOnlyMemory<byte> logData, ReadOnlyMemory<byte> indexData, ReadOnlyMemory<byte> timeIndexData,
            CancellationToken cancellationToken = default)
        {
            Uploaded.Add((topic, partition, baseOffset));
            return Task.CompletedTask;
        }

        public Task<(byte[] LogData, byte[] IndexData, byte[] TimeIndexData)> DownloadSegmentAsync(
            string topic, int partition, long baseOffset, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

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
            => throw new NotSupportedException();

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
