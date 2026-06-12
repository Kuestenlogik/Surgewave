using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Storage.Disaggregated;
using Kuestenlogik.Surgewave.Storage.Disaggregated.Wal;
using Kuestenlogik.Surgewave.Storage.Tiering;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Tests;

public sealed class DisaggregatedSubsystemTests
{
    private static readonly TopicPartition P0 = new() { Topic = "orders", Partition = 0 };
    private static readonly TopicPartition P1 = new() { Topic = "orders", Partition = 1 };

    [Fact]
    public async Task RegisterPartition_appears_in_WatchedPartitions()
    {
        await using var sut = NewSubsystem();

        sut.RegisterPartition(P0);
        sut.RegisterPartition(P1);

        var watched = sut.WatchedPartitions();
        Assert.Contains(P0, watched);
        Assert.Contains(P1, watched);
    }

    [Fact]
    public async Task RegisterPartition_twice_is_idempotent()
    {
        await using var sut = NewSubsystem();

        sut.RegisterPartition(P0);
        sut.RegisterPartition(P0);

        Assert.Single(sut.WatchedPartitions());
    }

    [Fact]
    public async Task UnregisterPartition_removes_from_watch()
    {
        await using var sut = NewSubsystem();
        sut.RegisterPartition(P0);

        sut.UnregisterPartition(P0);

        Assert.Empty(sut.WatchedPartitions());
    }

    [Fact]
    public async Task StartAsync_then_StopAsync_completes_cleanly()
    {
        await using var sut = NewSubsystem(options: new WalFlusherOptions { PollInterval = TimeSpan.FromMilliseconds(20) });
        await sut.StartAsync();

        // Let the loop spin at least once.
        await Task.Delay(60);
        await sut.StopAsync();

        // Second stop is a no-op, no exception expected.
        await sut.StopAsync();
    }

    [Fact]
    public async Task Background_loop_flushes_segments_for_registered_partition()
    {
        var source = new ReplayingSource();
        source.Add(P0, Segment(P0, 0, 99, 1024));
        await using var sut = NewSubsystem(source: source, options: new WalFlusherOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(20),
        });
        sut.RegisterPartition(P0);

        await sut.StartAsync();
        // Wait long enough that the loop runs at least one iteration.
        var manifest = await WaitForManifestObjectsAsync(sut, P0, expected: 1, timeoutMs: 1500);
        await sut.StopAsync();

        Assert.Single(manifest.Objects);
        Assert.Equal(0, manifest.Objects[0].FirstOffset);
    }

    [Fact]
    public async Task Reader_returns_bytes_for_offset_in_manifest_after_flush()
    {
        var remote = new ReplayingRemote();
        remote.Payload[(P0.Topic, P0.Partition, 0L)] = ("hi"u8.ToArray(), [], []);
        var source = new ReplayingSource();
        source.Add(P0, Segment(P0, 0, 99, 1024));
        await using var sut = NewSubsystem(source: source, remote: remote, options: new WalFlusherOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(20),
        });
        sut.RegisterPartition(P0);
        await sut.StartAsync();
        await WaitForManifestObjectsAsync(sut, P0, expected: 1, timeoutMs: 1500);
        await sut.StopAsync();

        var read = await sut.Reader.TryReadAsync(P0, startOffset: 42, maxBytes: 1024);

        Assert.True(read.HitManifest);
        Assert.Equal("hi"u8.ToArray(), read.LogBytes.ToArray());
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static DisaggregatedSubsystem NewSubsystem(
        IWalSegmentSource? source = null,
        IRemoteStorageProvider? remote = null,
        WalFlusherOptions? options = null)
    {
        return new DisaggregatedSubsystem(
            manifests: new InMemoryPartitionManifestStore(),
            segments: source ?? new ReplayingSource(),
            remote: remote ?? new ReplayingRemote(),
            options: options);
    }

    private static async Task<PartitionManifest> WaitForManifestObjectsAsync(
        DisaggregatedSubsystem sut, TopicPartition partition, int expected, int timeoutMs)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            var m = await sut.Manifests.GetAsync(partition);
            if (m.Objects.Count >= expected) return m;
            await Task.Delay(20);
        }
        return await sut.Manifests.GetAsync(partition);
    }

    private static WalSealedSegment Segment(TopicPartition p, long baseOffset, long lastOffset, long sizeBytes) =>
        new(
            Partition: p,
            BaseOffset: baseOffset,
            LastOffset: lastOffset,
            SizeBytes: sizeBytes,
            CreatedAt: new DateTime(2026, 6, 12, 0, 0, 0, DateTimeKind.Utc),
            ReadLogBytesAsync: _ => Task.FromResult(new byte[(int)sizeBytes]),
            ReadIndexBytesAsync: _ => Task.FromResult(Array.Empty<byte>()),
            ReadTimeIndexBytesAsync: _ => Task.FromResult(Array.Empty<byte>()));

    private sealed class ReplayingSource : IWalSegmentSource
    {
        private readonly Dictionary<TopicPartition, List<WalSealedSegment>> _bag = [];
        public void Add(TopicPartition p, WalSealedSegment s)
        {
            if (!_bag.TryGetValue(p, out var list))
            {
                list = [];
                _bag[p] = list;
            }
            list.Add(s);
        }
        public ValueTask<IReadOnlyList<WalSealedSegment>> ListSealedAsync(
            TopicPartition partition, CancellationToken cancellationToken = default)
            => ValueTask.FromResult<IReadOnlyList<WalSealedSegment>>(
                _bag.TryGetValue(partition, out var list) ? list : []);
    }

    private sealed class ReplayingRemote : IRemoteStorageProvider
    {
        public Dictionary<(string Topic, int Partition, long BaseOffset), (byte[] Log, byte[] Index, byte[] TimeIndex)> Payload { get; } = [];

        public Task UploadSegmentAsync(string topic, int partition, long baseOffset,
            ReadOnlyMemory<byte> logData, ReadOnlyMemory<byte> indexData, ReadOnlyMemory<byte> timeIndexData,
            CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task<(byte[] LogData, byte[] IndexData, byte[] TimeIndexData)> DownloadSegmentAsync(
            string topic, int partition, long baseOffset, CancellationToken cancellationToken = default)
            => Task.FromResult(Payload.TryGetValue((topic, partition, baseOffset), out var p)
                ? p
                : (Array.Empty<byte>(), Array.Empty<byte>(), Array.Empty<byte>()));

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
