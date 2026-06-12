using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Disaggregated.Wal;
using Microsoft.Win32.SafeHandles;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Tests.Wal;

public sealed class PartitionLogWalSegmentSourceTests : IDisposable
{
    private static readonly TopicPartition Partition = new() { Topic = "orders", Partition = 0 };
    private readonly string _tempDir;

    public PartitionLogWalSegmentSourceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "swv-wal-src-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task Returns_empty_when_lookup_returns_null()
    {
        var sut = new PartitionLogWalSegmentSource(
            segmentLookup: _ => null,
            segmentDirectory: _ => _tempDir);

        var sealedSegments = await sut.ListSealedAsync(Partition);

        Assert.Empty(sealedSegments);
    }

    [Fact]
    public async Task Skips_active_segment_only()
    {
        // Active segment is the last entry in the list; a one-segment log has
        // nothing sealed.
        var sut = new PartitionLogWalSegmentSource(
            segmentLookup: _ => [Segment(0, currentOffset: 100, size: 1024)],
            segmentDirectory: _ => _tempDir);

        var sealedSegments = await sut.ListSealedAsync(Partition);

        Assert.Empty(sealedSegments);
    }

    [Fact]
    public async Task Returns_sealed_segments_with_offsets_and_size()
    {
        var sut = new PartitionLogWalSegmentSource(
            segmentLookup: _ => [
                Segment(0,   currentOffset: 100, size: 1024),
                Segment(100, currentOffset: 200, size: 2048),
                Segment(200, currentOffset: 250, size: 512), // active — should be excluded
            ],
            segmentDirectory: _ => _tempDir);

        var sealedSegments = await sut.ListSealedAsync(Partition);

        Assert.Equal(2, sealedSegments.Count);
        Assert.Equal(0L, sealedSegments[0].BaseOffset);
        Assert.Equal(99L, sealedSegments[0].LastOffset);
        Assert.Equal(1024, sealedSegments[0].SizeBytes);
        Assert.Equal(100L, sealedSegments[1].BaseOffset);
        Assert.Equal(199L, sealedSegments[1].LastOffset);
    }

    [Fact]
    public async Task Skips_empty_sealed_segments()
    {
        // BaseOffset == CurrentOffset means the segment was rolled before any
        // record landed in it. There's nothing to upload.
        var sut = new PartitionLogWalSegmentSource(
            segmentLookup: _ => [
                Segment(0, currentOffset: 0, size: 0),
                Segment(0, currentOffset: 50, size: 256),  // active
            ],
            segmentDirectory: _ => _tempDir);

        var sealedSegments = await sut.ListSealedAsync(Partition);

        Assert.Empty(sealedSegments);
    }

    [Fact]
    public async Task Deferred_log_reader_loads_bytes_from_disk()
    {
        // Place a real .log file in the partition dir so the deferred reader
        // returns its contents on demand.
        var partitionDir = Path.Combine(_tempDir, Partition.Topic, $"partition-{Partition.Partition}");
        Directory.CreateDirectory(partitionDir);
        var logFile = Path.Combine(partitionDir, "00000000000000000000.log");
        await File.WriteAllBytesAsync(logFile, "hello"u8.ToArray());

        var sut = new PartitionLogWalSegmentSource(
            segmentLookup: _ => [
                Segment(0,   currentOffset: 5, size: 5),
                Segment(100, currentOffset: 150, size: 256), // active
            ],
            segmentDirectory: tp => Path.Combine(_tempDir, tp.Topic, $"partition-{tp.Partition}"));

        var sealedSegments = await sut.ListSealedAsync(Partition);
        var bytes = await sealedSegments[0].ReadLogBytesAsync(CancellationToken.None);

        Assert.Equal("hello"u8.ToArray(), bytes);
    }

    [Fact]
    public async Task Missing_index_files_return_empty_arrays_not_throw()
    {
        var partitionDir = Path.Combine(_tempDir, Partition.Topic, $"partition-{Partition.Partition}");
        Directory.CreateDirectory(partitionDir);
        // Only the .log file is present; no .index, no .timeindex.
        await File.WriteAllBytesAsync(Path.Combine(partitionDir, "00000000000000000000.log"), [1, 2]);

        var sut = new PartitionLogWalSegmentSource(
            segmentLookup: _ => [
                Segment(0, currentOffset: 2, size: 2),
                Segment(100, currentOffset: 150, size: 256),
            ],
            segmentDirectory: tp => Path.Combine(_tempDir, tp.Topic, $"partition-{tp.Partition}"));

        var sealedSegments = await sut.ListSealedAsync(Partition);
        var index = await sealedSegments[0].ReadIndexBytesAsync(CancellationToken.None);
        var timeIndex = await sealedSegments[0].ReadTimeIndexBytesAsync(CancellationToken.None);

        Assert.Empty(index);
        Assert.Empty(timeIndex);
    }

    private static FakeSegment Segment(long baseOffset, long currentOffset, long size) =>
        new(baseOffset, currentOffset, size);

    private sealed class FakeSegment : ILogSegment
    {
        public FakeSegment(long baseOffset, long currentOffset, long size)
        {
            BaseOffset = baseOffset;
            CurrentOffset = currentOffset;
            Size = size;
        }

        public long BaseOffset { get; }
        public long CurrentOffset { get; }
        public long Size { get; }
        public bool IsFull => false;
        public DateTime CreatedAt { get; } = new(2026, 6, 12, 0, 0, 0, DateTimeKind.Utc);
        public long MaxTimestamp => 0;
        public long? GetFirstMessageOffset() => CurrentOffset > BaseOffset ? BaseOffset : null;
        public ValueTask<(long baseOffset, int recordCount)> AppendBatchAsync(byte[] recordBatch, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();
        public ValueTask FlushAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public ValueTask<List<byte[]>> ReadBatchesAsync(long startOffset, int maxBytes, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();
        public ValueTask<(ReadOnlyMemory<byte> Data, List<int> BatchOffsets)> ReadBatchesContiguousAsync(long startOffset, int maxBytes, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();
        public long? GetFilePositionForOffset(long startOffset) => null;
        public long? FindOffsetByTimestamp(long targetTimestamp) => null;
        public void DeleteFiles() { }
        public void Dispose() { }
    }
}
