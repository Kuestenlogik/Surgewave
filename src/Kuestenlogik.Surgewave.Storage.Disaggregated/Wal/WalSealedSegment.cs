using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Wal;

/// <summary>
/// A sealed (no longer being appended to) WAL segment that the
/// <see cref="WalFlusher"/> is about to hand to the object store.
/// The reader is deferred — the flusher only opens the segment bytes
/// at PUT time, so a partition with hundreds of sealed segments does
/// not have its whole tail loaded into memory.
/// </summary>
public sealed record WalSealedSegment(
    TopicPartition Partition,
    long BaseOffset,
    long LastOffset,
    long SizeBytes,
    DateTime CreatedAt,
    Func<CancellationToken, Task<byte[]>> ReadLogBytesAsync,
    Func<CancellationToken, Task<byte[]>> ReadIndexBytesAsync,
    Func<CancellationToken, Task<byte[]>> ReadTimeIndexBytesAsync)
{
    /// <summary>
    /// Optional delegate that deletes the local segment files after a
    /// successful flush + manifest-commit. When
    /// <see cref="WalFlusherOptions.TrimAfterFlush"/> is on and this
    /// delegate is set, the flusher invokes it as the final step of
    /// <c>FlushOneAsync</c>. Null = keep the local copy (default; the
    /// segment falls under the topic's normal <c>retention.ms</c> until
    /// it expires).
    ///
    /// Safety note: the broker's Fetch path must consult the partition
    /// manifest via <c>DisaggregatedSegmentReader</c> before reading
    /// from the local log, otherwise a trimmed offset becomes
    /// unreachable. The runtime wires this together when both
    /// <c>WithPartitionAppender</c> and <c>WithDisaggregatedReader</c>
    /// are set.
    /// </summary>
    public Func<CancellationToken, Task>? TrimAsync { get; init; }
}
