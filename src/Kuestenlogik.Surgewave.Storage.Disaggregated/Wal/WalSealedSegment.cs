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
    Func<CancellationToken, Task<byte[]>> ReadTimeIndexBytesAsync);
