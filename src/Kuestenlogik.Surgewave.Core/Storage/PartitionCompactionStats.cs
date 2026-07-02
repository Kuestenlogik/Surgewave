namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// In-memory stats of the last compaction run for one partition.
/// Tracked by <see cref="LogManager"/> so admin surfaces (gRPC/REST) can
/// report real compaction status instead of placeholders.
/// </summary>
public sealed record PartitionCompactionStats(
    DateTimeOffset LastCompaction,
    long RecordsRemoved,
    long BytesRemoved);
