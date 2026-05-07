using Kuestenlogik.Surgewave.Streams.Runtime;

namespace Kuestenlogik.Surgewave.Streams.Monitoring;

/// <summary>
/// Application-level lag information aggregated across all partitions.
/// </summary>
public sealed record ApplicationLag(
    string ApplicationId,
    long TotalLag,
    IReadOnlyList<StreamsPartitionLag> Partitions,
    DateTimeOffset Timestamp);

/// <summary>
/// Per-partition lag information.
/// </summary>
public sealed record StreamsPartitionLag(
    string Topic,
    int Partition,
    long CurrentOffset,
    long CommittedOffset,
    long HighWatermark,
    long Lag);
