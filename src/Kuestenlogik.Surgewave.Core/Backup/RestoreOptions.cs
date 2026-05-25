namespace Kuestenlogik.Surgewave.Core.Backup;

/// <summary>
/// Filters that turn a full restore into a point-in-time restore. Without any
/// option set, restore is unbounded (every segment in the backup is copied).
/// PIT operates at segment granularity — a segment that spans the cutoff is
/// treated as fully before; within-segment truncation is a follow-up. In
/// practice segment rotation defaults (~1 GB or hourly) make segment-boundary
/// PIT precise enough for the disaster-recovery use case where the operator
/// targets "the state before the bad write started" with hour-scale tolerance.
/// </summary>
public sealed class RestoreOptions
{
    /// <summary>
    /// Restore only segments whose <see cref="BackupSegmentInfo.MaxTimestampMs"/>
    /// is at or before this Unix-millisecond cutoff. Segments with
    /// <c>MaxTimestampMs == 0</c> (no time-index entries — a freshly rotated
    /// or empty segment) are always included; the time index simply did not
    /// have enough data to make a decision either way, and excluding them
    /// would silently drop the data they hold.
    /// </summary>
    public long? TargetTimestampMs { get; init; }

    /// <summary>
    /// Per-partition offset cutoff. Key is "{topic}/{partitionId}"; value is
    /// the largest <see cref="BackupSegmentInfo.BaseOffset"/> to restore for
    /// that partition (segments with <c>BaseOffset &gt; cutoff</c> are
    /// skipped). Useful when only one bad partition needs rolling back.
    /// </summary>
    public Dictionary<string, long>? TargetOffsetsPerPartition { get; init; }

    /// <summary>
    /// Subset of topics to restore — same semantics as the legacy <c>topics</c>
    /// argument on <c>RestoreAsync</c>, lifted to <see cref="RestoreOptions"/>
    /// so callers can describe a complete restore plan in one object.
    /// </summary>
    public IReadOnlyList<string>? Topics { get; init; }

    /// <summary>
    /// Whether to verify SHA256 checksums during restore. Default <c>true</c>.
    /// </summary>
    public bool VerifyChecksums { get; init; } = true;

    /// <summary>
    /// Whether to overwrite existing files at the destination. Default
    /// <c>false</c> — a partial-restore-into-live-broker is unsafe by
    /// default and must be opted into.
    /// </summary>
    public bool Overwrite { get; init; }

    /// <summary>
    /// Build the per-partition lookup key. Stays a private convention so the
    /// option dictionary uses a stable separator.
    /// </summary>
    public static string PartitionKey(string topic, int partitionId) => $"{topic}/{partitionId}";

    /// <summary>
    /// Helper for the common case: single-partition cutoff.
    /// </summary>
    public RestoreOptions WithOffsetCutoff(string topic, int partitionId, long offset)
    {
        var dict = TargetOffsetsPerPartition is null
            ? new Dictionary<string, long>(StringComparer.Ordinal)
            : new Dictionary<string, long>(TargetOffsetsPerPartition, StringComparer.Ordinal);
        dict[PartitionKey(topic, partitionId)] = offset;
        return new RestoreOptions
        {
            TargetTimestampMs = TargetTimestampMs,
            TargetOffsetsPerPartition = dict,
            Topics = Topics,
            VerifyChecksums = VerifyChecksums,
            Overwrite = Overwrite,
        };
    }
}
