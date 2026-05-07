namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Retention policy configuration for log cleanup
/// </summary>
public sealed record RetentionPolicy
{
    /// <summary>
    /// Retention period in hours. Segments older than this will be deleted.
    /// Set to -1 for unlimited time-based retention.
    /// </summary>
    public int RetentionHours { get; init; } = 168; // 7 days default

    /// <summary>
    /// Maximum total log size in bytes per partition.
    /// Set to -1 for unlimited size-based retention.
    /// </summary>
    public long RetentionBytes { get; init; } = -1;

    /// <summary>
    /// Minimum number of segments to always keep, regardless of retention policy.
    /// Must be at least 1 to keep the active segment.
    /// </summary>
    public int MinSegmentsToKeep { get; init; } = 1;

    public static RetentionPolicy Default => new();
    public static RetentionPolicy Unlimited => new() { RetentionHours = -1, RetentionBytes = -1 };
}
