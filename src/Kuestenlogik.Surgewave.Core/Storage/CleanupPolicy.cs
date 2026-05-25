namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Topic cleanup policy - determines how old log segments are handled
/// </summary>
[Flags]
public enum CleanupPolicy
{
    /// <summary>
    /// Delete old segments based on retention time/size (default Kafka behavior)
    /// </summary>
    Delete = 1,

    /// <summary>
    /// Compact the log - keep only the latest value for each key
    /// </summary>
    Compact = 2,

    /// <summary>
    /// Both delete and compact (Kafka supports this combination)
    /// </summary>
    DeleteAndCompact = Delete | Compact,

    /// <summary>
    /// Ephemeral mode - messages are only delivered to active consumers.
    /// No persistence, uses a ring-buffer for short-term buffering of slow consumers.
    /// Similar to Redis Pub/Sub or NATS Core semantics.
    /// </summary>
    Ephemeral = 4
}

/// <summary>
/// Configuration for log compaction
/// </summary>
public sealed record CompactionConfig
{
    /// <summary>
    /// Minimum time before a message becomes eligible for compaction (milliseconds)
    /// Default: 0 (immediately eligible)
    /// </summary>
    public long MinCompactionLagMs { get; init; } = 0;

    /// <summary>
    /// How long to retain tombstones (delete markers) before removing them (milliseconds)
    /// Default: 24 hours
    /// </summary>
    public long DeleteRetentionMs { get; init; } = 24 * 60 * 60 * 1000;

    /// <summary>
    /// Minimum ratio of dirty (uncompacted) bytes to total bytes before compaction triggers
    /// Default: 0.5 (50% dirty ratio triggers compaction)
    /// </summary>
    public double MinCleanableDirtyRatio { get; init; } = 0.5;

    /// <summary>
    /// Maximum number of bytes to compact in a single run (0 = unlimited)
    /// </summary>
    public long MaxCompactionBytes { get; init; } = 0;

    public static CompactionConfig Default => new();
}
