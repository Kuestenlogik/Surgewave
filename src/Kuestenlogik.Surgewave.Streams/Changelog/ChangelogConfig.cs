namespace Kuestenlogik.Surgewave.Streams.Changelog;

/// <summary>
/// Configuration for changelog-backed state stores.
/// </summary>
public sealed class ChangelogConfig
{
    /// <summary>
    /// Whether changelog logging is enabled for the store.
    /// </summary>
    public bool Enabled { get; init; } = true;

    /// <summary>
    /// Whether the changelog topic should be compacted.
    /// </summary>
    public bool Compacted { get; init; } = true;

    /// <summary>
    /// Retention period for the changelog topic.
    /// </summary>
    public TimeSpan? Retention { get; init; }

    /// <summary>
    /// The cleanup policy for the changelog topic.
    /// </summary>
    public CleanupPolicy CleanupPolicy { get; init; } = CleanupPolicy.Compact;

    public static ChangelogConfig Default => new();
    public static ChangelogConfig Disabled => new() { Enabled = false };
}

/// <summary>
/// Cleanup policy for changelog topics.
/// </summary>
public enum CleanupPolicy
{
    Compact,
    Delete,
    CompactAndDelete
}
