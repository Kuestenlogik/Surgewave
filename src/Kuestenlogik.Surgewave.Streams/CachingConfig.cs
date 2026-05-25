namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Configuration for state store record caching.
/// When enabled, writes are buffered in memory and flushed on commit.
/// </summary>
public sealed class CachingConfig
{
    /// <summary>
    /// Whether caching is enabled. Default: false.
    /// </summary>
    public bool Enabled { get; init; }

    /// <summary>
    /// Maximum number of entries per cache. When exceeded, the cache is flushed.
    /// Default: 10,000.
    /// </summary>
    public int MaxCacheSize { get; init; } = 10_000;

    /// <summary>
    /// Disabled caching configuration.
    /// </summary>
    public static CachingConfig Disabled => new() { Enabled = false };
}
