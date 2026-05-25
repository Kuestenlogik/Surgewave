using Kuestenlogik.Surgewave.Streams.Changelog;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Configuration for materializing a stream or table into a named state store.
/// Controls the store name, retention, caching, and changelog behavior.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public sealed class Materialized<TKey, TValue>
{
    /// <summary>Gets the state store name, or null for auto-generated names.</summary>
    public string? StoreName { get; private init; }

    /// <summary>Gets the retention period, or null for unlimited retention.</summary>
    public TimeSpan? Retention { get; private init; }

    /// <summary>Gets whether record caching is enabled for this store.</summary>
    public bool CachingEnabled { get; private init; } = true;

    /// <summary>Gets whether changelog logging is enabled for fault tolerance.</summary>
    public bool LoggingEnabled { get; private init; } = true;

    /// <summary>Gets the changelog topic configuration, or null for defaults.</summary>
    public ChangelogConfig? ChangelogConfig { get; private init; }

    /// <summary>Creates a materialization with the specified state store name.</summary>
    /// <param name="storeName">The name for the state store (used in interactive queries).</param>
    /// <returns>A new Materialized instance.</returns>
    public static Materialized<TKey, TValue> As(string storeName)
        => new() { StoreName = storeName };

    /// <summary>Creates a materialization with the specified retention period.</summary>
    /// <param name="retention">How long to retain data in the store.</param>
    /// <returns>A new Materialized instance.</returns>
    public static Materialized<TKey, TValue> With(TimeSpan retention)
        => new() { Retention = retention };

    /// <summary>Returns a new Materialized instance with the specified retention period.</summary>
    /// <param name="retention">How long to retain data in the store.</param>
    /// <returns>A new Materialized instance.</returns>
    public Materialized<TKey, TValue> WithRetention(TimeSpan retention)
        => new()
        {
            StoreName = StoreName,
            Retention = retention,
            CachingEnabled = CachingEnabled,
            LoggingEnabled = LoggingEnabled,
            ChangelogConfig = ChangelogConfig
        };

    /// <summary>Returns a new Materialized instance with caching disabled.</summary>
    /// <returns>A new Materialized instance.</returns>
    public Materialized<TKey, TValue> WithCachingDisabled()
        => new()
        {
            StoreName = StoreName,
            Retention = Retention,
            CachingEnabled = false,
            LoggingEnabled = LoggingEnabled,
            ChangelogConfig = ChangelogConfig
        };

    /// <summary>Returns a new Materialized instance with changelog logging disabled.</summary>
    /// <returns>A new Materialized instance.</returns>
    public Materialized<TKey, TValue> WithLoggingDisabled()
        => new()
        {
            StoreName = StoreName,
            Retention = Retention,
            CachingEnabled = CachingEnabled,
            LoggingEnabled = false,
            ChangelogConfig = new ChangelogConfig { Enabled = false }
        };

    /// <summary>
    /// Enables changelog logging with the specified configuration.
    /// </summary>
    public Materialized<TKey, TValue> WithLogging(ChangelogConfig config)
        => new()
        {
            StoreName = StoreName,
            Retention = Retention,
            CachingEnabled = CachingEnabled,
            LoggingEnabled = config.Enabled,
            ChangelogConfig = config
        };

    /// <summary>
    /// Sets the cleanup policy for the changelog topic.
    /// </summary>
    public Materialized<TKey, TValue> WithCleanupPolicy(CleanupPolicy policy)
        => new()
        {
            StoreName = StoreName,
            Retention = Retention,
            CachingEnabled = CachingEnabled,
            LoggingEnabled = LoggingEnabled,
            ChangelogConfig = new ChangelogConfig
            {
                Enabled = ChangelogConfig?.Enabled ?? true,
                Compacted = policy is CleanupPolicy.Compact or CleanupPolicy.CompactAndDelete,
                Retention = Retention,
                CleanupPolicy = policy
            }
        };
}
