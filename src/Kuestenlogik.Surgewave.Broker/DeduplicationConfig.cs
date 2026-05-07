using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Configuration for broker-level content-based deduplication.
/// Detects duplicate messages using XxHash64 content fingerprints
/// without requiring producer-side idempotence configuration.
/// </summary>
public sealed class DeduplicationConfig : IValidatableConfig
{
    /// <summary>
    /// Enable content-based deduplication globally.
    /// Individual topics can opt-in via topic config "surgewave.dedup.enabled=true".
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Maximum number of hash entries to track per partition.
    /// Oldest entries are evicted when this limit is reached.
    /// Default: 10,000.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxEntriesPerPartition { get; set; } = 10_000;

    /// <summary>
    /// Time window in milliseconds for deduplication.
    /// Entries older than this are cleaned up.
    /// Default: 300,000 ms (5 minutes).
    /// </summary>
    [Range(1, long.MaxValue)]
    public long WindowSizeMs { get; set; } = 300_000;

    /// <summary>
    /// Interval in milliseconds for cleaning up expired deduplication entries.
    /// Default: 10,000 ms (10 seconds).
    /// </summary>
    [Range(1, int.MaxValue)]
    public int CleanupIntervalMs { get; set; } = 10_000;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}
