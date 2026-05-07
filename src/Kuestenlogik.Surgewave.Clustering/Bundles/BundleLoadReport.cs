namespace Kuestenlogik.Surgewave.Clustering.Bundles;

/// <summary>
/// Aggregated load report across all bundles, used for hot-spot detection
/// and rebalancing decisions.
/// </summary>
public sealed class BundleLoadReport
{
    /// <summary>
    /// Per-bundle load metrics.
    /// </summary>
    public List<BundleLoad> Bundles { get; init; } = [];

    /// <summary>
    /// The bundle with the highest combined load, if any.
    /// </summary>
    public string? HottestBundleId { get; init; }

    /// <summary>
    /// Whether a split is recommended for the hottest bundle based on configured thresholds.
    /// </summary>
    public bool SplitRecommended { get; init; }
}

/// <summary>
/// Load metrics for a single bundle.
/// </summary>
public sealed class BundleLoad
{
    /// <summary>
    /// Bundle identifier.
    /// </summary>
    public required string BundleId { get; init; }

    /// <summary>
    /// Broker that currently owns this bundle.
    /// </summary>
    public int OwnerBrokerId { get; init; }

    /// <summary>
    /// Number of topics currently mapped to this bundle's hash range.
    /// </summary>
    public int TopicCount { get; init; }

    /// <summary>
    /// Current message throughput in messages per second.
    /// </summary>
    public long MessageRatePerSecond { get; set; }

    /// <summary>
    /// Current byte throughput in bytes per second.
    /// </summary>
    public long ByteRatePerSecond { get; set; }

    /// <summary>
    /// Total number of active sessions (producers + consumers) on topics in this bundle.
    /// </summary>
    public int SessionCount { get; set; }
}
