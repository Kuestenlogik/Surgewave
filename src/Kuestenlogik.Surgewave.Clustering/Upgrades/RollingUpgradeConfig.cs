namespace Kuestenlogik.Surgewave.Clustering.Upgrades;

/// <summary>
/// Configuration for rolling upgrade operations.
/// Controls timeouts, concurrency, and safety checks during broker upgrades.
/// </summary>
public sealed class RollingUpgradeConfig
{
    /// <summary>
    /// Maximum time to wait for a graceful shutdown to complete.
    /// Includes leadership transfer, in-flight request draining, and connection closing.
    /// Default: 60 seconds.
    /// </summary>
    public TimeSpan GracefulShutdownTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Maximum time to wait for a single partition leadership transfer.
    /// Default: 30 seconds.
    /// </summary>
    public TimeSpan LeaderTransferTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Maximum time to wait for ISR recovery after a broker restarts.
    /// The next broker upgrade will not proceed until ISR is fully recovered or this timeout expires.
    /// Default: 5 minutes.
    /// </summary>
    public TimeSpan IsrRecoveryTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Whether to require full ISR recovery before proceeding to the next broker upgrade.
    /// When true, the next broker will not be upgraded until all partitions have a full ISR.
    /// Default: true.
    /// </summary>
    public bool RequireFullIsr { get; set; } = true;

    /// <summary>
    /// Maximum number of brokers that can be upgraded concurrently.
    /// Setting this higher than 1 is risky and may cause data loss if ISR shrinks too much.
    /// Default: 1 (upgrade one broker at a time).
    /// </summary>
    public int MaxConcurrentUpgrades { get; set; } = 1;
}
