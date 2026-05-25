namespace Kuestenlogik.Surgewave.Clustering.Upgrades;

/// <summary>
/// Tracks the overall state of a cluster-wide rolling upgrade operation.
/// </summary>
public sealed class RollingUpgradeState
{
    /// <summary>
    /// Whether a rolling upgrade is currently in progress.
    /// </summary>
    public bool InProgress { get; set; }

    /// <summary>
    /// The target version that brokers are being upgraded to.
    /// </summary>
    public BrokerVersion? TargetVersion { get; set; }

    /// <summary>
    /// Per-broker upgrade status entries.
    /// </summary>
    public List<BrokerUpgradeStatus> Brokers { get; set; } = [];

    /// <summary>
    /// When the rolling upgrade was started.
    /// </summary>
    public DateTimeOffset? StartedAt { get; set; }

    /// <summary>
    /// Current phase of the rolling upgrade.
    /// </summary>
    public UpgradePhase Phase { get; set; } = UpgradePhase.NotStarted;

    /// <summary>
    /// When the rolling upgrade completed (or failed).
    /// </summary>
    public DateTimeOffset? CompletedAt { get; set; }
}

/// <summary>
/// Tracks upgrade status for a single broker in the cluster.
/// </summary>
public sealed class BrokerUpgradeStatus
{
    /// <summary>
    /// The broker ID.
    /// </summary>
    public required int BrokerId { get; init; }

    /// <summary>
    /// The broker's current version.
    /// </summary>
    public required BrokerVersion Version { get; init; }

    /// <summary>
    /// Current upgrade state for this broker.
    /// </summary>
    public BrokerUpgradeState State { get; set; } = BrokerUpgradeState.Pending;

    /// <summary>
    /// When this broker was upgraded (if applicable).
    /// </summary>
    public DateTimeOffset? UpgradedAt { get; set; }
}

/// <summary>
/// The upgrade state of a single broker.
/// </summary>
public enum BrokerUpgradeState
{
    /// <summary>Broker has not been upgraded yet.</summary>
    Pending,

    /// <summary>Broker is shutting down gracefully (transferring leadership, draining connections).</summary>
    ShuttingDown,

    /// <summary>Broker binary is being upgraded.</summary>
    Upgrading,

    /// <summary>Broker is restarting with the new version.</summary>
    Restarting,

    /// <summary>Broker has been upgraded and verified (ISR recovered).</summary>
    Verified,

    /// <summary>Broker upgrade failed.</summary>
    Failed
}

/// <summary>
/// The overall phase of a rolling upgrade operation.
/// </summary>
public enum UpgradePhase
{
    /// <summary>No upgrade in progress.</summary>
    NotStarted,

    /// <summary>Running pre-flight compatibility checks.</summary>
    PreCheck,

    /// <summary>Upgrade is actively rolling through brokers.</summary>
    InProgress,

    /// <summary>All brokers upgraded, verifying cluster health.</summary>
    Verifying,

    /// <summary>Upgrade completed successfully.</summary>
    Completed,

    /// <summary>Upgrade failed or was aborted.</summary>
    Failed
}
