namespace Kuestenlogik.Surgewave.Clustering.Reassignment;

/// <summary>
/// Configuration for online partition reassignment operations.
/// Controls throttling, concurrency, and automatic rebalancing behaviour.
/// </summary>
public sealed class ReassignmentConfig
{
    /// <summary>
    /// Default throttle rate for data replication during reassignment (bytes/sec).
    /// Prevents reassignment traffic from saturating the network.
    /// Default: 50 MB/s.
    /// </summary>
    public int DefaultThrottleRateBytesPerSec { get; set; } = 50_000_000;

    /// <summary>
    /// Maximum number of partition reassignments that can execute concurrently.
    /// Limits resource consumption during large-scale rebalances.
    /// Default: 5.
    /// </summary>
    public int MaxConcurrentReassignments { get; set; } = 5;

    /// <summary>
    /// Interval between progress checks for running reassignments.
    /// Default: 10 seconds.
    /// </summary>
    public TimeSpan ProgressCheckInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Whether to automatically generate and execute a balance plan
    /// when a new broker joins the cluster.
    /// Default: false.
    /// </summary>
    public bool AutoRebalanceOnBrokerJoin { get; set; } = false;
}
