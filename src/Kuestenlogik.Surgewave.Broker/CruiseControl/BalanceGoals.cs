namespace Kuestenlogik.Surgewave.Broker.CruiseControl;

/// <summary>
/// Defines the thresholds for what constitutes an "imbalanced" cluster.
/// Each metric has a maximum allowed imbalance percentage before Cruise Control triggers.
/// </summary>
public sealed class BalanceGoals
{
    /// <summary>
    /// Maximum allowed partition count imbalance percentage across brokers.
    /// If the most-loaded broker has more than this percentage above the average, it triggers.
    /// Default: 20%.
    /// </summary>
    public double MaxPartitionImbalancePercent { get; set; } = 20.0;

    /// <summary>
    /// Maximum allowed disk usage imbalance percentage across brokers.
    /// Default: 25%.
    /// </summary>
    public double MaxDiskImbalancePercent { get; set; } = 25.0;

    /// <summary>
    /// Maximum allowed leader count imbalance percentage across brokers.
    /// Default: 15%.
    /// </summary>
    public double MaxLeaderImbalancePercent { get; set; } = 15.0;

    /// <summary>
    /// Maximum allowed network utilization imbalance percentage across brokers.
    /// Default: 30%.
    /// </summary>
    public double MaxNetworkImbalancePercent { get; set; } = 30.0;

    /// <summary>
    /// Minimum number of partition moves in a plan before it is worth executing.
    /// Small plans with fewer moves than this threshold are suppressed.
    /// Default: 3.
    /// </summary>
    public int MinPartitionsToRebalance { get; set; } = 3;
}
