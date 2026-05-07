namespace Kuestenlogik.Surgewave.Broker.CruiseControl;

/// <summary>
/// Represents the balance quality of a cluster across multiple metrics.
/// Each score is 0-100 where 100 means perfectly balanced.
/// </summary>
public sealed class BalanceScore
{
    /// <summary>
    /// Partition distribution balance score (0-100).
    /// 100 means all brokers have exactly the same number of partitions.
    /// </summary>
    public double PartitionBalance { get; init; }

    /// <summary>
    /// Leader distribution balance score (0-100).
    /// 100 means all brokers lead exactly the same number of partitions.
    /// </summary>
    public double LeaderBalance { get; init; }

    /// <summary>
    /// Disk usage distribution balance score (0-100).
    /// 100 means all brokers use exactly the same amount of disk.
    /// </summary>
    public double DiskBalance { get; init; }

    /// <summary>
    /// Network utilization distribution balance score (0-100).
    /// 100 means all brokers have exactly the same network load.
    /// </summary>
    public double NetworkBalance { get; init; }

    /// <summary>
    /// Weighted average of all individual balance scores.
    /// Weights: partitions 30%, leaders 25%, disk 25%, network 20%.
    /// </summary>
    public double OverallScore { get; init; }
}
