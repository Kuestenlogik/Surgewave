namespace Kuestenlogik.Surgewave.Broker.CruiseControl;

/// <summary>
/// A point-in-time snapshot of load metrics for a single broker.
/// Collected periodically by <see cref="LoadCollector"/> to feed balance analysis.
/// </summary>
public sealed class BrokerLoadSnapshot
{
    /// <summary>
    /// The broker ID this snapshot was collected from.
    /// </summary>
    public required int BrokerId { get; init; }

    /// <summary>
    /// Number of partitions assigned to this broker (as leader or follower).
    /// </summary>
    public int PartitionCount { get; init; }

    /// <summary>
    /// Number of partitions for which this broker is the leader.
    /// </summary>
    public int LeaderCount { get; init; }

    /// <summary>
    /// Total disk usage in bytes across all partition logs on this broker.
    /// </summary>
    public long DiskUsageBytes { get; init; }

    /// <summary>
    /// Current produce (ingress) rate in bytes per second.
    /// </summary>
    public double ProduceRateBytesPerSec { get; init; }

    /// <summary>
    /// Current consume (egress) rate in bytes per second.
    /// </summary>
    public double ConsumeRateBytesPerSec { get; init; }

    /// <summary>
    /// CPU utilization percentage (0-100).
    /// </summary>
    public double CpuPercent { get; init; }

    /// <summary>
    /// Network utilization percentage (0-100).
    /// </summary>
    public double NetworkUtilizationPercent { get; init; }

    /// <summary>
    /// When this snapshot was collected.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
}
