namespace Kuestenlogik.Surgewave.Broker.CruiseControl;

/// <summary>
/// Identifies the cluster balance metric being measured.
/// </summary>
public enum ImbalanceMetric
{
    /// <summary>Partition count distribution across brokers.</summary>
    Partitions,
    /// <summary>Leader partition distribution across brokers.</summary>
    Leaders,
    /// <summary>Disk usage distribution across brokers.</summary>
    Disk,
    /// <summary>Network throughput distribution across brokers.</summary>
    Network
}

/// <summary>
/// Describes a specific imbalance between two brokers for a particular metric.
/// </summary>
public sealed class ImbalanceDetail
{
    /// <summary>
    /// The metric that is imbalanced.
    /// </summary>
    public required ImbalanceMetric Metric { get; init; }

    /// <summary>
    /// The broker ID that is overloaded relative to the cluster average.
    /// </summary>
    public required int OverloadedBrokerId { get; init; }

    /// <summary>
    /// The broker ID that is underloaded relative to the cluster average.
    /// </summary>
    public required int UnderloadedBrokerId { get; init; }

    /// <summary>
    /// The imbalance percentage between the two brokers for this metric.
    /// </summary>
    public double ImbalancePercent { get; init; }

    /// <summary>
    /// Human-readable description of the imbalance.
    /// </summary>
    public string Description { get; init; } = "";
}
