namespace Kuestenlogik.Surgewave.Broker.Serverless;

/// <summary>
/// Input metrics snapshot used by <see cref="ScaleDecisionEngine"/> to evaluate scaling decisions.
/// Captures a point-in-time view of cluster load indicators.
/// </summary>
public sealed record ScaleMetrics
{
    /// <summary>
    /// Number of currently active client connections across the cluster.
    /// </summary>
    public required int ActiveConnections { get; init; }

    /// <summary>
    /// Aggregate produce throughput in messages per second.
    /// </summary>
    public required double ProduceRatePerSecond { get; init; }

    /// <summary>
    /// Aggregate fetch throughput in messages per second.
    /// </summary>
    public required double FetchRatePerSecond { get; init; }

    /// <summary>
    /// Current CPU usage as a percentage (0-100).
    /// </summary>
    public required double CpuUsagePercent { get; init; }

    /// <summary>
    /// Total bytes in write buffers that have not yet been flushed to object storage.
    /// </summary>
    public required long UnflushedBytes { get; init; }

    /// <summary>
    /// Current number of active broker instances.
    /// </summary>
    public required int CurrentBrokerCount { get; init; }

    /// <summary>
    /// Timestamp when these metrics were collected.
    /// </summary>
    public required DateTimeOffset Timestamp { get; init; }
}
