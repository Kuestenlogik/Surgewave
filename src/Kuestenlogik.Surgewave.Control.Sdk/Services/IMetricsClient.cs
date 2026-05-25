namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Client for fetching real-time metrics from Surgewave broker.
/// </summary>
public interface IMetricsClient
{
    /// <summary>
    /// Fetches current metrics from the broker's Prometheus endpoint.
    /// </summary>
    Task<MetricsSnapshot> GetMetricsAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Per-topic metrics.
/// </summary>
public sealed record TopicMetrics
{
    public string Topic { get; init; } = "";
    public double MessagesProducedTotal { get; init; }
    public double MessagesFetchedTotal { get; init; }
    public double BytesProducedTotal { get; init; }
    public double BytesFetchedTotal { get; init; }
}

/// <summary>
/// Snapshot of broker metrics at a point in time.
/// </summary>
public sealed record MetricsSnapshot
{
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;

    // Throughput
    public double MessagesProducedTotal { get; init; }
    public double MessagesFetchedTotal { get; init; }
    public double BytesProducedTotal { get; init; }
    public double BytesFetchedTotal { get; init; }

    // Latency (from histograms)
    public double ProduceLatencyP50 { get; init; }
    public double ProduceLatencyP90 { get; init; }
    public double ProduceLatencyP99 { get; init; }
    public double FetchLatencyP50 { get; init; }
    public double FetchLatencyP90 { get; init; }
    public double FetchLatencyP99 { get; init; }

    // Connections
    public int ActiveConnections { get; init; }
    public long ConnectionsTotal { get; init; }

    // Topics & Partitions
    public int TopicCount { get; init; }
    public int PartitionCount { get; init; }
    public long TotalLogSizeBytes { get; init; }

    // Consumer Groups
    public int ActiveConsumerGroups { get; init; }
    public long MaxConsumerLag { get; init; }

    // Transactions
    public int ActiveTransactions { get; init; }
    public long TransactionsTotal { get; init; }
    public long TransactionCommitsTotal { get; init; }
    public long TransactionAbortsTotal { get; init; }

    // Errors
    public long ErrorsTotal { get; init; }
    public long ProduceErrorsTotal { get; init; }
    public long ThrottledRequestsTotal { get; init; }

    // Requests
    public long RequestsTotal { get; init; }

    // Per-topic metrics
    public IReadOnlyList<TopicMetrics> TopicMetrics { get; init; } = [];
}
