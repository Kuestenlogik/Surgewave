namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Aggregated metrics for a pipeline.
/// </summary>
public record PipelineMetrics
{
    public required string PipelineId { get; init; }
    public DateTimeOffset StartedAt { get; init; }
    public long TotalRecordsProcessed { get; init; }
    public long TotalErrors { get; init; }
    public double RecordsPerSecond { get; init; }
    public required Dictionary<string, NodeMetrics> Nodes { get; init; }
    public Dictionary<string, ConnectionMetrics> Connections { get; init; } = new();
}

/// <summary>
/// Metrics for a single node within a pipeline.
/// </summary>
public record NodeMetrics
{
    public required string NodeId { get; init; }
    public long RecordsIn { get; init; }
    public long RecordsOut { get; init; }
    public long Errors { get; init; }
    public double AvgLatencyMs { get; init; }
    public double P50LatencyMs { get; init; }
    public double P95LatencyMs { get; init; }
    public double P99LatencyMs { get; init; }
    public long RetryAttempts { get; init; }
    public long RetryExhausted { get; init; }
}

/// <summary>
/// Metrics for a connection between pipeline nodes.
/// </summary>
public record ConnectionMetrics
{
    public required string ConnectionId { get; init; }
    public string? Topic { get; init; }
    public long QueueDepth { get; init; }
    public double ThroughputPerSec { get; init; }
}
