namespace Kuestenlogik.Surgewave.Control.Models.Pipeline;

/// <summary>
/// Response model for pipeline metrics from the broker API.
/// </summary>
public record PipelineMetricsResponse
{
    public required string PipelineId { get; init; }
    public long TotalRecords { get; init; }
    public long TotalErrors { get; init; }
    public double RecordsPerSec { get; init; }
    public Dictionary<string, NodeMetricsInfo> Nodes { get; init; } = new();
    public Dictionary<string, ConnectionMetricsInfo> Connections { get; init; } = new();
}

/// <summary>
/// Metrics for a single node.
/// </summary>
public record NodeMetricsInfo
{
    public required string NodeId { get; init; }
    public long RecordsIn { get; init; }
    public long RecordsOut { get; init; }
    public long Errors { get; init; }
    public double AvgLatencyMs { get; init; }
    public double P99LatencyMs { get; init; }
    public double RecordsPerSec { get; init; }
}

/// <summary>
/// Metrics for a single connection between nodes.
/// </summary>
public record ConnectionMetricsInfo
{
    public required string ConnectionId { get; init; }
    public string? Topic { get; init; }
    public long QueueDepth { get; init; }
    public double ThroughputPerSec { get; init; }
    public BackPressureLevel Level { get; init; }
}

/// <summary>
/// Back-pressure severity level.
/// </summary>
public enum BackPressureLevel
{
    Normal,
    Warning,
    Critical
}
