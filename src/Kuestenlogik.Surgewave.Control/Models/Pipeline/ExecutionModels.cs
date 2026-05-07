namespace Kuestenlogik.Surgewave.Control.Models.Pipeline;

/// <summary>
/// Represents a pipeline execution.
/// </summary>
public record ExecutionRecord
{
    public string Id { get; init; } = "";
    public string PipelineId { get; init; } = "";
    public string PipelineName { get; init; } = "";
    public ExecutionStatus Status { get; init; }
    public DateTimeOffset StartedAt { get; init; }
    public DateTimeOffset? CompletedAt { get; init; }
    public long? DurationMs { get; init; }
    public long RecordsProcessed { get; init; }
    public long RecordsFailed { get; init; }
    public string? Error { get; init; }
    public string? StackTrace { get; init; }
    public List<NodeExecutionRecord> Nodes { get; init; } = [];
    public string TriggerType { get; init; } = "manual";
    public Dictionary<string, string>? TriggerMetadata { get; init; }
}

/// <summary>
/// Node-level execution record.
/// </summary>
public record NodeExecutionRecord
{
    public string NodeId { get; init; } = "";
    public string? NodeLabel { get; init; }
    public string ConnectorType { get; init; } = "";
    public ExecutionStatus Status { get; init; }
    public long RecordsIn { get; init; }
    public long RecordsOut { get; init; }
    public long RecordsFailed { get; init; }
    public long DurationMs { get; init; }
    public string? Error { get; init; }
    public List<SampleRecord>? SampleInputs { get; init; }
    public List<SampleRecord>? SampleOutputs { get; init; }
}

/// <summary>
/// Sample record for debugging.
/// </summary>
public record SampleRecord
{
    public string? Key { get; init; }
    public string? Value { get; init; }
    public DateTimeOffset Timestamp { get; init; }
    public bool Truncated { get; init; }
    public Dictionary<string, string>? Headers { get; init; }
}

/// <summary>
/// Execution status.
/// </summary>
public enum ExecutionStatus
{
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled
}

/// <summary>
/// Response containing a list of executions.
/// </summary>
public record ExecutionListResponse
{
    public List<ExecutionRecord> Executions { get; init; } = [];
    public int TotalCount { get; init; }
    public bool HasMore { get; init; }
}

/// <summary>
/// Real-time execution stats.
/// </summary>
public record ExecutionStats
{
    public string ExecutionId { get; init; } = "";
    public string PipelineId { get; init; } = "";
    public DateTimeOffset StartedAt { get; init; }
    public long TotalRecordsIn { get; init; }
    public long TotalRecordsOut { get; init; }
    public long TotalRecordsFailed { get; init; }
    public List<NodeStats> NodeStats { get; init; } = [];
}

/// <summary>
/// Real-time node stats.
/// </summary>
public record NodeStats
{
    public string NodeId { get; init; } = "";
    public long RecordsIn { get; init; }
    public long RecordsOut { get; init; }
    public long RecordsFailed { get; init; }
}
