namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Represents a single pipeline execution.
/// </summary>
public record ExecutionRecord
{
    /// <summary>
    /// Unique execution ID.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Pipeline ID.
    /// </summary>
    public required string PipelineId { get; init; }

    /// <summary>
    /// Pipeline name at time of execution.
    /// </summary>
    public required string PipelineName { get; init; }

    /// <summary>
    /// Execution status.
    /// </summary>
    public ExecutionStatus Status { get; init; }

    /// <summary>
    /// When the execution started.
    /// </summary>
    public DateTimeOffset StartedAt { get; init; }

    /// <summary>
    /// When the execution completed (null if still running).
    /// </summary>
    public DateTimeOffset? CompletedAt { get; init; }

    /// <summary>
    /// Duration in milliseconds.
    /// </summary>
    public long? DurationMs => CompletedAt.HasValue
        ? (long)(CompletedAt.Value - StartedAt).TotalMilliseconds
        : null;

    /// <summary>
    /// Total records processed.
    /// </summary>
    public long RecordsProcessed { get; init; }

    /// <summary>
    /// Total records that failed.
    /// </summary>
    public long RecordsFailed { get; init; }

    /// <summary>
    /// Error message if failed.
    /// </summary>
    public string? Error { get; init; }

    /// <summary>
    /// Stack trace if failed.
    /// </summary>
    public string? StackTrace { get; init; }

    /// <summary>
    /// Node-level execution details.
    /// </summary>
    public List<NodeExecutionRecord> Nodes { get; init; } = [];

    /// <summary>
    /// Trigger type (manual, schedule, webhook).
    /// </summary>
    public string TriggerType { get; init; } = "manual";

    /// <summary>
    /// Trigger metadata (e.g., cron expression, webhook source).
    /// </summary>
    public Dictionary<string, string>? TriggerMetadata { get; init; }
}

/// <summary>
/// Execution record for a single node within a pipeline execution.
/// </summary>
public record NodeExecutionRecord
{
    /// <summary>
    /// Node ID within the pipeline.
    /// </summary>
    public required string NodeId { get; init; }

    /// <summary>
    /// Node label/name.
    /// </summary>
    public string? NodeLabel { get; init; }

    /// <summary>
    /// Connector type.
    /// </summary>
    public required string ConnectorType { get; init; }

    /// <summary>
    /// Node execution status.
    /// </summary>
    public ExecutionStatus Status { get; init; }

    /// <summary>
    /// Records received by this node.
    /// </summary>
    public long RecordsIn { get; init; }

    /// <summary>
    /// Records output by this node.
    /// </summary>
    public long RecordsOut { get; init; }

    /// <summary>
    /// Records that failed processing.
    /// </summary>
    public long RecordsFailed { get; init; }

    /// <summary>
    /// Processing duration in milliseconds.
    /// </summary>
    public long DurationMs { get; init; }

    /// <summary>
    /// Error message if failed.
    /// </summary>
    public string? Error { get; init; }

    /// <summary>
    /// Sample input records (for debugging).
    /// </summary>
    public List<SampleRecord>? SampleInputs { get; init; }

    /// <summary>
    /// Sample output records (for debugging).
    /// </summary>
    public List<SampleRecord>? SampleOutputs { get; init; }
}

/// <summary>
/// Sample record for debugging purposes.
/// </summary>
public record SampleRecord
{
    /// <summary>
    /// Record key (as string).
    /// </summary>
    public string? Key { get; init; }

    /// <summary>
    /// Record value (as string, truncated if large).
    /// </summary>
    public string? Value { get; init; }

    /// <summary>
    /// Record timestamp.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Whether the value was truncated.
    /// </summary>
    public bool Truncated { get; init; }
}

/// <summary>
/// Execution status.
/// </summary>
public enum ExecutionStatus
{
    /// <summary>
    /// Execution is pending/queued.
    /// </summary>
    Pending,

    /// <summary>
    /// Execution is currently running.
    /// </summary>
    Running,

    /// <summary>
    /// Execution completed successfully.
    /// </summary>
    Completed,

    /// <summary>
    /// Execution failed.
    /// </summary>
    Failed,

    /// <summary>
    /// Execution was cancelled.
    /// </summary>
    Cancelled
}
