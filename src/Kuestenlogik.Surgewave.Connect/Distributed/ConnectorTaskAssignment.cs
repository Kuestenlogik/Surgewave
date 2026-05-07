namespace Kuestenlogik.Surgewave.Connect.Distributed;

/// <summary>
/// Represents the assignment of connector tasks to a worker.
/// </summary>
public sealed class ConnectorTaskAssignment
{
    /// <summary>
    /// Name of the connector.
    /// </summary>
    public required string ConnectorName { get; init; }

    /// <summary>
    /// ID of the worker assigned to run this connector's tasks.
    /// </summary>
    public required string WorkerId { get; init; }

    /// <summary>
    /// List of task IDs assigned to this worker.
    /// </summary>
    public IList<int> TaskIds { get; init; } = [];

    /// <summary>
    /// Generation number when this assignment was made.
    /// </summary>
    public int Generation { get; init; }

    /// <summary>
    /// Timestamp when the assignment was created.
    /// </summary>
    public long Timestamp { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}

/// <summary>
/// Status of a connector task.
/// </summary>
public sealed class ConnectorTaskStatus
{
    /// <summary>
    /// Connector name.
    /// </summary>
    public required string ConnectorName { get; init; }

    /// <summary>
    /// Task ID.
    /// </summary>
    public int TaskId { get; init; }

    /// <summary>
    /// Worker running this task.
    /// </summary>
    public required string WorkerId { get; init; }

    /// <summary>
    /// Current state of the task.
    /// </summary>
    public TaskState State { get; set; } = TaskState.Unassigned;

    /// <summary>
    /// Error message if the task is in failed state.
    /// </summary>
    public string? ErrorMessage { get; set; }

    /// <summary>
    /// Error trace if the task is in failed state.
    /// </summary>
    public string? ErrorTrace { get; set; }

    /// <summary>
    /// Last time the task was active.
    /// </summary>
    public long LastActive { get; set; }
}

/// <summary>
/// State of a connector task.
/// </summary>
public enum TaskState
{
    /// <summary>
    /// Task has not been assigned to a worker.
    /// </summary>
    Unassigned,

    /// <summary>
    /// Task is running normally.
    /// </summary>
    Running,

    /// <summary>
    /// Task is paused.
    /// </summary>
    Paused,

    /// <summary>
    /// Task has failed with an error.
    /// </summary>
    Failed,

    /// <summary>
    /// Task is being restarted.
    /// </summary>
    Restarting
}
