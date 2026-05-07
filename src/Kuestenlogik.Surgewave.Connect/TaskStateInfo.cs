namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Task state information.
/// </summary>
public sealed class TaskStateInfo
{
    /// <summary>
    /// Task ID.
    /// </summary>
    public required int Id { get; init; }

    /// <summary>
    /// Current state of the task.
    /// </summary>
    public required string State { get; init; }

    /// <summary>
    /// Worker ID running this task.
    /// </summary>
    public required string WorkerId { get; init; }

    /// <summary>
    /// Error trace if the task has failed.
    /// </summary>
    public string? Trace { get; init; }
}
