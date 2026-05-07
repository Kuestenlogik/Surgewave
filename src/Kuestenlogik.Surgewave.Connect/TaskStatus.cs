namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Status of a task.
/// </summary>
public sealed class TaskStatus
{
    public int Id { get; init; }
    public required string State { get; init; }
    public required string WorkerId { get; init; }
    public string? Trace { get; init; }
}
