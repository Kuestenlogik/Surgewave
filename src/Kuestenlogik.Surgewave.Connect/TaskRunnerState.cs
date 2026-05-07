namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// State of a task runner.
/// </summary>
public enum TaskRunnerState
{
    Unassigned,
    Running,
    Paused,
    Failed
}
