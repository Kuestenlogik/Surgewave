namespace Kuestenlogik.Surgewave.Connect.Eos;

/// <summary>
/// Interface for task runners that execute connector tasks.
/// Supports both at-least-once (standard) and exactly-once (EOS) semantics.
/// </summary>
public interface ITaskRunner : IDisposable
{
    /// <summary>
    /// Gets the task ID within the connector.
    /// </summary>
    int TaskId { get; }

    /// <summary>
    /// Gets the current state of the task runner.
    /// </summary>
    TaskRunnerState State { get; }

    /// <summary>
    /// Starts the task runner.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for graceful shutdown.</param>
    Task StartAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Stops the task runner and waits for completion.
    /// </summary>
    Task StopAsync();

    /// <summary>
    /// Pauses the task runner. The run loop will block at the next checkpoint.
    /// </summary>
    Task PauseAsync();

    /// <summary>
    /// Resumes a paused task runner.
    /// </summary>
    Task ResumeAsync();
}
