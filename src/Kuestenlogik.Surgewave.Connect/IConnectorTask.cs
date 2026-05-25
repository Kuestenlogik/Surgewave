namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Base interface for all connector tasks.
/// A task performs the actual data transfer work.
/// </summary>
public interface IConnectorTask : IDisposable
{
    /// <summary>
    /// Returns the task version.
    /// </summary>
    string Version { get; }

    /// <summary>
    /// Initialize the task with the provided context.
    /// </summary>
    void Initialize(TaskContext context);

    /// <summary>
    /// Start the task with the provided configuration.
    /// </summary>
    void Start(IDictionary<string, string> config);

    /// <summary>
    /// Stop the task.
    /// </summary>
    void Stop();
}
