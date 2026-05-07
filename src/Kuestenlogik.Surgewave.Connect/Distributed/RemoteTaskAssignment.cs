namespace Kuestenlogik.Surgewave.Connect.Distributed;

/// <summary>
/// Message published to the config topic to assign a connector task to a remote worker.
/// The target worker subscribes to the config topic and starts the connector when it
/// receives an assignment matching its worker ID.
/// </summary>
public sealed class RemoteTaskAssignment
{
    /// <summary>
    /// Name of the connector instance to create.
    /// </summary>
    public required string ConnectorName { get; init; }

    /// <summary>
    /// Fully qualified connector class name.
    /// </summary>
    public required string ConnectorType { get; init; }

    /// <summary>
    /// ID of the worker that should execute this task.
    /// </summary>
    public required string WorkerId { get; init; }

    /// <summary>
    /// Full connector configuration including pipeline metadata.
    /// </summary>
    public required Dictionary<string, string> Config { get; init; }

    /// <summary>
    /// ID of the pipeline this assignment belongs to.
    /// </summary>
    public required string PipelineId { get; init; }

    /// <summary>
    /// ID of the pipeline node this assignment represents.
    /// </summary>
    public required string NodeId { get; init; }

    /// <summary>
    /// Timestamp when the assignment was created.
    /// </summary>
    public long Timestamp { get; init; }
}
