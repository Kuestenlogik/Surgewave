namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Context provided to tasks by the Connect runtime.
/// </summary>
public sealed class TaskContext
{
    /// <summary>
    /// The assigned partitions for this task (sink tasks only).
    /// </summary>
    public IReadOnlyCollection<TopicPartition>? AssignedPartitions { get; init; }

    /// <summary>
    /// Request offset storage for source connectors.
    /// </summary>
    public IOffsetStorageReader? OffsetStorageReader { get; init; }

    /// <summary>
    /// Raise an error for this task.
    /// </summary>
    public Action<Exception>? RaiseError { get; init; }

    /// <summary>
    /// Producer for tasks that need to write records back to Surgewave.
    /// Used by transform connectors.
    /// </summary>
    public IConnectProducer? Producer { get; init; }

    /// <summary>
    /// Schema registry operations for schema-aware nodes (e.g., SchemaDecodeNode).
    /// Null if no schema registry is available.
    /// </summary>
    public Kuestenlogik.Surgewave.Client.Native.Operations.Schema.ISchemaRegistryOperations? SchemaRegistry { get; init; }

    /// <summary>
    /// Metrics collector for pipeline performance tracking.
    /// </summary>
    public Kuestenlogik.Surgewave.Connect.Pipelines.PipelineMetricsCollector? MetricsCollector { get; init; }

    /// <summary>
    /// Debugger for pipeline breakpoints and stepping.
    /// </summary>
    public Kuestenlogik.Surgewave.Connect.Pipelines.PipelineDebugger? Debugger { get; init; }
}

/// <summary>
/// Interface for producing records to Surgewave from within a connector task.
/// </summary>
public interface IConnectProducer
{
    /// <summary>
    /// Produces a record to the specified topic.
    /// </summary>
    Task ProduceAsync(string topic, byte[]? key, byte[] value, CancellationToken cancellationToken = default);

    /// <summary>
    /// Produces a record with headers.
    /// </summary>
    Task ProduceAsync(string topic, byte[]? key, byte[] value, IDictionary<string, byte[]>? headers, CancellationToken cancellationToken = default);
}
