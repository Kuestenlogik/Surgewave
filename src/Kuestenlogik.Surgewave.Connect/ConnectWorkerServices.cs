namespace Kuestenlogik.Surgewave.Connect;

using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;
using Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Optional services available to the Connect worker and its tasks.
/// Injected via constructor — no static state.
/// </summary>
public sealed record ConnectWorkerServices
{
    /// <summary>Schema registry operations. Null if no schema registry is available.</summary>
    public ISchemaRegistryOperations? SchemaRegistry { get; init; }

    /// <summary>Pipeline metrics collector. Null for non-pipeline connectors.</summary>
    public PipelineMetricsCollector? MetricsCollector { get; init; }

    /// <summary>Pipeline debugger. Null for non-pipeline connectors.</summary>
    public PipelineDebugger? Debugger { get; init; }

    /// <summary>Default instance with no services.</summary>
    public static ConnectWorkerServices None { get; } = new();
}
