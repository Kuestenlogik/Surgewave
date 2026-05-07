using Blazor.Diagrams.Core.Geometry;
using Blazor.Diagrams.Core.Models;

namespace Kuestenlogik.Surgewave.Control.Components.Pipeline;

/// <summary>
/// Validation state for a connector node.
/// </summary>
public enum NodeValidationState
{
    Valid,
    Warning,
    Error
}

/// <summary>
/// Custom node model for connector nodes in the pipeline diagram.
/// </summary>
public sealed class ConnectorNode : NodeModel
{
    public ConnectorNode(Point? position = null) : base(position)
    {
    }

    public required string NodeId { get; init; }
    public required string ConnectorType { get; init; }
    public string? Label { get; set; }
    public bool IsSource { get; init; }
    public bool IsSink { get; init; }
    public Dictionary<string, string> Config { get; init; } = new();

    /// <summary>
    /// Reference to another pipeline when this is a composite (sub-pipeline) node.
    /// </summary>
    public string? SubPipelineId { get; set; }

    /// <summary>
    /// Cached display name of the referenced sub-pipeline.
    /// </summary>
    public string? SubPipelineName { get; set; }

    /// <summary>
    /// Whether this node represents an embedded sub-pipeline.
    /// </summary>
    public bool IsComposite => !string.IsNullOrEmpty(SubPipelineId);

    /// <summary>
    /// Gets the display name for the node.
    /// </summary>
    public string DisplayName => Label ?? (IsComposite ? SubPipelineName ?? "Sub-Pipeline" : ExtractSimpleName(ConnectorType));

    /// <summary>
    /// Whether this is an AI/ML processing node.
    /// </summary>
    public bool IsAiNode => ConnectorType.Contains("Surgewave.AI.Nodes", StringComparison.OrdinalIgnoreCase)
        || ConnectorType.Contains("LlmNode", StringComparison.OrdinalIgnoreCase)
        || ConnectorType.Contains("EmbedderNode", StringComparison.OrdinalIgnoreCase)
        || ConnectorType.Contains("DocumentParserNode", StringComparison.OrdinalIgnoreCase)
        || ConnectorType.Contains("RetrieverNode", StringComparison.OrdinalIgnoreCase)
        || ConnectorType.Contains("PromptBuilderNode", StringComparison.OrdinalIgnoreCase)
        || ConnectorType.Contains("RerankerNode", StringComparison.OrdinalIgnoreCase)
        || ConnectorType.Contains("AgentNode", StringComparison.OrdinalIgnoreCase)
        || ConnectorType.Contains("ChatEndpointNode", StringComparison.OrdinalIgnoreCase)
        || ConnectorType.Contains("ChatResponseNode", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Whether this is a workflow control node.
    /// </summary>
    public bool IsWorkflowNode => ConnectorType.Contains("Workflow.", StringComparison.OrdinalIgnoreCase)
        || ConnectorType.Contains("LoopNode", StringComparison.OrdinalIgnoreCase)
        || ConnectorType.Contains("WaitForInputNode", StringComparison.OrdinalIgnoreCase)
        || ConnectorType.Contains("GateNode", StringComparison.OrdinalIgnoreCase)
        || ConnectorType.Contains("AccumulatorNode", StringComparison.OrdinalIgnoreCase)
        || ConnectorType.Contains("MultiOutputNode", StringComparison.OrdinalIgnoreCase)
        || ConnectorType.Contains("StateNode", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Gets the category (Sources/Sinks/Pipelines/AI/Workflow).
    /// </summary>
    public string Category => IsComposite ? "Pipelines"
        : IsAiNode ? "AI"
        : IsWorkflowNode ? "Workflow"
        : IsSource ? "Sources"
        : IsSink ? "Sinks"
        : "Other";

    /// <summary>
    /// Gets the status color based on node state.
    /// </summary>
    public string StatusColor { get; set; } = "default";

    /// <summary>
    /// Gets or sets the current status message.
    /// </summary>
    public string? StatusMessage { get; set; }

    /// <summary>
    /// Gets or sets whether the node is disabled (excluded from execution).
    /// </summary>
    public bool IsDisabled { get; set; }

    /// <summary>
    /// Gets or sets the validation state of the node.
    /// </summary>
    public NodeValidationState ValidationState { get; set; } = NodeValidationState.Valid;

    /// <summary>
    /// Gets or sets the validation message.
    /// </summary>
    public string? ValidationMessage { get; set; }

    // Live metrics (Feature 4)

    /// <summary>
    /// Current throughput in records per second.
    /// </summary>
    public double RecordsPerSec { get; set; }

    /// <summary>
    /// Total errors recorded by this node.
    /// </summary>
    public long TotalErrors { get; set; }

    /// <summary>
    /// P99 latency in milliseconds.
    /// </summary>
    public double P99LatencyMs { get; set; }

    /// <summary>
    /// Whether the node is currently running.
    /// </summary>
    public bool IsRunning { get; set; }

    /// <summary>
    /// ID of the worker executing this node. Null if running locally.
    /// </summary>
    public string? WorkerId { get; set; }

    // Debug (Feature 1)

    /// <summary>
    /// Whether this node has a breakpoint set.
    /// </summary>
    public bool HasBreakpoint { get; set; }

    /// <summary>
    /// Whether this node is currently paused at a breakpoint.
    /// </summary>
    public bool IsPaused { get; set; }

    /// <summary>
    /// Number of records queued while paused at breakpoint.
    /// </summary>
    public int QueueDepth { get; set; }

    // Error port (Feature 5)

    /// <summary>
    /// Whether this node has an error output port.
    /// </summary>
    public bool HasErrorPort { get; set; }

    /// <summary>
    /// Retry policy for this node.
    /// </summary>
    public Models.Pipeline.RetryPolicy? RetryPolicy { get; set; }

    /// <summary>
    /// Whether this node is highlighted for lineage visualization.
    /// </summary>
    public bool IsLineageHighlighted { get; set; }

    /// <summary>
    /// Placement configuration for worker assignment.
    /// </summary>
    public Models.Pipeline.NodePlacementConfig? Placement { get; set; }

    /// <summary>
    /// Port mappings JSON for composite (sub-pipeline) nodes.
    /// </summary>
    public string? PortMappingsJson { get; set; }

    /// <summary>
    /// Labels for input ports (sub-pipeline port mapping).
    /// </summary>
    public List<string> InputPortLabels { get; set; } = [];

    /// <summary>
    /// Labels for output ports (sub-pipeline port mapping).
    /// </summary>
    public List<string> OutputPortLabels { get; set; } = [];

    // Built-in Streams node types

    /// <summary>
    /// Whether this is a SQL Transform node.
    /// </summary>
    public bool IsSqlTransform => ConnectorType == SqlTransformType;

    /// <summary>
    /// Whether this is an Exception Handler node.
    /// </summary>
    public bool IsExceptionHandler => ConnectorType == ExceptionHandlerType;

    /// <summary>
    /// Whether this is a built-in Streams processing node.
    /// </summary>
    public bool IsStreamsNode => IsSqlTransform || IsExceptionHandler;

    /// <summary>
    /// SQL query result preview rows (debug mode).
    /// </summary>
    public List<string[]>? SqlResultPreview { get; set; }

    /// <summary>
    /// SQL query result column headers (debug mode).
    /// </summary>
    public string[]? SqlResultColumns { get; set; }

    /// <summary>
    /// Number of exceptions caught by the handler (debug mode).
    /// </summary>
    public long ExceptionsCaught { get; set; }

    /// <summary>
    /// Number of threads replaced by the handler (debug mode).
    /// </summary>
    public long ThreadsReplaced { get; set; }

    /// <summary>
    /// Last exception message (debug mode).
    /// </summary>
    public string? LastExceptionMessage { get; set; }

    public const string SqlTransformType = "surgewave.streams.SqlTransform";
    public const string ExceptionHandlerType = "surgewave.streams.ExceptionHandler";

    private static string ExtractSimpleName(string fullName)
    {
        var lastDot = fullName.LastIndexOf('.');
        var name = lastDot >= 0 ? fullName[(lastDot + 1)..] : fullName;

        if (name.EndsWith("Connector", StringComparison.OrdinalIgnoreCase))
        {
            name = name[..^9];
        }
        else if (name.EndsWith("Source", StringComparison.OrdinalIgnoreCase))
        {
            name = name[..^6];
        }
        else if (name.EndsWith("Sink", StringComparison.OrdinalIgnoreCase))
        {
            name = name[..^4];
        }

        return name;
    }
}
