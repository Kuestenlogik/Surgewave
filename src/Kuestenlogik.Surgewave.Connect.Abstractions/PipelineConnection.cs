namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Defines a connection between two nodes in a <see cref="PipelineDefinition"/>,
/// representing the flow of data from a source node to a target node.
/// </summary>
public record PipelineConnection
{
    /// <summary>Unique identifier for this connection.</summary>
    public required string Id { get; init; }

    /// <summary>The ID of the source node that produces data.</summary>
    public required string SourceNodeId { get; init; }

    /// <summary>The ID of the target node that receives data.</summary>
    public required string TargetNodeId { get; init; }

    /// <summary>The internal Surgewave topic used to transport data between nodes. Auto-generated if not set.</summary>
    public string? InternalTopic { get; init; }

    /// <summary>The connection type (normal data flow, error routing, etc.).</summary>
    public PipelineConnectionType Type { get; init; } = PipelineConnectionType.Normal;
}
