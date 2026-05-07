namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// A node in a <see cref="PipelineDefinition"/> representing a single connector instance
/// with its configuration and visual position in the pipeline editor.
/// </summary>
public record PipelineNode
{
    /// <summary>Unique identifier for this node within the pipeline.</summary>
    public required string Id { get; init; }

    /// <summary>The fully qualified connector type name (e.g., "FileStream", "PostgreSQL").</summary>
    public required string ConnectorType { get; init; }

    /// <summary>Configuration key-value pairs passed to the connector.</summary>
    public required Dictionary<string, string> Config { get; init; }

    /// <summary>X coordinate in the visual pipeline editor.</summary>
    public required double X { get; init; }

    /// <summary>Y coordinate in the visual pipeline editor.</summary>
    public required double Y { get; init; }

    /// <summary>Optional display label for the node.</summary>
    public string? Label { get; init; }

    /// <summary>Optional retry policy for this node's connector tasks.</summary>
    public RetryPolicy? RetryPolicy { get; init; }

    /// <summary>Placement strategy controlling where this node executes.</summary>
    public PlacementConfig? Placement { get; init; }

    /// <summary>If set, this node represents a sub-pipeline reference.</summary>
    public string? SubPipelineId { get; init; }

    /// <summary>JSON-serialized port mappings for sub-pipeline nodes.</summary>
    public string? PortMappingsJson { get; init; }
}
