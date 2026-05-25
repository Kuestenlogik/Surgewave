namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Defines a data pipeline consisting of interconnected connector nodes.
/// Pipelines are the visual, composable unit of data flow in Surgewave Connect.
/// </summary>
public record PipelineDefinition
{
    /// <summary>Unique identifier for this pipeline.</summary>
    public required string Id { get; init; }

    /// <summary>Display name of the pipeline.</summary>
    public required string Name { get; init; }

    /// <summary>Optional description of the pipeline's purpose.</summary>
    public string? Description { get; init; }

    /// <summary>The connector nodes that make up this pipeline.</summary>
    public required List<PipelineNode> Nodes { get; init; }

    /// <summary>The connections between nodes defining data flow.</summary>
    public required List<PipelineConnection> Connections { get; init; }

    /// <summary>Current execution status of the pipeline.</summary>
    public PipelineStatus Status { get; init; }

    /// <summary>When the pipeline was created.</summary>
    public DateTimeOffset CreatedAt { get; init; }

    /// <summary>When the pipeline was last updated, if ever.</summary>
    public DateTimeOffset? UpdatedAt { get; init; }

    /// <summary>Error message if the pipeline is in a failed state.</summary>
    public string? Error { get; init; }

    /// <summary>User-defined parameters that can be referenced in node configurations.</summary>
    public Dictionary<string, string>? Parameters { get; init; }

    /// <summary>Optional schedule configuration for periodic pipeline execution.</summary>
    public ScheduleConfig? Schedule { get; init; }
}
