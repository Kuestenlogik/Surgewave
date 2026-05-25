namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Export format for pipelines with metadata for portability.
/// </summary>
public record PipelineExportFormat
{
    /// <summary>
    /// Format version for compatibility.
    /// </summary>
    public required string Version { get; init; }

    /// <summary>
    /// Export timestamp.
    /// </summary>
    public required DateTimeOffset ExportedAt { get; init; }

    /// <summary>
    /// Surgewave version that created this export.
    /// </summary>
    public string? SurgewaveVersion { get; init; }

    /// <summary>
    /// The pipeline definition.
    /// </summary>
    public required PipelineExportData Pipeline { get; init; }
}

/// <summary>
/// Pipeline data within an export (excludes runtime fields like Id, Status).
/// </summary>
public record PipelineExportData
{
    /// <summary>
    /// Pipeline name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Pipeline description.
    /// </summary>
    public string? Description { get; init; }

    /// <summary>
    /// Nodes in the pipeline.
    /// </summary>
    public required List<PipelineNodeExport> Nodes { get; init; }

    /// <summary>
    /// Connections between nodes.
    /// </summary>
    public required List<PipelineConnectionExport> Connections { get; init; }
}

/// <summary>
/// Node data for export (uses stable IDs for reference).
/// </summary>
public record PipelineNodeExport
{
    /// <summary>
    /// Stable node identifier within the pipeline.
    /// </summary>
    public required string NodeId { get; init; }

    /// <summary>
    /// Connector type class name.
    /// </summary>
    public required string ConnectorType { get; init; }

    /// <summary>
    /// Node configuration.
    /// </summary>
    public required Dictionary<string, string> Config { get; init; }

    /// <summary>
    /// X position for visual layout.
    /// </summary>
    public required double X { get; init; }

    /// <summary>
    /// Y position for visual layout.
    /// </summary>
    public required double Y { get; init; }

    /// <summary>
    /// Display label.
    /// </summary>
    public string? Label { get; init; }
}

/// <summary>
/// Connection data for export.
/// </summary>
public record PipelineConnectionExport
{
    /// <summary>
    /// Source node ID.
    /// </summary>
    public required string SourceNodeId { get; init; }

    /// <summary>
    /// Target node ID.
    /// </summary>
    public required string TargetNodeId { get; init; }
}

/// <summary>
/// Request to import a pipeline.
/// </summary>
public record ImportPipelineRequest
{
    /// <summary>
    /// The export data to import.
    /// </summary>
    public required PipelineExportFormat Export { get; init; }

    /// <summary>
    /// Optional name override (uses export name if not specified).
    /// </summary>
    public string? NameOverride { get; init; }
}

/// <summary>
/// Predefined pipeline template.
/// </summary>
public record PipelineTemplate
{
    /// <summary>
    /// Template identifier.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Template display name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Template description.
    /// </summary>
    public required string Description { get; init; }

    /// <summary>
    /// Template category (e.g., "Data Integration", "Event Processing").
    /// </summary>
    public required string Category { get; init; }

    /// <summary>
    /// Template icon.
    /// </summary>
    public string? Icon { get; init; }

    /// <summary>
    /// The pipeline export data.
    /// </summary>
    public required PipelineExportData Pipeline { get; init; }
}
