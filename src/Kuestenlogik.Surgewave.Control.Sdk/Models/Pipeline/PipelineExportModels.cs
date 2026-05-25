namespace Kuestenlogik.Surgewave.Control.Models.Pipeline;

/// <summary>
/// Export format for pipelines with metadata for portability.
/// </summary>
public record PipelineExportFormat
{
    public string Version { get; init; } = "1.0";
    public DateTimeOffset ExportedAt { get; init; }
    public string? SurgewaveVersion { get; init; }
    public PipelineExportData Pipeline { get; init; } = null!;
}

/// <summary>
/// Pipeline data within an export.
/// </summary>
public record PipelineExportData
{
    public string Name { get; init; } = "";
    public string? Description { get; init; }
    public List<PipelineNodeExport> Nodes { get; init; } = [];
    public List<PipelineConnectionExport> Connections { get; init; } = [];
    public Dictionary<string, PipelineExportData>? SubPipelines { get; init; }
}

/// <summary>
/// Node data for export.
/// </summary>
public record PipelineNodeExport
{
    public string NodeId { get; init; } = "";
    public string ConnectorType { get; init; } = "";
    public Dictionary<string, string> Config { get; init; } = new();
    public double X { get; init; }
    public double Y { get; init; }
    public string? Label { get; init; }
    public string? SubPipelineId { get; init; }
}

/// <summary>
/// Connection data for export.
/// </summary>
public record PipelineConnectionExport
{
    public string SourceNodeId { get; init; } = "";
    public string TargetNodeId { get; init; } = "";
}

/// <summary>
/// Request to import a pipeline.
/// </summary>
public record ImportPipelineRequest
{
    public PipelineExportFormat Export { get; init; } = null!;
    public string? NameOverride { get; init; }
}

/// <summary>
/// Summary of a pipeline template.
/// </summary>
public record PipelineTemplateSummary
{
    public string Id { get; init; } = "";
    public string Name { get; init; } = "";
    public string Description { get; init; } = "";
    public string Category { get; init; } = "";
    public string? Icon { get; init; }
}

/// <summary>
/// Pipeline template with full data.
/// </summary>
public record PipelineTemplate
{
    public string Id { get; init; } = "";
    public string Name { get; init; } = "";
    public string Description { get; init; } = "";
    public string Category { get; init; } = "";
    public string? Icon { get; init; }
    public PipelineExportData Pipeline { get; init; } = null!;
}
