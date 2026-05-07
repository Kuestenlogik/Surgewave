namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Describes how parent pipeline ports map to sub-pipeline source/sink nodes.
/// </summary>
public sealed record SubPipelinePortMapping
{
    public required string SubPipelineId { get; init; }
    public List<PortMapping> InputMappings { get; init; } = [];
    public List<PortMapping> OutputMappings { get; init; } = [];
}

/// <summary>
/// Maps a parent-level port to a specific node within a sub-pipeline.
/// </summary>
public sealed record PortMapping
{
    public required string ParentPortId { get; init; }
    public required string SubPipelineNodeId { get; init; }
    public string? SubNodeLabel { get; init; }
}
