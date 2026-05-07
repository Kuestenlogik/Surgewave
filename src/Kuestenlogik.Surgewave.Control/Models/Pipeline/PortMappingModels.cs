namespace Kuestenlogik.Surgewave.Control.Models.Pipeline;

/// <summary>
/// Frontend mirror of SubPipelinePortMapping for sub-pipeline port mapping.
/// </summary>
public sealed record SubPipelinePortMappingModel
{
    public required string SubPipelineId { get; init; }
    public List<PortMappingModel> InputMappings { get; init; } = [];
    public List<PortMappingModel> OutputMappings { get; init; } = [];
}

/// <summary>
/// Maps a parent port to a sub-pipeline node.
/// </summary>
public sealed record PortMappingModel
{
    public required string ParentPortId { get; init; }
    public required string SubPipelineNodeId { get; init; }
    public string? SubNodeLabel { get; init; }
}
