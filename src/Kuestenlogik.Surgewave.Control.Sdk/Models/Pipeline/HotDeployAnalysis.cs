namespace Kuestenlogik.Surgewave.Control.Models.Pipeline;

/// <summary>
/// Result of analyzing proposed pipeline changes for hot-deploy compatibility.
/// </summary>
public sealed record HotDeployAnalysis
{
    public bool RequiresRestart { get; init; }
    public List<string> RestartReasons { get; init; } = [];
    public List<NodeConfigChange> ConfigChangedNodes { get; init; } = [];
    public bool IsHotDeployable => !RequiresRestart && ConfigChangedNodes.Count > 0;
}

public sealed record NodeConfigChange
{
    public required string NodeId { get; init; }
    public Dictionary<string, ConfigChange> ChangedKeys { get; init; } = new();
}

public sealed record ConfigChange(string? OldValue, string? NewValue);

/// <summary>
/// Request to update a pipeline (used for hot-deploy analysis).
/// </summary>
public sealed record UpdatePipelineRequest
{
    public string? Name { get; init; }
    public string? Description { get; init; }
    public List<PipelineNode>? Nodes { get; init; }
    public List<PipelineConnection>? Connections { get; init; }
}

/// <summary>
/// Port information for sub-pipeline mapping.
/// </summary>
public sealed record SubPipelinePortInfo
{
    public required string PipelineId { get; init; }
    public List<PortInfo> InputPorts { get; init; } = [];
    public List<PortInfo> OutputPorts { get; init; } = [];
}

public sealed record PortInfo
{
    public required string NodeId { get; init; }
    public string? Label { get; init; }
    public required string ConnectorType { get; init; }
}
