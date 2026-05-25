namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Analyzes a pipeline definition to extract input/output ports for sub-pipeline mapping.
/// </summary>
public static class SubPipelineAnalyzer
{
    /// <summary>
    /// Identifies source nodes (no incoming connections) as input ports
    /// and sink nodes (no outgoing connections) as output ports.
    /// </summary>
    public static SubPipelinePortInfo AnalyzePorts(PipelineDefinition pipeline)
    {
        var nodeIds = pipeline.Nodes.Select(n => n.Id).ToHashSet(StringComparer.Ordinal);
        var targetNodeIds = pipeline.Connections
            .Select(c => c.TargetNodeId)
            .ToHashSet(StringComparer.Ordinal);
        var sourceNodeIds = pipeline.Connections
            .Select(c => c.SourceNodeId)
            .ToHashSet(StringComparer.Ordinal);

        var inputPorts = new List<SubPipelinePortInfo.PortInfo>();
        var outputPorts = new List<SubPipelinePortInfo.PortInfo>();

        foreach (var node in pipeline.Nodes)
        {
            var hasIncoming = targetNodeIds.Contains(node.Id);
            var hasOutgoing = sourceNodeIds.Contains(node.Id);

            // Input port: no incoming connections (entry point)
            if (!hasIncoming)
            {
                inputPorts.Add(new SubPipelinePortInfo.PortInfo
                {
                    NodeId = node.Id,
                    Label = node.Label ?? node.ConnectorType,
                    ConnectorType = node.ConnectorType
                });
            }

            // Output port: no outgoing connections (exit point)
            if (!hasOutgoing)
            {
                outputPorts.Add(new SubPipelinePortInfo.PortInfo
                {
                    NodeId = node.Id,
                    Label = node.Label ?? node.ConnectorType,
                    ConnectorType = node.ConnectorType
                });
            }
        }

        return new SubPipelinePortInfo
        {
            PipelineId = pipeline.Id ?? "",
            InputPorts = inputPorts,
            OutputPorts = outputPorts
        };
    }
}

/// <summary>
/// Port information for a sub-pipeline, identifying entry and exit nodes.
/// </summary>
public sealed record SubPipelinePortInfo
{
    public required string PipelineId { get; init; }
    public List<PortInfo> InputPorts { get; init; } = [];
    public List<PortInfo> OutputPorts { get; init; } = [];

    public sealed record PortInfo
    {
        public required string NodeId { get; init; }
        public string? Label { get; init; }
        public required string ConnectorType { get; init; }
    }
}
