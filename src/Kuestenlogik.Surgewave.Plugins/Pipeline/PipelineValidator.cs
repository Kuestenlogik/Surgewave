namespace Kuestenlogik.Surgewave.Plugins.Pipeline;

/// <summary>
/// Validates pipeline DAG topology using node port constraints.
/// </summary>
public static class PipelineValidator
{
    /// <summary>
    /// Validates a pipeline definition.
    /// </summary>
    /// <param name="nodes">Nodes in the pipeline, keyed by node ID.</param>
    /// <param name="connections">Connections as (sourceNodeId, targetNodeId) pairs.</param>
    /// <returns>Validation result with errors and warnings.</returns>
    public static PipelineValidationResult Validate(
        IReadOnlyDictionary<string, IPipelineNode> nodes,
        IReadOnlyList<(string SourceId, string TargetId)> connections)
    {
        var errors = new List<string>();
        var warnings = new List<string>();

        if (nodes.Count == 0)
        {
            errors.Add("Pipeline has no nodes");
            return new PipelineValidationResult(errors, warnings);
        }

        // Track actual connections per node
        var incomingCount = new Dictionary<string, int>();
        var outgoingCount = new Dictionary<string, int>();
        foreach (var nodeId in nodes.Keys)
        {
            incomingCount[nodeId] = 0;
            outgoingCount[nodeId] = 0;
        }

        // Validate connections reference existing nodes
        foreach (var (sourceId, targetId) in connections)
        {
            if (!nodes.ContainsKey(sourceId))
            {
                errors.Add($"Connection references unknown source node '{sourceId}'");
                continue;
            }
            if (!nodes.ContainsKey(targetId))
            {
                errors.Add($"Connection references unknown target node '{targetId}'");
                continue;
            }

            outgoingCount[sourceId]++;
            incomingCount[targetId]++;
        }

        // Validate port constraints
        var startNodes = new List<string>();
        var endNodes = new List<string>();

        foreach (var (nodeId, node) in nodes)
        {
            var incoming = incomingCount[nodeId];
            var outgoing = outgoingCount[nodeId];

            // Source nodes (InputPorts=0) must not have incoming connections
            if (node.InputPorts == 0 && incoming > 0)
                errors.Add($"Node '{nodeId}' ({node.DisplayName}) has InputPorts=0 but receives {incoming} connection(s)");

            // Sink nodes (OutputPorts=0) must not have outgoing connections
            if (node.OutputPorts == 0 && outgoing > 0)
                errors.Add($"Node '{nodeId}' ({node.DisplayName}) has OutputPorts=0 but sends {outgoing} connection(s)");

            // Nodes with required inputs must have incoming connections
            if (node.InputPorts > 0 && incoming == 0)
                warnings.Add($"Node '{nodeId}' ({node.DisplayName}) expects input but has no incoming connections");

            // Nodes with required outputs must have outgoing connections
            if (node.OutputPorts > 0 && outgoing == 0)
                warnings.Add($"Node '{nodeId}' ({node.DisplayName}) expects output but has no outgoing connections");

            if (node.InputPorts == 0) startNodes.Add(nodeId);
            if (node.OutputPorts == 0) endNodes.Add(nodeId);
        }

        // Pipeline must have at least one start and one end node
        if (startNodes.Count == 0)
            errors.Add("Pipeline has no start node (a node with InputPorts=0)");
        if (endNodes.Count == 0)
            errors.Add("Pipeline has no end node (a node with OutputPorts=0)");

        // Cycle detection via DFS
        var cycleError = DetectCycles(nodes.Keys.ToList(), connections);
        if (cycleError != null)
            errors.Add(cycleError);

        return new PipelineValidationResult(errors, warnings);
    }

    private static string? DetectCycles(
        IReadOnlyList<string> nodeIds,
        IReadOnlyList<(string SourceId, string TargetId)> connections)
    {
        var adjacency = new Dictionary<string, List<string>>();
        foreach (var nodeId in nodeIds)
            adjacency[nodeId] = [];
        foreach (var (sourceId, targetId) in connections)
        {
            if (adjacency.TryGetValue(sourceId, out var neighbors))
                neighbors.Add(targetId);
        }

        var visited = new HashSet<string>();
        var inStack = new HashSet<string>();

        foreach (var nodeId in nodeIds)
        {
            if (HasCycleDfs(nodeId, adjacency, visited, inStack))
                return $"Pipeline contains a cycle involving node '{nodeId}'";
        }

        return null;
    }

    private static bool HasCycleDfs(
        string nodeId,
        Dictionary<string, List<string>> adjacency,
        HashSet<string> visited,
        HashSet<string> inStack)
    {
        if (inStack.Contains(nodeId)) return true;
        if (visited.Contains(nodeId)) return false;

        visited.Add(nodeId);
        inStack.Add(nodeId);

        foreach (var neighbor in adjacency[nodeId])
        {
            if (HasCycleDfs(neighbor, adjacency, visited, inStack))
                return true;
        }

        inStack.Remove(nodeId);
        return false;
    }
}

/// <summary>
/// Result of pipeline validation.
/// </summary>
public sealed record PipelineValidationResult(
    IReadOnlyList<string> Errors,
    IReadOnlyList<string> Warnings)
{
    /// <summary>Whether the pipeline is valid (no errors).</summary>
    public bool IsValid => Errors.Count == 0;
}
