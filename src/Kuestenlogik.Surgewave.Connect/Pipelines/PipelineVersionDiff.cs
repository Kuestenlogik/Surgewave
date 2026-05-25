namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Diff between two pipeline versions.
/// </summary>
public record PipelineVersionDiff
{
    public required int FromVersion { get; init; }
    public required int ToVersion { get; init; }
    public required List<string> NodesAdded { get; init; }
    public required List<string> NodesRemoved { get; init; }
    public required List<string> NodesModified { get; init; }
    public required List<string> ConnectionsAdded { get; init; }
    public required List<string> ConnectionsRemoved { get; init; }
    public required List<ConfigChange> ConfigChanges { get; init; }

    /// <summary>
    /// Compute the diff between two pipeline definitions.
    /// </summary>
    public static PipelineVersionDiff Compute(int fromVersion, int toVersion, PipelineDefinition from, PipelineDefinition to)
    {
        var fromNodes = (from.Nodes ?? []).ToDictionary(n => n.Id);
        var toNodes = (to.Nodes ?? []).ToDictionary(n => n.Id);

        var nodesAdded = toNodes.Keys.Except(fromNodes.Keys).ToList();
        var nodesRemoved = fromNodes.Keys.Except(toNodes.Keys).ToList();
        var nodesModified = new List<string>();
        var configChanges = new List<ConfigChange>();

        // Check modified nodes (present in both)
        foreach (var nodeId in fromNodes.Keys.Intersect(toNodes.Keys))
        {
            var fromNode = fromNodes[nodeId];
            var toNode = toNodes[nodeId];
            var modified = false;

            if (fromNode.ConnectorType != toNode.ConnectorType)
            {
                modified = true;
            }

            // Compare configs
            var fromConfig = fromNode.Config ?? new Dictionary<string, string>();
            var toConfig = toNode.Config ?? new Dictionary<string, string>();
            var allKeys = fromConfig.Keys.Union(toConfig.Keys);

            foreach (var key in allKeys)
            {
                var hasFrom = fromConfig.TryGetValue(key, out var fromVal);
                var hasTo = toConfig.TryGetValue(key, out var toVal);

                if (hasFrom && hasTo && fromVal == toVal)
                    continue;

                modified = true;
                configChanges.Add(new ConfigChange
                {
                    NodeId = nodeId,
                    Key = key,
                    OldValue = hasFrom ? fromVal : null,
                    NewValue = hasTo ? toVal : null
                });
            }

            if (modified)
                nodesModified.Add(nodeId);
        }

        // Compare connections
        var fromConns = (from.Connections ?? []).Select(c => $"{c.SourceNodeId}->{c.TargetNodeId}").ToHashSet();
        var toConns = (to.Connections ?? []).Select(c => $"{c.SourceNodeId}->{c.TargetNodeId}").ToHashSet();

        var connectionsAdded = toConns.Except(fromConns).ToList();
        var connectionsRemoved = fromConns.Except(toConns).ToList();

        return new PipelineVersionDiff
        {
            FromVersion = fromVersion,
            ToVersion = toVersion,
            NodesAdded = nodesAdded,
            NodesRemoved = nodesRemoved,
            NodesModified = nodesModified,
            ConnectionsAdded = connectionsAdded,
            ConnectionsRemoved = connectionsRemoved,
            ConfigChanges = configChanges
        };
    }
}

/// <summary>
/// A single configuration change between versions.
/// </summary>
public record ConfigChange
{
    public required string NodeId { get; init; }
    public required string Key { get; init; }
    public string? OldValue { get; init; }
    public string? NewValue { get; init; }
}
