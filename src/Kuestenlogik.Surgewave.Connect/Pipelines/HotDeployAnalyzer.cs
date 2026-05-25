namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Analyzes proposed pipeline changes to determine if they can be hot-deployed
/// without restarting the pipeline.
/// </summary>
public static class HotDeployAnalyzer
{
    public static HotDeployAnalysis Analyze(PipelineDefinition current, PipelineDefinition proposed)
    {
        var restartReasons = new List<string>();
        var configChanges = new List<NodeHotDeployConfigChange>();

        var currentNodeIds = current.Nodes.Select(n => n.Id).ToHashSet();
        var proposedNodeIds = proposed.Nodes.Select(n => n.Id).ToHashSet();

        // Check for added nodes
        var addedNodes = proposedNodeIds.Except(currentNodeIds).ToList();
        if (addedNodes.Count > 0)
            restartReasons.Add($"Nodes added: {string.Join(", ", addedNodes)}");

        // Check for removed nodes
        var removedNodes = currentNodeIds.Except(proposedNodeIds).ToList();
        if (removedNodes.Count > 0)
            restartReasons.Add($"Nodes removed: {string.Join(", ", removedNodes)}");

        // Check for connector type changes
        foreach (var currentNode in current.Nodes)
        {
            var proposedNode = proposed.Nodes.FirstOrDefault(n => n.Id == currentNode.Id);
            if (proposedNode == null) continue;

            if (currentNode.ConnectorType != proposedNode.ConnectorType)
            {
                restartReasons.Add($"Node '{currentNode.Id}' connector type changed");
            }
        }

        // Check for connection changes
        var currentConns = current.Connections
            .Select(c => $"{c.SourceNodeId}->{c.TargetNodeId}:{c.Type}")
            .ToHashSet();
        var proposedConns = proposed.Connections
            .Select(c => $"{c.SourceNodeId}->{c.TargetNodeId}:{c.Type}")
            .ToHashSet();

        if (!currentConns.SetEquals(proposedConns))
            restartReasons.Add("Pipeline topology changed");

        // Check for config-only changes (hot-deployable)
        foreach (var currentNode in current.Nodes)
        {
            var proposedNode = proposed.Nodes.FirstOrDefault(n => n.Id == currentNode.Id);
            if (proposedNode == null || currentNode.ConnectorType != proposedNode.ConnectorType)
                continue;

            var changes = new Dictionary<string, HotDeployConfigChange>();
            var allKeys = currentNode.Config.Keys.Union(proposedNode.Config.Keys).ToHashSet();

            foreach (var key in allKeys)
            {
                currentNode.Config.TryGetValue(key, out var oldVal);
                proposedNode.Config.TryGetValue(key, out var newVal);

                if (oldVal != newVal)
                {
                    changes[key] = new HotDeployConfigChange(oldVal, newVal);
                }
            }

            if (changes.Count > 0)
            {
                configChanges.Add(new NodeHotDeployConfigChange(currentNode.Id, changes));
            }
        }

        return new HotDeployAnalysis(
            restartReasons.Count > 0,
            restartReasons,
            configChanges);
    }
}

/// <summary>
/// Result of hot-deploy analysis.
/// </summary>
public sealed record HotDeployAnalysis(
    bool RequiresRestart,
    List<string> RestartReasons,
    List<NodeHotDeployConfigChange> HotDeployConfigChangedNodes)
{
    public bool IsHotDeployable => !RequiresRestart && HotDeployConfigChangedNodes.Count > 0;
}

public sealed record NodeHotDeployConfigChange(string NodeId, Dictionary<string, HotDeployConfigChange> ChangedKeys);
public sealed record HotDeployConfigChange(string? OldValue, string? NewValue);
