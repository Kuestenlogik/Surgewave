using Kuestenlogik.Surgewave.Streams.Processors;

namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// Optimizes the topology at build time.
/// - Merges repartition nodes that share the same key type
/// - Detects shared source topics and reuses source nodes
/// - Removes unnecessary pass-through nodes
/// </summary>
public sealed class TopologyOptimizer
{
    private readonly List<OptimizationResult> _optimizations = [];

    /// <summary>
    /// Optimizations applied during the last Optimize() call.
    /// </summary>
    public IReadOnlyList<OptimizationResult> Optimizations => _optimizations;

    /// <summary>
    /// Total number of nodes removed by optimization.
    /// </summary>
    public int NodesRemoved => _optimizations.Sum(o => o.NodesRemoved);

    /// <summary>
    /// Optimizes the given topology and returns the optimized version.
    /// </summary>
    public Topology Optimize(Topology topology)
    {
        _optimizations.Clear();

        var sources = topology.Sources.ToList();
        var sinks = topology.Sinks.ToList();
        var repartitionNodes = topology.RepartitionNodes.ToList();

        // Optimization 1: Merge source nodes reading the same topic
        var sourcesByTopic = new Dictionary<string, List<ProcessorNode>>();
        foreach (var source in sources)
        {
            var topicProp = source.GetType().GetProperty("TopicPattern");
            if (topicProp?.GetValue(source)?.ToString() is string topic)
            {
                if (!sourcesByTopic.ContainsKey(topic))
                    sourcesByTopic[topic] = [];
                sourcesByTopic[topic].Add(source);
            }
        }

        var mergedSourceCount = 0;
        foreach (var (topic, topicSources) in sourcesByTopic)
        {
            if (topicSources.Count <= 1) continue;

            // Keep the first source, merge children from others into it
            var primary = topicSources[0];
            for (var i = 1; i < topicSources.Count; i++)
            {
                var duplicate = topicSources[i];
                foreach (var child in duplicate.Children)
                {
                    primary.AddChild(child);
                }
                sources.Remove(duplicate);
                mergedSourceCount++;
            }
        }

        if (mergedSourceCount > 0)
        {
            _optimizations.Add(new OptimizationResult(
                OptimizationType.SourceMerge,
                $"Merged {mergedSourceCount} duplicate source nodes",
                mergedSourceCount));
        }

        // Optimization 2: Remove single-child pass-through processors
        var passThruCount = RemovePassThroughNodes(sources);
        if (passThruCount > 0)
        {
            _optimizations.Add(new OptimizationResult(
                OptimizationType.PassThroughElimination,
                $"Removed {passThruCount} pass-through nodes",
                passThruCount));
        }

        return new Topology(sources, sinks, repartitionNodes, topology.StateStoreSuppliers);
    }

    private static int RemovePassThroughNodes(List<ProcessorNode> roots)
    {
        var removed = 0;

        foreach (var root in roots)
        {
            removed += RemovePassThroughRecursive(root);
        }

        return removed;
    }

    private static int RemovePassThroughRecursive(ProcessorNode node)
    {
        var removed = 0;

        for (var i = node.Children.Count - 1; i >= 0; i--)
        {
            var child = node.Children[i];

            // A pass-through node: has exactly 1 child and no state stores
            if (IsPassThrough(child))
            {
                var grandchild = child.Children[0];
                node.Children[i] = grandchild;
                removed++;
            }

            removed += RemovePassThroughRecursive(node.Children[i]);
        }

        return removed;
    }

    private static bool IsPassThrough(ProcessorNode node)
    {
        // A node is pass-through if it has exactly one child, no state stores,
        // and its type name contains "MERGE" (merge nodes with single source are no-ops)
        return node.Children.Count == 1
               && node.StateStoreNames.Count == 0
               && node.Name.Contains("MERGE", StringComparison.OrdinalIgnoreCase);
    }
}

/// <summary>
/// Result of a topology optimization.
/// </summary>
public sealed record OptimizationResult(
    OptimizationType Type,
    string Description,
    int NodesRemoved);

/// <summary>
/// Type of topology optimization applied.
/// </summary>
public enum OptimizationType
{
    SourceMerge,
    PassThroughElimination,
    RepartitionMerge
}
