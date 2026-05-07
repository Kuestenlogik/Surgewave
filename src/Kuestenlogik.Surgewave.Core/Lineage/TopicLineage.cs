namespace Kuestenlogik.Surgewave.Core.Lineage;

/// <summary>
/// Per-topic lineage view showing all upstream producers and downstream consumers.
/// </summary>
public sealed class TopicLineage
{
    /// <summary>The topic name this lineage view describes.</summary>
    public required string TopicName { get; init; }

    /// <summary>
    /// Upstream nodes that produce data into this topic (producers, source connectors).
    /// </summary>
    public IReadOnlyList<LineageNode> Upstream { get; init; } = [];

    /// <summary>
    /// Downstream nodes that consume data from this topic (consumers, streams apps, sink connectors).
    /// </summary>
    public IReadOnlyList<LineageNode> Downstream { get; init; } = [];

    /// <summary>Maximum depth of this topic in the lineage DAG.</summary>
    public int Depth { get; init; }
}
