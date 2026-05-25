namespace Kuestenlogik.Surgewave.Core.Lineage;

/// <summary>
/// The complete data flow graph containing all tracked nodes and edges.
/// </summary>
public sealed class LineageGraph
{
    /// <summary>All nodes (topics, producers, consumers, streams apps, connectors).</summary>
    public IReadOnlyList<LineageNode> Nodes { get; init; } = [];

    /// <summary>All directed edges describing data flow relationships.</summary>
    public IReadOnlyList<LineageEdge> Edges { get; init; } = [];

    /// <summary>When this graph snapshot was generated.</summary>
    public DateTime GeneratedAt { get; init; }
}
