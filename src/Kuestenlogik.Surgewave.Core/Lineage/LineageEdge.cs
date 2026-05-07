namespace Kuestenlogik.Surgewave.Core.Lineage;

/// <summary>
/// A directed edge in the lineage graph representing a data flow relationship.
/// </summary>
public sealed class LineageEdge
{
    /// <summary>The ID of the source node.</summary>
    public required string SourceId { get; init; }

    /// <summary>The ID of the target node.</summary>
    public required string TargetId { get; init; }

    /// <summary>The type of relationship.</summary>
    public required LineageEdgeType Type { get; init; }

    /// <summary>When this edge was first observed.</summary>
    public required DateTime FirstSeen { get; init; }

    /// <summary>When this edge was most recently observed.</summary>
    public DateTime LastSeen { get; set; }
}
