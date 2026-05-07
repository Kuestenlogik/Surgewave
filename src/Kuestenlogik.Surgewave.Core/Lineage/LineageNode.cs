namespace Kuestenlogik.Surgewave.Core.Lineage;

/// <summary>
/// A single node in the lineage graph representing a topic, producer, consumer,
/// streams application, or connector.
/// </summary>
public sealed class LineageNode
{
    /// <summary>Unique identifier for this node (e.g. "producer:my-client", "topic:orders").</summary>
    public required string Id { get; init; }

    /// <summary>Human-readable display name.</summary>
    public required string Name { get; init; }

    /// <summary>The category of this node.</summary>
    public required LineageNodeType Type { get; init; }

    /// <summary>Arbitrary metadata attached to this node.</summary>
    public Dictionary<string, string> Metadata { get; init; } = [];
}
