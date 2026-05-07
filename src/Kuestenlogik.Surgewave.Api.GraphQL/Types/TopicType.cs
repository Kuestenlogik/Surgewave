namespace Kuestenlogik.Surgewave.Api.GraphQL.Types;

/// <summary>
/// Represents a Surgewave topic in the GraphQL schema.
/// </summary>
public sealed class TopicType
{
    /// <summary>
    /// Topic name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Number of partitions in this topic.
    /// </summary>
    public int PartitionCount { get; init; }

    /// <summary>
    /// Replication factor for this topic.
    /// </summary>
    public int ReplicationFactor { get; init; }

    /// <summary>
    /// Total number of messages across all partitions.
    /// </summary>
    public long MessageCount { get; init; }

    /// <summary>
    /// Whether this topic is a mirror (geo-replicated) topic.
    /// </summary>
    public bool IsMirror { get; init; }

    /// <summary>
    /// Whether this topic is read-only.
    /// </summary>
    public bool IsReadOnly { get; init; }

    /// <summary>
    /// When the topic was created.
    /// </summary>
    public DateTimeOffset CreatedAt { get; init; }
}
