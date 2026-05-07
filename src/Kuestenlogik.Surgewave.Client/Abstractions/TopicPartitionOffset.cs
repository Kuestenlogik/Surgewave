using System.Diagnostics.CodeAnalysis;

namespace Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Represents a topic-partition-offset combination.
/// </summary>
public readonly record struct TopicPartitionOffset
{
    /// <summary>
    /// The topic name.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// The partition number.
    /// </summary>
    public required int Partition { get; init; }

    /// <summary>
    /// The offset.
    /// </summary>
    public required long Offset { get; init; }

    /// <summary>
    /// Create a new TopicPartitionOffset.
    /// </summary>
    [SetsRequiredMembers]
    public TopicPartitionOffset(string topic, int partition, long offset)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
    }
}
