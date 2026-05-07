namespace Confluent.Kafka;

/// <summary>
/// Represents a Kafka topic, partition, and offset.
/// </summary>
public class TopicPartitionOffset : IEquatable<TopicPartitionOffset>
{
    /// <summary>
    /// Creates a new TopicPartitionOffset.
    /// </summary>
    public TopicPartitionOffset(string topic, Partition partition, Offset offset)
    {
        Topic = topic ?? throw new ArgumentNullException(nameof(topic));
        Partition = partition;
        Offset = offset;
    }

    /// <summary>
    /// Creates a new TopicPartitionOffset from a TopicPartition.
    /// </summary>
    public TopicPartitionOffset(TopicPartition topicPartition, Offset offset)
        : this(topicPartition.Topic, topicPartition.Partition, offset)
    {
    }

    /// <summary>
    /// The topic name.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// The partition.
    /// </summary>
    public Partition Partition { get; }

    /// <summary>
    /// The offset.
    /// </summary>
    public Offset Offset { get; }

    /// <summary>
    /// Gets the TopicPartition component.
    /// </summary>
    public TopicPartition TopicPartition => new(Topic, Partition);

    /// <summary>
    /// Optional error associated with this offset.
    /// </summary>
    public Error? Error { get; init; }

    /// <inheritdoc/>
    public bool Equals(TopicPartitionOffset? other) =>
        other is not null && Topic == other.Topic && Partition == other.Partition && Offset == other.Offset;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => Equals(obj as TopicPartitionOffset);

    /// <inheritdoc/>
    public override int GetHashCode() => HashCode.Combine(Topic, Partition, Offset);

    /// <inheritdoc/>
    public override string ToString() => $"{Topic} [{Partition}] @{Offset}";

    /// <summary>Equality operator.</summary>
    public static bool operator ==(TopicPartitionOffset? left, TopicPartitionOffset? right) =>
        left is null ? right is null : left.Equals(right);

    /// <summary>Inequality operator.</summary>
    public static bool operator !=(TopicPartitionOffset? left, TopicPartitionOffset? right) => !(left == right);
}
