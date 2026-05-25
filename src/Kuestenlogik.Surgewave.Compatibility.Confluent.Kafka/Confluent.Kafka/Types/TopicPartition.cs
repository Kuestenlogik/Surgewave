namespace Confluent.Kafka;

/// <summary>
/// Represents a Kafka topic and partition.
/// </summary>
public class TopicPartition : IEquatable<TopicPartition>
{
    /// <summary>
    /// Creates a new TopicPartition.
    /// </summary>
    public TopicPartition(string topic, Partition partition)
    {
        Topic = topic ?? throw new ArgumentNullException(nameof(topic));
        Partition = partition;
    }

    /// <summary>
    /// The topic name.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// The partition.
    /// </summary>
    public Partition Partition { get; }

    /// <inheritdoc/>
    public bool Equals(TopicPartition? other) =>
        other is not null && Topic == other.Topic && Partition == other.Partition;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => Equals(obj as TopicPartition);

    /// <inheritdoc/>
    public override int GetHashCode() => HashCode.Combine(Topic, Partition);

    /// <inheritdoc/>
    public override string ToString() => $"{Topic} [{Partition}]";

    /// <summary>Equality operator.</summary>
    public static bool operator ==(TopicPartition? left, TopicPartition? right) =>
        left is null ? right is null : left.Equals(right);

    /// <summary>Inequality operator.</summary>
    public static bool operator !=(TopicPartition? left, TopicPartition? right) => !(left == right);
}
