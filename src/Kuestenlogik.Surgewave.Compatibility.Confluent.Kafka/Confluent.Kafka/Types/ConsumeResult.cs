namespace Confluent.Kafka;

/// <summary>
/// The result of a consume operation.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public class ConsumeResult<TKey, TValue>
{
    /// <summary>
    /// The topic the message came from.
    /// </summary>
    public string Topic { get; init; } = string.Empty;

    /// <summary>
    /// The partition the message came from.
    /// </summary>
    public Partition Partition { get; init; }

    /// <summary>
    /// The offset of the message.
    /// </summary>
    public Offset Offset { get; init; }

    /// <summary>
    /// The timestamp of the message.
    /// </summary>
    public Timestamp Timestamp { get; init; }

    /// <summary>
    /// The consumed message.
    /// </summary>
    public Message<TKey, TValue>? Message { get; init; }

    /// <summary>
    /// The TopicPartitionOffset of the message.
    /// </summary>
    public TopicPartitionOffset TopicPartitionOffset => new(Topic, Partition, Offset);

    /// <summary>
    /// Whether this is an end-of-partition marker (no actual message).
    /// </summary>
    public bool IsPartitionEOF { get; init; }
}
