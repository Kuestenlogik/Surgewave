namespace Confluent.Kafka;

/// <summary>
/// The result of a produce operation.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public class DeliveryResult<TKey, TValue>
{
    /// <summary>
    /// The topic the message was delivered to.
    /// </summary>
    public string Topic { get; init; } = string.Empty;

    /// <summary>
    /// The partition the message was delivered to.
    /// </summary>
    public Partition Partition { get; init; }

    /// <summary>
    /// The offset of the delivered message.
    /// </summary>
    public Offset Offset { get; init; }

    /// <summary>
    /// The timestamp of the delivered message.
    /// </summary>
    public Timestamp Timestamp { get; init; }

    /// <summary>
    /// The message that was delivered.
    /// </summary>
    public Message<TKey, TValue>? Message { get; init; }

    /// <summary>
    /// The TopicPartitionOffset of the delivered message.
    /// </summary>
    public TopicPartitionOffset TopicPartitionOffset => new(Topic, Partition, Offset);

    /// <summary>
    /// Any error that occurred during delivery.
    /// </summary>
    public Error? Error { get; init; }

    /// <summary>
    /// Whether the delivery was successful.
    /// </summary>
    public PersistenceStatus Status { get; init; } = PersistenceStatus.Persisted;
}

/// <summary>
/// Delivery report with callback support.
/// </summary>
public class DeliveryReport<TKey, TValue> : DeliveryResult<TKey, TValue>
{
}

/// <summary>
/// Message persistence status.
/// </summary>
public enum PersistenceStatus
{
    /// <summary>Message was not persisted.</summary>
    NotPersisted = 0,

    /// <summary>Message may have been persisted.</summary>
    PossiblyPersisted = 1,

    /// <summary>Message was persisted.</summary>
    Persisted = 2
}
