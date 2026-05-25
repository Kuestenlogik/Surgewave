namespace Confluent.Kafka;

/// <summary>
/// Defines a high-level Apache Kafka producer.
/// </summary>
/// <typeparam name="TKey">The message key type.</typeparam>
/// <typeparam name="TValue">The message value type.</typeparam>
public interface IProducer<TKey, TValue> : IDisposable
{
    /// <summary>
    /// Gets the producer name (for logging).
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Asynchronously produce a message to a topic.
    /// </summary>
    /// <param name="topic">The target topic.</param>
    /// <param name="message">The message to produce.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Delivery result with offset and partition info.</returns>
    Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        string topic,
        Message<TKey, TValue> message,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously produce a message to a specific partition.
    /// </summary>
    /// <param name="topicPartition">The target topic and partition.</param>
    /// <param name="message">The message to produce.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Delivery result with offset and partition info.</returns>
    Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        TopicPartition topicPartition,
        Message<TKey, TValue> message,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Produce a message asynchronously with a delivery handler callback.
    /// </summary>
    /// <param name="topic">The target topic.</param>
    /// <param name="message">The message to produce.</param>
    /// <param name="deliveryHandler">Called when delivery completes.</param>
    void Produce(
        string topic,
        Message<TKey, TValue> message,
        Action<DeliveryReport<TKey, TValue>>? deliveryHandler = null);

    /// <summary>
    /// Produce a message to a specific partition with a delivery handler.
    /// </summary>
    /// <param name="topicPartition">The target topic and partition.</param>
    /// <param name="message">The message to produce.</param>
    /// <param name="deliveryHandler">Called when delivery completes.</param>
    void Produce(
        TopicPartition topicPartition,
        Message<TKey, TValue> message,
        Action<DeliveryReport<TKey, TValue>>? deliveryHandler = null);

    /// <summary>
    /// Wait for all pending produce requests to complete.
    /// </summary>
    /// <param name="timeout">Maximum time to wait.</param>
    /// <returns>Number of messages still pending (0 if all flushed).</returns>
    int Flush(TimeSpan timeout);

    /// <summary>
    /// Wait for all pending produce requests to complete.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    void Flush(CancellationToken cancellationToken = default);

    /// <summary>
    /// Initialize transactions for exactly-once semantics.
    /// </summary>
    /// <param name="timeout">Initialization timeout.</param>
    void InitTransactions(TimeSpan timeout);

    /// <summary>
    /// Begin a new transaction.
    /// </summary>
    void BeginTransaction();

    /// <summary>
    /// Commit the current transaction.
    /// </summary>
    /// <param name="timeout">Commit timeout.</param>
    void CommitTransaction(TimeSpan timeout);

    /// <summary>
    /// Commit the current transaction.
    /// </summary>
    void CommitTransaction();

    /// <summary>
    /// Abort the current transaction.
    /// </summary>
    /// <param name="timeout">Abort timeout.</param>
    void AbortTransaction(TimeSpan timeout);

    /// <summary>
    /// Abort the current transaction.
    /// </summary>
    void AbortTransaction();

    /// <summary>
    /// Send offsets to transaction for exactly-once consuming.
    /// </summary>
    /// <param name="offsets">The offsets to send.</param>
    /// <param name="groupMetadata">Consumer group metadata.</param>
    /// <param name="timeout">Operation timeout.</param>
    void SendOffsetsToTransaction(
        IEnumerable<TopicPartitionOffset> offsets,
        IConsumerGroupMetadata groupMetadata,
        TimeSpan timeout);

    /// <summary>
    /// Adds one or more brokers to the producer's list of initial bootstrap brokers.
    /// </summary>
    /// <param name="brokers">Comma-separated broker addresses.</param>
    /// <returns>Number of brokers added.</returns>
    int AddBrokers(string brokers);
}

/// <summary>
/// Consumer group metadata for transactional offset commits.
/// </summary>
public interface IConsumerGroupMetadata
{
    /// <summary>
    /// The consumer group ID.
    /// </summary>
    string GroupId { get; }
}
