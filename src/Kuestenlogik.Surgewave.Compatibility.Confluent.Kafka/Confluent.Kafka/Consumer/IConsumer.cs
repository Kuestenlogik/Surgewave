namespace Confluent.Kafka;

/// <summary>
/// Defines a high-level Apache Kafka consumer.
/// </summary>
/// <typeparam name="TKey">The message key type.</typeparam>
/// <typeparam name="TValue">The message value type.</typeparam>
public interface IConsumer<TKey, TValue> : IDisposable
{
    /// <summary>
    /// Gets the consumer name (for logging).
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Gets the consumer group ID.
    /// </summary>
    string? MemberId { get; }

    /// <summary>
    /// Gets the current partition assignment.
    /// </summary>
    List<TopicPartition> Assignment { get; }

    /// <summary>
    /// Gets the current topic subscription.
    /// </summary>
    List<string> Subscription { get; }

    /// <summary>
    /// Gets the consumer group metadata for transactional offset commits.
    /// </summary>
    IConsumerGroupMetadata ConsumerGroupMetadata { get; }

    /// <summary>
    /// Subscribe to one or more topics.
    /// </summary>
    /// <param name="topic">The topic to subscribe to.</param>
    void Subscribe(string topic);

    /// <summary>
    /// Subscribe to one or more topics.
    /// </summary>
    /// <param name="topics">The topics to subscribe to.</param>
    void Subscribe(IEnumerable<string> topics);

    /// <summary>
    /// Unsubscribe from all topics.
    /// </summary>
    void Unsubscribe();

    /// <summary>
    /// Manually assign partitions to this consumer.
    /// </summary>
    /// <param name="partitions">The partitions to assign.</param>
    void Assign(IEnumerable<TopicPartition> partitions);

    /// <summary>
    /// Manually assign partitions with starting offsets.
    /// </summary>
    /// <param name="partitions">The partitions with offsets to assign.</param>
    void Assign(IEnumerable<TopicPartitionOffset> partitions);

    /// <summary>
    /// Remove all partition assignments.
    /// </summary>
    void Unassign();

    /// <summary>
    /// Consume a single message (blocking).
    /// </summary>
    /// <param name="millisecondsTimeout">Maximum wait time in milliseconds.</param>
    /// <returns>The consume result, or null if timeout.</returns>
    ConsumeResult<TKey, TValue>? Consume(int millisecondsTimeout);

    /// <summary>
    /// Consume a single message (blocking).
    /// </summary>
    /// <param name="timeout">Maximum wait time.</param>
    /// <returns>The consume result, or null if timeout.</returns>
    ConsumeResult<TKey, TValue>? Consume(TimeSpan timeout);

    /// <summary>
    /// Consume a single message.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The consume result.</returns>
    ConsumeResult<TKey, TValue>? Consume(CancellationToken cancellationToken = default);

    /// <summary>
    /// Commit offsets for all consumed messages.
    /// </summary>
    /// <returns>The committed offsets.</returns>
    List<TopicPartitionOffset> Commit();

    /// <summary>
    /// Commit the offset of a specific message.
    /// </summary>
    /// <param name="result">The consume result to commit.</param>
    void Commit(ConsumeResult<TKey, TValue> result);

    /// <summary>
    /// Commit specific offsets.
    /// </summary>
    /// <param name="offsets">The offsets to commit.</param>
    void Commit(IEnumerable<TopicPartitionOffset> offsets);

    /// <summary>
    /// Seek to a specific offset.
    /// </summary>
    /// <param name="topicPartitionOffset">The position to seek to.</param>
    void Seek(TopicPartitionOffset topicPartitionOffset);

    /// <summary>
    /// Store an offset for later auto-commit.
    /// </summary>
    /// <param name="result">The consume result to store.</param>
    void StoreOffset(ConsumeResult<TKey, TValue> result);

    /// <summary>
    /// Store an offset for later auto-commit.
    /// </summary>
    /// <param name="offset">The offset to store.</param>
    void StoreOffset(TopicPartitionOffset offset);

    /// <summary>
    /// Pause consumption on partitions.
    /// </summary>
    /// <param name="partitions">The partitions to pause.</param>
    void Pause(IEnumerable<TopicPartition> partitions);

    /// <summary>
    /// Resume consumption on partitions.
    /// </summary>
    /// <param name="partitions">The partitions to resume.</param>
    void Resume(IEnumerable<TopicPartition> partitions);

    /// <summary>
    /// Get the committed offset for a partition.
    /// </summary>
    /// <param name="topicPartition">The partition.</param>
    /// <param name="timeout">Operation timeout.</param>
    /// <returns>The committed offset.</returns>
    Offset Committed(TopicPartition topicPartition, TimeSpan timeout);

    /// <summary>
    /// Get the committed offsets for partitions.
    /// </summary>
    /// <param name="partitions">The partitions.</param>
    /// <param name="timeout">Operation timeout.</param>
    /// <returns>The committed offsets.</returns>
    List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout);

    /// <summary>
    /// Get the current position for a partition.
    /// </summary>
    /// <param name="partition">The partition.</param>
    /// <returns>The current position.</returns>
    Offset Position(TopicPartition partition);

    /// <summary>
    /// Close the consumer, leaving the consumer group.
    /// </summary>
    void Close();

    /// <summary>
    /// Adds brokers to the consumer's broker list.
    /// </summary>
    /// <param name="brokers">Comma-separated broker addresses.</param>
    /// <returns>Number of brokers added.</returns>
    int AddBrokers(string brokers);

    /// <summary>
    /// Get watermark offsets for a partition.
    /// </summary>
    /// <param name="topicPartition">The partition.</param>
    /// <param name="timeout">Operation timeout.</param>
    /// <returns>Low and high watermarks.</returns>
    WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout);

    /// <summary>
    /// Query watermark offsets for a partition (cached).
    /// </summary>
    /// <param name="topicPartition">The partition.</param>
    /// <returns>Low and high watermarks.</returns>
    WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition);
}

/// <summary>
/// Watermark offsets (low and high) for a partition.
/// </summary>
public readonly struct WatermarkOffsets
{
    /// <summary>
    /// Creates new WatermarkOffsets.
    /// </summary>
    public WatermarkOffsets(Offset low, Offset high)
    {
        Low = low;
        High = high;
    }

    /// <summary>
    /// The low watermark (earliest available offset).
    /// </summary>
    public Offset Low { get; }

    /// <summary>
    /// The high watermark (next offset to be written).
    /// </summary>
    public Offset High { get; }

    /// <inheritdoc/>
    public override string ToString() => $"[{Low}, {High}]";
}
