using Kuestenlogik.Surgewave.Client.Consumer;

namespace Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Unified consumer interface supporting both Surgewave Native and Kafka protocols.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public interface IConsumer<TKey, TValue> : IAsyncDisposable
{
    /// <summary>
    /// Subscribe to topics.
    /// </summary>
    /// <param name="topics">The topics to subscribe to.</param>
    void Subscribe(params string[] topics);

    /// <summary>
    /// Subscribe to topics asynchronously with full partition discovery.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="topics">The topics to subscribe to.</param>
    Task SubscribeAsync(CancellationToken cancellationToken = default, params string[] topics);

    /// <summary>
    /// Assign specific topic-partition with optional starting offset.
    /// </summary>
    /// <param name="topic">The topic.</param>
    /// <param name="partition">The partition.</param>
    /// <param name="offset">The starting offset (default: 0).</param>
    void Assign(string topic, int partition, long offset = 0);

    /// <summary>
    /// Consume a single message.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The consumed result, or null if no message is available.</returns>
    Task<ConsumeResult<TKey, TValue>?> ConsumeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Consume a single message with timeout.
    /// </summary>
    /// <param name="timeout">Maximum time to wait for a message.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The consumed result, or null if timeout.</returns>
    Task<ConsumeResult<TKey, TValue>?> ConsumeAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Seek to a specific offset.
    /// </summary>
    /// <param name="topic">The topic.</param>
    /// <param name="partition">The partition.</param>
    /// <param name="offset">The offset to seek to.</param>
    void Seek(string topic, int partition, long offset);

    /// <summary>
    /// Commit offsets for all assigned partitions (for consumer groups).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task CommitAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Commit a specific offset for a topic-partition.
    /// </summary>
    /// <param name="offset">The topic-partition-offset to commit.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task CommitAsync(TopicPartitionOffset offset, CancellationToken cancellationToken = default);

    /// <summary>
    /// Commit offsets for the consumed message (commits offset + 1).
    /// </summary>
    /// <param name="result">The consume result to commit.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task CommitAsync(ConsumeResult<TKey, TValue> result, CancellationToken cancellationToken = default);

    /// <summary>
    /// Batch commit multiple offsets at once.
    /// </summary>
    /// <param name="offsets">The offsets to commit.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task CommitAsync(IEnumerable<TopicPartitionOffset> offsets, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets whether the consumer is currently connected to the broker.
    /// </summary>
    bool IsConnected { get; }

    /// <summary>
    /// Gets the current partition assignment.
    /// </summary>
    IReadOnlyList<(string topic, int partition)> Assignment { get; }

    /// <summary>
    /// Gets the protocol type of this consumer.
    /// </summary>
    ProtocolType Protocol { get; }
}
