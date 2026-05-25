using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Options for creating a consumer via ISurgewaveClient.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public sealed class ConsumerOptions<TKey, TValue>
{
    /// <summary>
    /// The key deserializer. If not set, a default deserializer will be used based on the type.
    /// </summary>
    public IDeserializer<TKey>? KeyDeserializer { get; set; }

    /// <summary>
    /// The value deserializer. If not set, a default deserializer will be used based on the type.
    /// </summary>
    public IDeserializer<TValue>? ValueDeserializer { get; set; }

    /// <summary>
    /// Async key deserializer for schema registry integration. Takes precedence over KeyDeserializer if set.
    /// </summary>
    public IAsyncDeserializer<TKey>? AsyncKeyDeserializer { get; set; }

    /// <summary>
    /// Async value deserializer for schema registry integration. Takes precedence over ValueDeserializer if set.
    /// </summary>
    public IAsyncDeserializer<TValue>? AsyncValueDeserializer { get; set; }

    /// <summary>
    /// Consumer group ID. Required for consumer group semantics.
    /// </summary>
    public string? GroupId { get; set; }

    /// <summary>
    /// Where to start consuming if no committed offset exists.
    /// </summary>
    public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Latest;

    /// <summary>
    /// Whether to automatically commit offsets.
    /// </summary>
    public bool EnableAutoCommit { get; set; } = true;

    /// <summary>
    /// Auto commit interval in milliseconds.
    /// </summary>
    public int AutoCommitIntervalMs { get; set; } = 5000;

    /// <summary>
    /// Maximum time between polls before the consumer is considered dead (milliseconds).
    /// </summary>
    public int MaxPollIntervalMs { get; set; } = 300000;

    /// <summary>
    /// Session timeout for consumer group membership (milliseconds).
    /// </summary>
    public int SessionTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Transaction isolation level for consuming messages.
    /// ReadCommitted: Only return messages that have been committed (for exactly-once semantics).
    /// ReadUncommitted: Return all messages including uncommitted transactional messages.
    /// </summary>
    public IsolationLevel IsolationLevel { get; set; } = IsolationLevel.ReadUncommitted;

    /// <summary>
    /// List of interceptors to apply to messages. Interceptors are invoked in order.
    /// </summary>
    public IList<IConsumerInterceptor<TKey, TValue>> Interceptors { get; } = [];
}
