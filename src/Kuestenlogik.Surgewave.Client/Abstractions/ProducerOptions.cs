using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Options for creating a producer via ISurgewaveClient.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public sealed class ProducerOptions<TKey, TValue>
{
    /// <summary>
    /// The key serializer. If not set, a default serializer will be used based on the type.
    /// </summary>
    public ISerializer<TKey>? KeySerializer { get; set; }

    /// <summary>
    /// The value serializer. If not set, a default serializer will be used based on the type.
    /// </summary>
    public ISerializer<TValue>? ValueSerializer { get; set; }

    /// <summary>
    /// Async key serializer for schema registry integration. Takes precedence over KeySerializer if set.
    /// </summary>
    public IAsyncSerializer<TKey>? AsyncKeySerializer { get; set; }

    /// <summary>
    /// Async value serializer for schema registry integration. Takes precedence over ValueSerializer if set.
    /// </summary>
    public IAsyncSerializer<TValue>? AsyncValueSerializer { get; set; }

    /// <summary>
    /// Maximum number of messages to batch before sending.
    /// </summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// Time to wait for more messages before sending a batch (milliseconds).
    /// </summary>
    public int LingerMs { get; set; } = 5;

    /// <summary>
    /// Request timeout in milliseconds.
    /// </summary>
    public int RequestTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Required acknowledgments: 0=none, 1=leader, -1=all replicas.
    /// </summary>
    public short RequiredAcks { get; set; } = 1;

    /// <summary>
    /// List of interceptors to apply to messages. Interceptors are invoked in order.
    /// </summary>
    public IList<IProducerInterceptor<TKey, TValue>> Interceptors { get; } = [];

    /// <summary>
    /// Callback invoked when a message delivery completes (success or failure).
    /// This is called asynchronously after the produce operation completes.
    /// </summary>
    public DeliveryHandler<TKey, TValue>? DeliveryHandler { get; set; }
}
