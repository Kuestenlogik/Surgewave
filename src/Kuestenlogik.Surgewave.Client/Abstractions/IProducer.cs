namespace Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Unified producer interface supporting both Surgewave Native and Kafka protocols.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public interface IProducer<TKey, TValue> : IAsyncDisposable
{
    /// <summary>
    /// Produce a message to a topic.
    /// </summary>
    /// <param name="topic">The topic to produce to.</param>
    /// <param name="key">The message key.</param>
    /// <param name="value">The message value.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Metadata about the produced record.</returns>
    Task<ProduceResult> ProduceAsync(
        string topic,
        TKey? key,
        TValue value,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Produce a message to a topic with headers.
    /// </summary>
    /// <param name="topic">The topic to produce to.</param>
    /// <param name="key">The message key.</param>
    /// <param name="value">The message value.</param>
    /// <param name="headers">Message headers.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Metadata about the produced record.</returns>
    Task<ProduceResult> ProduceAsync(
        string topic,
        TKey? key,
        TValue value,
        IReadOnlyDictionary<string, byte[]>? headers,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Produce a message to a specific partition.
    /// </summary>
    /// <param name="topic">The topic to produce to.</param>
    /// <param name="partition">The partition to produce to.</param>
    /// <param name="key">The message key.</param>
    /// <param name="value">The message value.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Metadata about the produced record.</returns>
    Task<ProduceResult> ProduceAsync(
        string topic,
        int partition,
        TKey? key,
        TValue value,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Produce a message to a specific partition with headers.
    /// </summary>
    /// <param name="topic">The topic to produce to.</param>
    /// <param name="partition">The partition to produce to.</param>
    /// <param name="key">The message key.</param>
    /// <param name="value">The message value.</param>
    /// <param name="headers">Message headers.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Metadata about the produced record.</returns>
    Task<ProduceResult> ProduceAsync(
        string topic,
        int partition,
        TKey? key,
        TValue value,
        IReadOnlyDictionary<string, byte[]>? headers,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Flush any pending messages.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task FlushAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the protocol type of this producer.
    /// </summary>
    ProtocolType Protocol { get; }
}
