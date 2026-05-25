using Kuestenlogik.Surgewave.Client.Native;

namespace Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Unified client interface for Surgewave/Kafka messaging.
/// Creates producers and consumers that use the configured protocol.
/// </summary>
public interface ISurgewaveClient : IAsyncDisposable
{
    /// <summary>
    /// Gets the protocol type of this client.
    /// </summary>
    ProtocolType Protocol { get; }

    /// <summary>
    /// Gets the bootstrap servers this client is connected to.
    /// </summary>
    string BootstrapServers { get; }

    /// <summary>
    /// Gets whether the client is currently connected.
    /// </summary>
    bool IsConnected { get; }

    /// <summary>
    /// Gets the client ID used for this client.
    /// </summary>
    string? ClientId { get; }

    /// <summary>
    /// Gets the underlying native client for advanced operations like transactions.
    /// Returns null if not connected or if using Kafka protocol.
    /// </summary>
    SurgewaveNativeClient? NativeClient { get; }

    /// <summary>
    /// Connect to the broker(s).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task ConnectAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Create a producer with optional configuration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>A producer instance.</returns>
    IProducer<TKey, TValue> CreateProducer<TKey, TValue>(
        Action<ProducerOptions<TKey, TValue>>? configure = null);

    /// <summary>
    /// Create a consumer with optional configuration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>A consumer instance.</returns>
    IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(
        Action<ConsumerOptions<TKey, TValue>>? configure = null);
}
