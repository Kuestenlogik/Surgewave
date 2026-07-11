using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Security;

namespace Kuestenlogik.Surgewave.Client.Kafka;

/// <summary>
/// Kafka protocol client implementation.
/// Creates producers and consumers that use the Kafka wire protocol for compatibility
/// with real Kafka clusters or when Kafka protocol is required.
/// </summary>
public sealed class KafkaClient : ISurgewaveClient
{
    private readonly string _bootstrapServers;
    private readonly string? _clientId;
    private readonly SslOptions? _ssl;
    private readonly SaslOptions? _sasl;
    private bool _disposed;
    private bool _connected;

    /// <inheritdoc />
    public ProtocolType Protocol => ProtocolType.Kafka;

    /// <inheritdoc />
    public string BootstrapServers => _bootstrapServers;

    /// <inheritdoc />
    public bool IsConnected => _connected;

    /// <inheritdoc />
    public string? ClientId => _clientId;

    /// <inheritdoc />
    /// <remarks>
    /// Kafka protocol does not use Surgewave native client.
    /// For transactions with Kafka protocol, use Kafka's built-in transaction APIs.
    /// </remarks>
    public SurgewaveNativeClient? NativeClient => null;

    internal KafkaClient(string bootstrapServers, string? clientId, SslOptions? ssl = null, SaslOptions? sasl = null)
    {
        _bootstrapServers = bootstrapServers;
        _clientId = clientId;
        _ssl = ssl;
        _sasl = sasl;
    }

    /// <inheritdoc />
    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        // Kafka protocol connects lazily on first produce/consume
        _connected = true;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public IProducer<TKey, TValue> CreateProducer<TKey, TValue>(
        Action<ProducerOptions<TKey, TValue>>? configure = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var options = new ProducerOptions<TKey, TValue>();
        configure?.Invoke(options);

        return new GenericKafkaProducer<TKey, TValue>(
            _bootstrapServers,
            _clientId,
            options,
            _ssl,
            _sasl);
    }

    /// <inheritdoc />
    public IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(
        Action<ConsumerOptions<TKey, TValue>>? configure = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var options = new ConsumerOptions<TKey, TValue>();
        configure?.Invoke(options);

        return new GenericKafkaConsumer<TKey, TValue>(
            _bootstrapServers,
            _clientId,
            options,
            _ssl,
            _sasl);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _disposed = true;
        _connected = false;
        return ValueTask.CompletedTask;
    }
}
