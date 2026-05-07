using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Serialization;
using Kuestenlogik.Surgewave.Transport;

namespace Kuestenlogik.Surgewave.Client;

/// <summary>
/// Surgewave Native protocol client implementation.
/// Creates producers and consumers that use the high-performance Surgewave Native protocol.
/// </summary>
public sealed class SurgewaveClient : ISurgewaveClient
{
    private readonly string _bootstrapServers;
    private readonly string? _clientId;
    private readonly SurgewaveTransportType _transport;
    private SurgewaveNativeClient? _nativeClient;
    private bool _disposed;

    /// <inheritdoc />
    public ProtocolType Protocol => ProtocolType.SurgewaveNative;

    /// <inheritdoc />
    public string BootstrapServers => _bootstrapServers;

    /// <inheritdoc />
    public bool IsConnected => _nativeClient?.IsConnected ?? false;

    /// <inheritdoc />
    public string? ClientId => _clientId;

    /// <summary>
    /// Gets the underlying native client for advanced operations like transactions.
    /// Returns null if not yet connected.
    /// </summary>
    public SurgewaveNativeClient? NativeClient => _nativeClient;

    internal SurgewaveClient(string bootstrapServers, string? clientId, SurgewaveTransportType transport)
    {
        _bootstrapServers = bootstrapServers;
        _clientId = clientId;
        _transport = transport;
    }

    /// <inheritdoc />
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_nativeClient != null)
            return;

        var (host, port) = ParseBootstrapServers(_bootstrapServers);
        _nativeClient = new SurgewaveNativeClient(host, port, _transport);
        await _nativeClient.ConnectAsync();
    }

    /// <inheritdoc />
    public IProducer<TKey, TValue> CreateProducer<TKey, TValue>(
        Action<ProducerOptions<TKey, TValue>>? configure = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var options = new ProducerOptions<TKey, TValue>();
        configure?.Invoke(options);

        return new SurgewaveProducer<TKey, TValue>(producerOpts =>
        {
            producerOpts.BootstrapServers = _bootstrapServers;
            producerOpts.ClientId = _clientId;
            producerOpts.Transport = _transport;
            producerOpts.BatchSize = options.BatchSize;
            producerOpts.LingerMs = options.LingerMs;
            producerOpts.RequestTimeoutMs = options.RequestTimeoutMs;

            if (options.KeySerializer != null)
                producerOpts.KeySerializer = options.KeySerializer;
            if (options.ValueSerializer != null)
                producerOpts.ValueSerializer = options.ValueSerializer;
        });
    }

    /// <inheritdoc />
    public IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(
        Action<ConsumerOptions<TKey, TValue>>? configure = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var options = new ConsumerOptions<TKey, TValue>();
        configure?.Invoke(options);

        return new SurgewaveConsumer<TKey, TValue>(consumerOpts =>
        {
            consumerOpts.BootstrapServers = _bootstrapServers;
            consumerOpts.ClientId = _clientId;
            consumerOpts.Transport = _transport;
            consumerOpts.GroupId = options.GroupId;
            consumerOpts.AutoOffsetReset = options.AutoOffsetReset;
            consumerOpts.EnableAutoCommit = options.EnableAutoCommit;
            consumerOpts.AutoCommitIntervalMs = options.AutoCommitIntervalMs;
            consumerOpts.MaxPollIntervalMs = options.MaxPollIntervalMs;
            consumerOpts.SessionTimeoutMs = options.SessionTimeoutMs;
            consumerOpts.IsolationLevel = options.IsolationLevel;

            if (options.KeyDeserializer != null)
                consumerOpts.KeyDeserializer = options.KeyDeserializer;
            if (options.ValueDeserializer != null)
                consumerOpts.ValueDeserializer = options.ValueDeserializer;
        });
    }

    /// <summary>
    /// Create a client builder for configuring and building a Surgewave client.
    /// </summary>
    /// <param name="bootstrapServers">The bootstrap servers (host:port).</param>
    /// <returns>A client builder.</returns>
    public static SurgewaveClientBuilder Create(string bootstrapServers)
        => new(bootstrapServers);

    private static (string host, int port) ParseBootstrapServers(string servers)
    {
        var parts = servers.Split(':');
        return (parts[0], parts.Length > 1 ? int.Parse(parts[1]) : 9092);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_nativeClient != null)
        {
            await _nativeClient.DisposeAsync();
        }
    }
}
