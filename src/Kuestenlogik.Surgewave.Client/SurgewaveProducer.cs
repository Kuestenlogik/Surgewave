using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Kuestenlogik.Surgewave.Client.Serialization;
using Kuestenlogik.Surgewave.Transport;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Client;

/// <summary>
/// A strongly-typed Surgewave producer.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
/// <example>
/// <code>
/// // Simple string producer
/// await using var producer = new SurgewaveProducer&lt;string, string&gt;(options =>
/// {
///     options.BootstrapServers = "localhost:9092";
/// });
/// await producer.ProduceAsync("my-topic", "key", "value");
///
/// // JSON producer
/// await using var producer = new SurgewaveProducer&lt;string, Order&gt;(options =>
/// {
///     options.BootstrapServers = "localhost:9092";
///     options.ValueSerializer = Serializers.Json&lt;Order&gt;();
/// });
/// await producer.ProduceAsync("orders", "order-1", new Order { Id = 1 });
/// </code>
/// </example>
public sealed class SurgewaveProducer<TKey, TValue> : IProducer<TKey, TValue>
{
    private readonly SurgewaveProducerOptions<TKey, TValue> _options;
    private readonly SurgewaveNativeClient _client;
    private readonly SurgewaveBatchingProducer _producer;
    private bool _disposed;

    /// <inheritdoc />
    public ProtocolType Protocol => ProtocolType.SurgewaveNative;

    /// <summary>
    /// Creates a new producer with configuration (connects synchronously).
    /// Prefer <see cref="CreateAsync(Action{SurgewaveProducerOptions{TKey, TValue}}, CancellationToken)"/> for async contexts.
    /// </summary>
    public SurgewaveProducer(Action<SurgewaveProducerOptions<TKey, TValue>> configure)
    {
        _options = new SurgewaveProducerOptions<TKey, TValue>();
        configure(_options);
        _options.Validate();

        var (host, port) = ParseBootstrapServers(_options.BootstrapServers!);
        _client = new SurgewaveNativeClient(host, port, _options.Transport);
        _client.ConnectAsync().GetAwaiter().GetResult();

        _producer = new SurgewaveBatchingProducer(
            _client,
            topic: null!, // Topic specified per message
            partition: 0,
            maxBatchSize: _options.BatchSize,
            lingerTime: TimeSpan.FromMilliseconds(_options.LingerMs));
    }

    /// <summary>
    /// Creates a new producer with options object (connects synchronously).
    /// Prefer <see cref="CreateAsync(SurgewaveProducerOptions{TKey, TValue}, CancellationToken)"/> for async contexts.
    /// </summary>
    public SurgewaveProducer(SurgewaveProducerOptions<TKey, TValue> options)
    {
        _options = options;
        _options.Validate();

        var (host, port) = ParseBootstrapServers(_options.BootstrapServers!);
        _client = new SurgewaveNativeClient(host, port, _options.Transport);
        _client.ConnectAsync().GetAwaiter().GetResult();

        _producer = new SurgewaveBatchingProducer(
            _client,
            topic: null!,
            partition: 0,
            maxBatchSize: _options.BatchSize,
            lingerTime: TimeSpan.FromMilliseconds(_options.LingerMs));
    }

    private SurgewaveProducer(SurgewaveProducerOptions<TKey, TValue> options, SurgewaveNativeClient client)
    {
        _options = options;
        _client = client;
        _producer = new SurgewaveBatchingProducer(
            _client,
            topic: null!,
            partition: 0,
            maxBatchSize: _options.BatchSize,
            lingerTime: TimeSpan.FromMilliseconds(_options.LingerMs));
    }

    /// <summary>
    /// Creates a new producer asynchronously (preferred over constructor in async contexts).
    /// </summary>
#pragma warning disable CA2000 // Client ownership transfers to returned producer
    public static async Task<SurgewaveProducer<TKey, TValue>> CreateAsync(
        Action<SurgewaveProducerOptions<TKey, TValue>> configure,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        var options = new SurgewaveProducerOptions<TKey, TValue>();
        configure(options);
        options.Validate();

        var (host, port) = ParseBootstrapServers(options.BootstrapServers!);
        var client = new SurgewaveNativeClient(host, port, options.Transport, logger: logger);
        try
        {
            await client.ConnectAsync(cancellationToken);
        }
        catch
        {
            await client.DisposeAsync();
            throw;
        }

        return new SurgewaveProducer<TKey, TValue>(options, client);
    }

    /// <summary>
    /// Creates a new producer asynchronously (preferred over constructor in async contexts).
    /// </summary>
    public static async Task<SurgewaveProducer<TKey, TValue>> CreateAsync(
        SurgewaveProducerOptions<TKey, TValue> options,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        options.Validate();

        var (host, port) = ParseBootstrapServers(options.BootstrapServers!);
        var client = new SurgewaveNativeClient(host, port, options.Transport, logger: logger);
        try
        {
            await client.ConnectAsync(cancellationToken);
        }
        catch
        {
            await client.DisposeAsync();
            throw;
        }

        return new SurgewaveProducer<TKey, TValue>(options, client);
    }
#pragma warning restore CA2000

    /// <inheritdoc />
    public Task<ProduceResult> ProduceAsync(
        string topic,
        TKey? key,
        TValue value,
        CancellationToken cancellationToken = default)
        => ProduceAsync(topic, key, value, null, cancellationToken);

    /// <inheritdoc />
    public async Task<ProduceResult> ProduceAsync(
        string topic,
        TKey? key,
        TValue value,
        IReadOnlyDictionary<string, byte[]>? headers,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        try
        {
            var keyBytes = await SerializeKeyAsync(key, topic, cancellationToken).ConfigureAwait(false);
            var valueBytes = await SerializeValueAsync(value, topic, cancellationToken).ConfigureAwait(false)
                ?? throw new ArgumentNullException(nameof(value), "Value cannot serialize to null");

            var builder = _client.Messaging.Send(topic)
                .ToPartition(0)
                .WithValue(valueBytes);

            if (keyBytes != null)
                builder.WithKey(keyBytes);

            if (headers != null)
            {
                foreach (var (headerKey, headerValue) in headers)
                    builder.WithHeader(headerKey, headerValue);
            }

            var offset = await builder.ExecuteAsync(cancellationToken).ConfigureAwait(false);

            var result = new ProduceResult
            {
                Topic = topic,
                Partition = 0,
                Offset = offset,
                Timestamp = DateTimeOffset.UtcNow
            };

            _options.DeliveryHandler?.Invoke(DeliveryReport<TKey, TValue>.Success(result, key, value, headers));

            return result;
        }
        catch (Exception ex)
        {
            _options.DeliveryHandler?.Invoke(DeliveryReport<TKey, TValue>.Failed(topic, key, value, ex, headers));
            throw;
        }
    }

    /// <inheritdoc />
    public Task<ProduceResult> ProduceAsync(
        string topic,
        int partition,
        TKey? key,
        TValue value,
        CancellationToken cancellationToken = default)
        => ProduceAsync(topic, partition, key, value, null, cancellationToken);

    /// <inheritdoc />
    public async Task<ProduceResult> ProduceAsync(
        string topic,
        int partition,
        TKey? key,
        TValue value,
        IReadOnlyDictionary<string, byte[]>? headers,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        try
        {
            var keyBytes = await SerializeKeyAsync(key, topic, cancellationToken).ConfigureAwait(false);
            var valueBytes = await SerializeValueAsync(value, topic, cancellationToken).ConfigureAwait(false)
                ?? throw new ArgumentNullException(nameof(value), "Value cannot serialize to null");

            var builder = _client.Messaging.Send(topic)
                .ToPartition(partition)
                .WithValue(valueBytes);

            if (keyBytes != null)
                builder.WithKey(keyBytes);

            if (headers != null)
            {
                foreach (var (headerKey, headerValue) in headers)
                    builder.WithHeader(headerKey, headerValue);
            }

            var offset = await builder.ExecuteAsync(cancellationToken).ConfigureAwait(false);

            var result = new ProduceResult
            {
                Topic = topic,
                Partition = partition,
                Offset = offset,
                Timestamp = DateTimeOffset.UtcNow
            };

            _options.DeliveryHandler?.Invoke(DeliveryReport<TKey, TValue>.Success(result, key, value, headers));

            return result;
        }
        catch (Exception ex)
        {
            _options.DeliveryHandler?.Invoke(DeliveryReport<TKey, TValue>.Failed(topic, key, value, ex, headers));
            throw;
        }
    }

    private ValueTask<byte[]?> SerializeKeyAsync(TKey? key, string topic, CancellationToken cancellationToken)
    {
        if (_options.AsyncKeySerializer != null)
            return _options.AsyncKeySerializer.SerializeAsync(key, topic, cancellationToken);

        return new ValueTask<byte[]?>(_options.KeySerializer.Serialize(key, topic));
    }

    private ValueTask<byte[]?> SerializeValueAsync(TValue value, string topic, CancellationToken cancellationToken)
    {
        if (_options.AsyncValueSerializer != null)
            return _options.AsyncValueSerializer.SerializeAsync(value, topic, cancellationToken);

        return new ValueTask<byte[]?>(_options.ValueSerializer.Serialize(value, topic));
    }

    /// <summary>
    /// Flush any pending messages.
    /// </summary>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        await _producer.FlushAsync(cancellationToken);
    }

    private static (string host, int port) ParseBootstrapServers(string servers)
    {
        var parts = servers.Split(':');
        return (parts[0], parts.Length > 1 ? int.Parse(parts[1]) : 9092);
    }

    /// <summary>Flushes pending messages and releases all resources.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        await _producer.DisposeAsync();
        await _client.DisposeAsync();
    }
}

/// <summary>
/// Configuration options for SurgewaveProducer.
/// </summary>
public sealed class SurgewaveProducerOptions<TKey, TValue>
{
    /// <summary>
    /// Bootstrap servers (host:port). Required.
    /// </summary>
    public string? BootstrapServers { get; set; }

    /// <summary>
    /// Client ID for identification.
    /// </summary>
    public string? ClientId { get; set; }

    /// <summary>
    /// Key serializer. Defaults based on type.
    /// </summary>
    public ISerializer<TKey> KeySerializer { get; set; } = GetDefaultSerializer<TKey>();

    /// <summary>
    /// Value serializer. Defaults based on type.
    /// </summary>
    public ISerializer<TValue> ValueSerializer { get; set; } = GetDefaultSerializer<TValue>();

    /// <summary>
    /// Async key serializer for schema registry integration. Takes precedence over KeySerializer if set.
    /// </summary>
    public IAsyncSerializer<TKey>? AsyncKeySerializer { get; set; }

    /// <summary>
    /// Async value serializer for schema registry integration. Takes precedence over ValueSerializer if set.
    /// </summary>
    public IAsyncSerializer<TValue>? AsyncValueSerializer { get; set; }

    /// <summary>
    /// Batch size for batching producer.
    /// </summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// Linger time in milliseconds.
    /// </summary>
    public int LingerMs { get; set; } = 5;

    /// <summary>
    /// Request timeout in milliseconds.
    /// </summary>
    public int RequestTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Transport type to use for connecting to the broker.
    /// Auto (default) uses SharedMemory for local brokers, TCP for remote.
    /// </summary>
    public SurgewaveTransportType Transport { get; set; } = SurgewaveTransportType.Auto;

    /// <summary>
    /// Callback invoked when a message delivery completes (success or failure).
    /// </summary>
    public DeliveryHandler<TKey, TValue>? DeliveryHandler { get; set; }

    internal void Validate()
    {
        Validation.Guard.ValidBootstrapServers(BootstrapServers);
        Validation.Guard.ValidClientId(ClientId);
        Validation.Guard.GreaterThan(BatchSize, 0);
        Validation.Guard.GreaterThanOrEqual(LingerMs, 0);
        Validation.Guard.ValidTimeoutMs(RequestTimeoutMs);
    }

    private static ISerializer<T> GetDefaultSerializer<T>()
    {
        var type = typeof(T);

        if (type == typeof(string))
            return (ISerializer<T>)(object)Serializers.String;
        if (type == typeof(byte[]))
            return (ISerializer<T>)(object)Serializers.ByteArray;
        if (type == typeof(int))
            return (ISerializer<T>)(object)Serializers.Int32;
        if (type == typeof(long))
            return (ISerializer<T>)(object)Serializers.Int64;
        if (type == typeof(Guid))
            return (ISerializer<T>)(object)Serializers.Guid;
        if (type == typeof(Null))
            return (ISerializer<T>)(object)Serializers.Null;

        // Default to JSON for complex types
        return Serializers.Json<T>();
    }
}
