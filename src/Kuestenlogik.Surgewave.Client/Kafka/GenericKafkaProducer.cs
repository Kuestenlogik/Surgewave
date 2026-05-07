using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Producer;
using Kuestenlogik.Surgewave.Client.Security;
using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Client.Kafka;

/// <summary>
/// Generic Kafka producer with serialization support.
/// Wraps the byte[]-based KafkaProducer and adds type-safe serialization.
/// </summary>
internal sealed class GenericKafkaProducer<TKey, TValue> : IProducer<TKey, TValue>
{
    private readonly KafkaProducer _inner;
    private readonly ISerializer<TKey> _keySerializer;
    private readonly ISerializer<TValue> _valueSerializer;
    private readonly IAsyncSerializer<TKey>? _asyncKeySerializer;
    private readonly IAsyncSerializer<TValue>? _asyncValueSerializer;
    private readonly DeliveryHandler<TKey, TValue>? _deliveryHandler;
    private bool _disposed;

    /// <inheritdoc />
    public ProtocolType Protocol => ProtocolType.Kafka;

    public GenericKafkaProducer(
        string bootstrapServers,
        string? clientId,
        ProducerOptions<TKey, TValue> options,
        SslOptions? ssl = null,
        SaslOptions? sasl = null)
    {
        _keySerializer = options.KeySerializer ?? GetDefaultSerializer<TKey>();
        _valueSerializer = options.ValueSerializer ?? GetDefaultSerializer<TValue>();
        _asyncKeySerializer = options.AsyncKeySerializer;
        _asyncValueSerializer = options.AsyncValueSerializer;
        _deliveryHandler = options.DeliveryHandler;

        _inner = new KafkaProducer(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            ClientId = clientId,
            RequestTimeoutMs = options.RequestTimeoutMs,
            RequiredAcks = options.RequiredAcks,
            BatchSize = options.BatchSize,
            LingerMs = options.LingerMs,
            Ssl = ssl,
            Sasl = sasl,
        });
    }

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

            var record = new ProducerRecord
            {
                Topic = topic,
                Key = keyBytes,
                Value = valueBytes,
                Headers = headers != null ? new Dictionary<string, byte[]>(headers) : null
            };

            var metadata = await _inner.SendAsync(record, cancellationToken).ConfigureAwait(false);

            var result = new ProduceResult
            {
                Topic = metadata.Topic,
                Partition = metadata.Partition,
                Offset = metadata.Offset,
                Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(metadata.Timestamp)
            };

            _deliveryHandler?.Invoke(DeliveryReport<TKey, TValue>.Success(result, key, value, headers));

            return result;
        }
        catch (Exception ex)
        {
            _deliveryHandler?.Invoke(DeliveryReport<TKey, TValue>.Failed(topic, key, value, ex, headers));
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

            var record = new ProducerRecord
            {
                Topic = topic,
                Partition = partition,
                Key = keyBytes,
                Value = valueBytes,
                Headers = headers != null ? new Dictionary<string, byte[]>(headers) : null
            };

            var metadata = await _inner.SendAsync(record, cancellationToken).ConfigureAwait(false);

            var result = new ProduceResult
            {
                Topic = metadata.Topic,
                Partition = metadata.Partition,
                Offset = metadata.Offset,
                Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(metadata.Timestamp)
            };

            _deliveryHandler?.Invoke(DeliveryReport<TKey, TValue>.Success(result, key, value, headers));

            return result;
        }
        catch (Exception ex)
        {
            _deliveryHandler?.Invoke(DeliveryReport<TKey, TValue>.Failed(topic, key, value, ex, headers));
            throw;
        }
    }

    private ValueTask<byte[]?> SerializeKeyAsync(TKey? key, string topic, CancellationToken cancellationToken)
    {
        if (_asyncKeySerializer != null)
            return _asyncKeySerializer.SerializeAsync(key, topic, cancellationToken);

        return new ValueTask<byte[]?>(_keySerializer.Serialize(key, topic));
    }

    private ValueTask<byte[]?> SerializeValueAsync(TValue value, string topic, CancellationToken cancellationToken)
    {
        if (_asyncValueSerializer != null)
            return _asyncValueSerializer.SerializeAsync(value, topic, cancellationToken);

        return new ValueTask<byte[]?>(_valueSerializer.Serialize(value, topic));
    }

    /// <inheritdoc />
    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        // Current Kafka producer sends immediately, no batching
        return Task.CompletedTask;
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

        // Default to JSON for complex types
        return Serializers.Json<T>();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await _inner.DisposeAsync();
    }
}
