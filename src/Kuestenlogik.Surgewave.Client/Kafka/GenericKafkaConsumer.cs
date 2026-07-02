using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Security;
using Kuestenlogik.Surgewave.Client.Serialization;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Client.Kafka;

/// <summary>
/// Generic Kafka consumer with deserialization support.
/// Wraps the byte[]-based KafkaConsumer and adds type-safe deserialization.
/// </summary>
internal sealed class GenericKafkaConsumer<TKey, TValue> : IConsumer<TKey, TValue>
{
    private readonly KafkaConsumer _inner;
    private readonly IDeserializer<TKey> _keyDeserializer;
    private readonly IDeserializer<TValue> _valueDeserializer;
    private readonly IAsyncDeserializer<TKey>? _asyncKeyDeserializer;
    private readonly IAsyncDeserializer<TValue>? _asyncValueDeserializer;
    private readonly ConsumerOptions<TKey, TValue> _options;
    private readonly List<(string topic, int partition)> _assignment = [];
    private bool _disposed;

    /// <inheritdoc />
    public ProtocolType Protocol => ProtocolType.Kafka;

    /// <inheritdoc />
    public bool IsConnected => true; // Kafka connects lazily

    /// <inheritdoc />
    public IReadOnlyList<(string topic, int partition)> Assignment => _assignment;

    public GenericKafkaConsumer(
        string bootstrapServers,
        string? clientId,
        ConsumerOptions<TKey, TValue> options,
        SslOptions? ssl = null,
        SaslOptions? sasl = null)
    {
        _options = options;
        _keyDeserializer = options.KeyDeserializer ?? GetDefaultDeserializer<TKey>();
        _valueDeserializer = options.ValueDeserializer ?? GetDefaultDeserializer<TValue>();
        _asyncKeyDeserializer = options.AsyncKeyDeserializer;
        _asyncValueDeserializer = options.AsyncValueDeserializer;

        _inner = new KafkaConsumer(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            ClientId = clientId,
            GroupId = options.GroupId,
            AutoOffsetReset = options.AutoOffsetReset.ToString().ToLowerInvariant(),
            EnableAutoCommit = options.EnableAutoCommit,
            AutoCommitIntervalMs = options.AutoCommitIntervalMs,
            MaxPollRecords = 500,
            FetchMinBytes = 1,
            FetchMaxWaitMs = 500,
            Ssl = ssl,
            Sasl = sasl,
        });
    }

    /// <inheritdoc />
    public void Subscribe(params string[] topics)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _inner.Subscribe(topics);
        SyncAssignmentFromInner();
    }

    /// <inheritdoc />
    public async Task SubscribeAsync(CancellationToken cancellationToken = default, params string[] topics)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _inner.SubscribeAsync(topics, cancellationToken).ConfigureAwait(false);
        SyncAssignmentFromInner();
    }

    /// <summary>
    /// Mirror the partitions discovered by the inner consumer's metadata
    /// lookup into this consumer's assignment view.
    /// </summary>
    private void SyncAssignmentFromInner()
    {
        _assignment.Clear();
        foreach (var tp in _inner.Assignment)
        {
            _assignment.Add((tp.Topic, tp.Partition));
        }
    }

    /// <inheritdoc />
    public void Assign(string topic, int partition, long offset = 0)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var tp = new TopicPartition { Topic = topic, Partition = partition };
        _inner.Assign(tp);
        if (offset > 0)
        {
            _inner.Seek(tp, offset);
        }
        _assignment.Add((topic, partition));
    }

    /// <inheritdoc />
    public async Task<ConsumeResult<TKey, TValue>?> ConsumeAsync(CancellationToken cancellationToken = default)
    {
        return await ConsumeAsync(TimeSpan.FromMilliseconds(_options.MaxPollIntervalMs), cancellationToken);
    }

    /// <inheritdoc />
    public async Task<ConsumeResult<TKey, TValue>?> ConsumeAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var record = await _inner.ConsumeAsync(timeout, cancellationToken).ConfigureAwait(false);
        if (record == null)
            return null;

        var keyValue = record.Key is { Length: > 0 }
            ? await DeserializeKeyAsync(record.Key, record.Topic, cancellationToken).ConfigureAwait(false)
            : default;
        var valueValue = await DeserializeValueAsync(record.Value, record.Topic, cancellationToken).ConfigureAwait(false);

        return new ConsumeResult<TKey, TValue>
        {
            Topic = record.Topic,
            Partition = record.Partition,
            Offset = record.Offset,
            Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(record.Timestamp),
            Key = keyValue,
            Value = valueValue,
            Headers = record.Headers
        };
    }

    private ValueTask<TKey> DeserializeKeyAsync(byte[] data, string topic, CancellationToken cancellationToken)
    {
        if (_asyncKeyDeserializer != null)
            return _asyncKeyDeserializer.DeserializeAsync(data, topic, cancellationToken);

        return new ValueTask<TKey>(_keyDeserializer.Deserialize(data.AsSpan(), topic));
    }

    private ValueTask<TValue> DeserializeValueAsync(byte[] data, string topic, CancellationToken cancellationToken)
    {
        if (_asyncValueDeserializer != null)
            return _asyncValueDeserializer.DeserializeAsync(data, topic, cancellationToken);

        return new ValueTask<TValue>(_valueDeserializer.Deserialize(data.AsSpan(), topic));
    }

    /// <inheritdoc />
    public void Seek(string topic, int partition, long offset)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _inner.Seek(new TopicPartition { Topic = topic, Partition = partition }, offset);
    }

    /// <inheritdoc />
    public Task CommitAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _inner.CommitAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task CommitAsync(TopicPartitionOffset offset, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var tp = new TopicPartition { Topic = offset.Topic, Partition = offset.Partition };
        return _inner.CommitAsync([(tp, offset.Offset)], cancellationToken);
    }

    /// <inheritdoc />
    public Task CommitAsync(ConsumeResult<TKey, TValue> result, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var tp = new TopicPartition { Topic = result.Topic, Partition = result.Partition };
        // Commit offset + 1 (next offset to consume)
        return _inner.CommitAsync([(tp, result.Offset + 1)], cancellationToken);
    }

    /// <inheritdoc />
    public Task CommitAsync(IEnumerable<TopicPartitionOffset> offsets, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var commitOffsets = offsets.Select(o =>
            (new TopicPartition { Topic = o.Topic, Partition = o.Partition }, o.Offset));
        return _inner.CommitAsync(commitOffsets, cancellationToken);
    }

    private static IDeserializer<T> GetDefaultDeserializer<T>()
    {
        var type = typeof(T);

        if (type == typeof(string))
            return (IDeserializer<T>)(object)Serializers.StringDeserializer;
        if (type == typeof(byte[]))
            return (IDeserializer<T>)(object)Serializers.ByteArrayDeserializer;
        if (type == typeof(int))
            return (IDeserializer<T>)(object)Serializers.Int32Deserializer;
        if (type == typeof(long))
            return (IDeserializer<T>)(object)Serializers.Int64Deserializer;
        if (type == typeof(Guid))
            return (IDeserializer<T>)(object)Serializers.GuidDeserializer;

        // Default to JSON for complex types
        return Serializers.JsonDeserializer<T>();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await _inner.DisposeAsync();
    }
}
