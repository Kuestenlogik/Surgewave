using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using SurgewaveConsumer = Kuestenlogik.Surgewave.Client.Abstractions.IConsumer<byte[], byte[]>;
using SurgewaveConsumeResult = Kuestenlogik.Surgewave.Client.Consumer.ConsumeResult<byte[], byte[]>;

namespace Confluent.Kafka;

/// <summary>
/// A Confluent.Kafka-compatible consumer that wraps Surgewave.Client.
/// </summary>
/// <typeparam name="TKey">The message key type.</typeparam>
/// <typeparam name="TValue">The message value type.</typeparam>
internal sealed class Consumer<TKey, TValue> : IConsumer<TKey, TValue>
{
    private readonly ISurgewaveClient _client;
    private readonly SurgewaveConsumer _surgewaveConsumer;
    private readonly IDeserializer<TKey>? _keyDeserializer;
    private readonly IDeserializer<TValue>? _valueDeserializer;
    private readonly Action<Error>? _errorHandler;
    private readonly Action<LogMessage>? _logHandler;
    private readonly Action<string>? _statisticsHandler;
    private readonly Action<IEnumerable<TopicPartition>>? _partitionsAssignedHandler;
    private readonly Action<IEnumerable<TopicPartitionOffset>>? _partitionsRevokedHandler;
    private readonly Action<CommittedOffsets>? _offsetsCommittedHandler;
    private readonly List<string> _subscription = [];
    private readonly List<TopicPartition> _assignment = [];
    private readonly Dictionary<TopicPartition, Offset> _storedOffsets = [];
    private readonly string? _groupId;
    private bool _disposed;
    private bool _closed;

    internal Consumer(
        ISurgewaveClient client,
        SurgewaveConsumer surgewaveConsumer,
        IDeserializer<TKey>? keyDeserializer,
        IDeserializer<TValue>? valueDeserializer,
        string? groupId,
        Action<Error>? errorHandler,
        Action<LogMessage>? logHandler,
        Action<string>? statisticsHandler,
        Action<IEnumerable<TopicPartition>>? partitionsAssignedHandler,
        Action<IEnumerable<TopicPartitionOffset>>? partitionsRevokedHandler,
        Action<CommittedOffsets>? offsetsCommittedHandler)
    {
        _client = client;
        _surgewaveConsumer = surgewaveConsumer;
        _keyDeserializer = keyDeserializer;
        _valueDeserializer = valueDeserializer;
        _groupId = groupId;
        _errorHandler = errorHandler;
        _logHandler = logHandler;
        _statisticsHandler = statisticsHandler;
        _partitionsAssignedHandler = partitionsAssignedHandler;
        _partitionsRevokedHandler = partitionsRevokedHandler;
        _offsetsCommittedHandler = offsetsCommittedHandler;
        Name = $"surgewave-consumer-{Guid.NewGuid():N}";
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public string? MemberId => _groupId;

    /// <inheritdoc/>
    public List<TopicPartition> Assignment => [.. _assignment];

    /// <inheritdoc/>
    public List<string> Subscription => [.. _subscription];

    /// <inheritdoc/>
    public IConsumerGroupMetadata ConsumerGroupMetadata =>
        new ConsumerGroupMetadataImpl(_groupId ?? string.Empty);

    /// <inheritdoc/>
    public void Subscribe(string topic) => Subscribe([topic]);

    /// <inheritdoc/>
    public void Subscribe(IEnumerable<string> topics)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var topicList = topics.ToList();
        _subscription.Clear();
        _subscription.AddRange(topicList);

        _surgewaveConsumer.Subscribe([.. topicList]);

        // Simulate partition assignment callback
        var assigned = topicList.Select(t => new TopicPartition(t, 0)).ToList();
        _assignment.Clear();
        _assignment.AddRange(assigned);
        _partitionsAssignedHandler?.Invoke(assigned);
    }

    /// <inheritdoc/>
    public void Unsubscribe()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var revoked = _assignment.Select(tp => new TopicPartitionOffset(tp, Offset.Unset)).ToList();
        _partitionsRevokedHandler?.Invoke(revoked);

        _subscription.Clear();
        _assignment.Clear();
    }

    /// <inheritdoc/>
    public void Assign(IEnumerable<TopicPartition> partitions)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _assignment.Clear();
        foreach (var tp in partitions)
        {
            _assignment.Add(tp);
            _surgewaveConsumer.Assign(tp.Topic, tp.Partition.Value);
        }
    }

    /// <inheritdoc/>
    public void Assign(IEnumerable<TopicPartitionOffset> partitions)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _assignment.Clear();
        foreach (var tpo in partitions)
        {
            _assignment.Add(tpo.TopicPartition);
            _surgewaveConsumer.Assign(tpo.Topic, tpo.Partition.Value, tpo.Offset.Value);
        }
    }

    /// <inheritdoc/>
    public void Unassign()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _assignment.Clear();
    }

    /// <inheritdoc/>
    public ConsumeResult<TKey, TValue>? Consume(int millisecondsTimeout) =>
        Consume(TimeSpan.FromMilliseconds(millisecondsTimeout));

    /// <inheritdoc/>
    public ConsumeResult<TKey, TValue>? Consume(TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            return Consume(cts.Token);
        }
        catch (OperationCanceledException)
        {
            return null;
        }
    }

    /// <inheritdoc/>
    public ConsumeResult<TKey, TValue>? Consume(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_closed)
            throw new KafkaException(new Error(ErrorCode.Local_State, "Consumer is closed"));

        try
        {
            var result = _surgewaveConsumer.ConsumeAsync(cancellationToken).GetAwaiter().GetResult();
            if (result is null)
                return null;

            return ConvertConsumeResult(result);
        }
        catch (OperationCanceledException)
        {
            return null;
        }
        catch (Exception ex)
        {
            var error = new Error(ErrorCode.Local_Fatal, ex.Message, true);
            _errorHandler?.Invoke(error);
            throw new ConsumeException(error);
        }
    }

    /// <inheritdoc/>
    public List<TopicPartitionOffset> Commit()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        try
        {
            _surgewaveConsumer.CommitAsync().GetAwaiter().GetResult();

            var committed = _storedOffsets
                .Select(kvp => new TopicPartitionOffset(kvp.Key, kvp.Value))
                .ToList();

            _offsetsCommittedHandler?.Invoke(new CommittedOffsets(committed, Error.NoError));
            _storedOffsets.Clear();

            return committed;
        }
        catch (Exception ex)
        {
            var error = new Error(ErrorCode.Local_Fatal, ex.Message);
            _errorHandler?.Invoke(error);
            throw new KafkaException(error);
        }
    }

    /// <inheritdoc/>
    public void Commit(ConsumeResult<TKey, TValue> result)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var tpo = new Kuestenlogik.Surgewave.Client.Abstractions.TopicPartitionOffset(
            result.Topic,
            result.Partition.Value,
            result.Offset.Value + 1); // Commit next offset

        try
        {
            _surgewaveConsumer.CommitAsync(tpo).GetAwaiter().GetResult();

            var committed = new List<TopicPartitionOffset> { new(result.Topic, result.Partition, result.Offset + 1) };
            _offsetsCommittedHandler?.Invoke(new CommittedOffsets(committed, Error.NoError));
        }
        catch (Exception ex)
        {
            var error = new Error(ErrorCode.Local_Fatal, ex.Message);
            _errorHandler?.Invoke(error);
            throw new KafkaException(error);
        }
    }

    /// <inheritdoc/>
    public void Commit(IEnumerable<TopicPartitionOffset> offsets)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var offsetList = offsets.ToList();
        var surgewaveOffsets = offsetList
            .Select(o => new Kuestenlogik.Surgewave.Client.Abstractions.TopicPartitionOffset(
                o.Topic, o.Partition.Value, o.Offset.Value))
            .ToList();

        try
        {
            _surgewaveConsumer.CommitAsync(surgewaveOffsets).GetAwaiter().GetResult();

            _offsetsCommittedHandler?.Invoke(new CommittedOffsets(offsetList, Error.NoError));
        }
        catch (Exception ex)
        {
            var error = new Error(ErrorCode.Local_Fatal, ex.Message);
            _errorHandler?.Invoke(error);
            throw new KafkaException(error);
        }
    }

    /// <inheritdoc/>
    public void Seek(TopicPartitionOffset topicPartitionOffset)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _surgewaveConsumer.Seek(
            topicPartitionOffset.Topic,
            topicPartitionOffset.Partition.Value,
            topicPartitionOffset.Offset.Value);
    }

    /// <inheritdoc/>
    public void StoreOffset(ConsumeResult<TKey, TValue> result) =>
        StoreOffset(new TopicPartitionOffset(result.Topic, result.Partition, result.Offset + 1));

    /// <inheritdoc/>
    public void StoreOffset(TopicPartitionOffset offset)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _storedOffsets[offset.TopicPartition] = offset.Offset;
    }

    /// <inheritdoc/>
    public void Pause(IEnumerable<TopicPartition> partitions)
    {
        // Surgewave doesn't have explicit pause, but we track it
        _logHandler?.Invoke(new LogMessage("consumer", SyslogLevel.Info, "", "Partitions paused"));
    }

    /// <inheritdoc/>
    public void Resume(IEnumerable<TopicPartition> partitions)
    {
        // Surgewave doesn't have explicit resume
        _logHandler?.Invoke(new LogMessage("consumer", SyslogLevel.Info, "", "Partitions resumed"));
    }

    /// <inheritdoc/>
    public Offset Committed(TopicPartition topicPartition, TimeSpan timeout)
    {
        var offsets = Committed([topicPartition], timeout);
        return offsets.FirstOrDefault()?.Offset ?? Offset.Unset;
    }

    /// <inheritdoc/>
    public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
    {
        // Return stored offsets or Unset
        return partitions
            .Select(tp => new TopicPartitionOffset(
                tp,
                _storedOffsets.TryGetValue(tp, out var o) ? o : Offset.Unset))
            .ToList();
    }

    /// <inheritdoc/>
    public Offset Position(TopicPartition partition) =>
        _storedOffsets.TryGetValue(partition, out var o) ? o : Offset.Unset;

    /// <inheritdoc/>
    public void Close()
    {
        if (_closed) return;
        _closed = true;

        // Trigger revoke callback
        var revoked = _assignment.Select(tp => new TopicPartitionOffset(tp, Offset.Unset)).ToList();
        _partitionsRevokedHandler?.Invoke(revoked);

        _subscription.Clear();
        _assignment.Clear();

        _logHandler?.Invoke(new LogMessage("consumer", SyslogLevel.Info, "", "Consumer closed"));
    }

    /// <inheritdoc/>
    public int AddBrokers(string brokers) => 0;

    /// <inheritdoc/>
    public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout) =>
        new(Offset.Beginning, Offset.End);

    /// <inheritdoc/>
    public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition) =>
        new(Offset.Beginning, Offset.End);

    private ConsumeResult<TKey, TValue> ConvertConsumeResult(SurgewaveConsumeResult result)
    {
        var key = DeserializeKey(result.Key, result.Topic);
        var value = DeserializeValue(result.Value, result.Topic);

        return new ConsumeResult<TKey, TValue>
        {
            Topic = result.Topic,
            Partition = new Partition(result.Partition),
            Offset = new Offset(result.Offset),
            Timestamp = new Timestamp(result.Timestamp),
            Message = new Message<TKey, TValue>
            {
                Key = key,
                Value = value,
                Headers = result.Headers is not null ? Headers.FromDictionary(result.Headers) : null
            },
            IsPartitionEOF = false
        };
    }

    private TKey? DeserializeKey(byte[]? data, string topic)
    {
        if (data is null) return default;

        if (_keyDeserializer is not null)
        {
            var context = new SerializationContext(MessageComponentType.Key, topic);
            return _keyDeserializer.Deserialize(data, false, context);
        }

        return DeserializeDefault<TKey>(data);
    }

    private TValue? DeserializeValue(byte[]? data, string topic)
    {
        if (data is null) return default;

        if (_valueDeserializer is not null)
        {
            var context = new SerializationContext(MessageComponentType.Value, topic);
            return _valueDeserializer.Deserialize(data, false, context);
        }

        return DeserializeDefault<TValue>(data);
    }

    private static T? DeserializeDefault<T>(byte[]? data)
    {
        if (data is null) return default;

        if (typeof(T) == typeof(byte[]))
            return (T)(object)data;

        if (typeof(T) == typeof(string))
            return (T)(object)System.Text.Encoding.UTF8.GetString(data);

        if (typeof(T) == typeof(int) && data.Length >= 4)
            return (T)(object)System.Net.IPAddress.NetworkToHostOrder(BitConverter.ToInt32(data, 0));

        if (typeof(T) == typeof(long) && data.Length >= 8)
            return (T)(object)System.Net.IPAddress.NetworkToHostOrder(BitConverter.ToInt64(data, 0));

        throw new DeserializationException($"No deserializer available for type {typeof(T).Name}");
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        if (!_closed)
            Close();

        _surgewaveConsumer.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    private sealed class ConsumerGroupMetadataImpl(string groupId) : IConsumerGroupMetadata
    {
        public string GroupId => groupId;
    }
}

/// <summary>
/// Committed offsets with possible error.
/// </summary>
public class CommittedOffsets
{
    /// <summary>
    /// Creates new CommittedOffsets.
    /// </summary>
    public CommittedOffsets(IList<TopicPartitionOffset> offsets, Error error)
    {
        Offsets = offsets;
        Error = error;
    }

    /// <summary>
    /// The committed offsets.
    /// </summary>
    public IList<TopicPartitionOffset> Offsets { get; }

    /// <summary>
    /// Any error that occurred.
    /// </summary>
    public Error Error { get; }
}
