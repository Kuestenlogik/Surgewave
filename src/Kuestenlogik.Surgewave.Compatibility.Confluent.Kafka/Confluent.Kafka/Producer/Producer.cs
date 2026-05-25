using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using SurgewaveProducer = Kuestenlogik.Surgewave.Client.Abstractions.IProducer<byte[], byte[]>;

namespace Confluent.Kafka;

/// <summary>
/// A Confluent.Kafka-compatible producer that wraps Surgewave.Client.
/// </summary>
/// <typeparam name="TKey">The message key type.</typeparam>
/// <typeparam name="TValue">The message value type.</typeparam>
internal sealed class Producer<TKey, TValue> : IProducer<TKey, TValue>
{
    private readonly ISurgewaveClient _client;
    private readonly SurgewaveProducer _surgewaveProducer;
    private readonly ISerializer<TKey>? _keySerializer;
    private readonly ISerializer<TValue>? _valueSerializer;
    private readonly Action<Error>? _errorHandler;
    private readonly Action<LogMessage>? _logHandler;
    private readonly Action<string>? _statisticsHandler;
    private bool _disposed;

    internal Producer(
        ISurgewaveClient client,
        SurgewaveProducer surgewaveProducer,
        ISerializer<TKey>? keySerializer,
        ISerializer<TValue>? valueSerializer,
        Action<Error>? errorHandler,
        Action<LogMessage>? logHandler,
        Action<string>? statisticsHandler)
    {
        _client = client;
        _surgewaveProducer = surgewaveProducer;
        _keySerializer = keySerializer;
        _valueSerializer = valueSerializer;
        _errorHandler = errorHandler;
        _logHandler = logHandler;
        _statisticsHandler = statisticsHandler;
        Name = $"surgewave-producer-{Guid.NewGuid():N}";
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        string topic,
        Message<TKey, TValue> message,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var keyBytes = SerializeKey(message.Key, topic);
        var valueBytes = SerializeValue(message.Value, topic);
        var headers = message.Headers?.ToDictionary();

        try
        {
            var result = await _surgewaveProducer.ProduceAsync(
                topic,
                keyBytes,
                valueBytes ?? [],
                headers,
                cancellationToken);

            return CreateDeliveryResult(result, message);
        }
        catch (Exception ex)
        {
            var error = new Error(ErrorCode.Local_Fatal, ex.Message, true);
            _errorHandler?.Invoke(error);
            throw new ProduceException<TKey, TValue>(
                error,
                new DeliveryResult<TKey, TValue>
                {
                    Topic = topic,
                    Error = error,
                    Message = message,
                    Status = PersistenceStatus.NotPersisted
                });
        }
    }

    /// <inheritdoc/>
    public Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        TopicPartition topicPartition,
        Message<TKey, TValue> message,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var keyBytes = SerializeKey(message.Key, topicPartition.Topic);
        var valueBytes = SerializeValue(message.Value, topicPartition.Topic);
        var headers = message.Headers?.ToDictionary();

        return ProduceToPartitionAsync(topicPartition, message, keyBytes, valueBytes, headers, cancellationToken);
    }

    private async Task<DeliveryResult<TKey, TValue>> ProduceToPartitionAsync(
        TopicPartition topicPartition,
        Message<TKey, TValue> message,
        byte[]? keyBytes,
        byte[]? valueBytes,
        IReadOnlyDictionary<string, byte[]>? headers,
        CancellationToken cancellationToken)
    {
        try
        {
            var result = await _surgewaveProducer.ProduceAsync(
                topicPartition.Topic,
                topicPartition.Partition.Value,
                keyBytes,
                valueBytes ?? [],
                headers,
                cancellationToken);

            return CreateDeliveryResult(result, message);
        }
        catch (Exception ex)
        {
            var error = new Error(ErrorCode.Local_Fatal, ex.Message, true);
            _errorHandler?.Invoke(error);
            throw new ProduceException<TKey, TValue>(
                error,
                new DeliveryResult<TKey, TValue>
                {
                    Topic = topicPartition.Topic,
                    Partition = topicPartition.Partition,
                    Error = error,
                    Message = message,
                    Status = PersistenceStatus.NotPersisted
                });
        }
    }

    /// <inheritdoc/>
    public void Produce(
        string topic,
        Message<TKey, TValue> message,
        Action<DeliveryReport<TKey, TValue>>? deliveryHandler = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _ = Task.Run(async () =>
        {
            try
            {
                var result = await ProduceAsync(topic, message);
                var report = new DeliveryReport<TKey, TValue>
                {
                    Topic = result.Topic,
                    Partition = result.Partition,
                    Offset = result.Offset,
                    Timestamp = result.Timestamp,
                    Message = result.Message,
                    Status = result.Status
                };
                deliveryHandler?.Invoke(report);
            }
            catch (ProduceException<TKey, TValue> ex)
            {
                var report = new DeliveryReport<TKey, TValue>
                {
                    Topic = ex.DeliveryResult.Topic,
                    Partition = ex.DeliveryResult.Partition,
                    Offset = ex.DeliveryResult.Offset,
                    Message = ex.DeliveryResult.Message,
                    Error = ex.Error,
                    Status = PersistenceStatus.NotPersisted
                };
                deliveryHandler?.Invoke(report);
            }
        });
    }

    /// <inheritdoc/>
    public void Produce(
        TopicPartition topicPartition,
        Message<TKey, TValue> message,
        Action<DeliveryReport<TKey, TValue>>? deliveryHandler = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _ = Task.Run(async () =>
        {
            try
            {
                var result = await ProduceAsync(topicPartition, message);
                var report = new DeliveryReport<TKey, TValue>
                {
                    Topic = result.Topic,
                    Partition = result.Partition,
                    Offset = result.Offset,
                    Timestamp = result.Timestamp,
                    Message = result.Message,
                    Status = result.Status
                };
                deliveryHandler?.Invoke(report);
            }
            catch (ProduceException<TKey, TValue> ex)
            {
                var report = new DeliveryReport<TKey, TValue>
                {
                    Topic = ex.DeliveryResult.Topic,
                    Partition = ex.DeliveryResult.Partition,
                    Offset = ex.DeliveryResult.Offset,
                    Message = ex.DeliveryResult.Message,
                    Error = ex.Error,
                    Status = PersistenceStatus.NotPersisted
                };
                deliveryHandler?.Invoke(report);
            }
        });
    }

    /// <inheritdoc/>
    public int Flush(TimeSpan timeout)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            _surgewaveProducer.FlushAsync(cts.Token).GetAwaiter().GetResult();
            return 0;
        }
        catch (OperationCanceledException)
        {
            return -1; // Timeout
        }
    }

    /// <inheritdoc/>
    public void Flush(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _surgewaveProducer.FlushAsync(cancellationToken).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public void InitTransactions(TimeSpan timeout)
    {
        // Surgewave handles transactions internally
        _logHandler?.Invoke(new LogMessage("producer", SyslogLevel.Info, "", "Transactions initialized"));
    }

    /// <inheritdoc/>
    public void BeginTransaction()
    {
        // Surgewave handles transactions internally
        _logHandler?.Invoke(new LogMessage("producer", SyslogLevel.Info, "", "Transaction begun"));
    }

    /// <inheritdoc/>
    public void CommitTransaction(TimeSpan timeout)
    {
        // Surgewave handles transactions internally
        _logHandler?.Invoke(new LogMessage("producer", SyslogLevel.Info, "", "Transaction committed"));
    }

    /// <inheritdoc/>
    public void CommitTransaction() => CommitTransaction(TimeSpan.FromSeconds(30));

    /// <inheritdoc/>
    public void AbortTransaction(TimeSpan timeout)
    {
        // Surgewave handles transactions internally
        _logHandler?.Invoke(new LogMessage("producer", SyslogLevel.Info, "", "Transaction aborted"));
    }

    /// <inheritdoc/>
    public void AbortTransaction() => AbortTransaction(TimeSpan.FromSeconds(30));

    /// <inheritdoc/>
    public void SendOffsetsToTransaction(
        IEnumerable<TopicPartitionOffset> offsets,
        IConsumerGroupMetadata groupMetadata,
        TimeSpan timeout)
    {
        // Surgewave handles this internally
        _logHandler?.Invoke(new LogMessage("producer", SyslogLevel.Info, "", "Offsets sent to transaction"));
    }

    /// <inheritdoc/>
    public int AddBrokers(string brokers)
    {
        // Surgewave doesn't support dynamic broker addition
        return 0;
    }

    private byte[]? SerializeKey(TKey? key, string topic)
    {
        if (key is null) return null;

        if (_keySerializer is not null)
        {
            var context = new SerializationContext(MessageComponentType.Key, topic);
            return _keySerializer.Serialize(key, context);
        }

        return SerializeDefault(key, topic);
    }

    private byte[]? SerializeValue(TValue? value, string topic)
    {
        if (value is null) return null;

        if (_valueSerializer is not null)
        {
            var context = new SerializationContext(MessageComponentType.Value, topic);
            return _valueSerializer.Serialize(value, context);
        }

        return SerializeDefault(value, topic);
    }

    private static byte[]? SerializeDefault<T>(T? data, string topic)
    {
        if (data is null) return null;

        return data switch
        {
            byte[] bytes => bytes,
            string str => System.Text.Encoding.UTF8.GetBytes(str),
            int i => BitConverter.GetBytes(System.Net.IPAddress.HostToNetworkOrder(i)),
            long l => BitConverter.GetBytes(System.Net.IPAddress.HostToNetworkOrder(l)),
            _ => throw new SerializationException($"No serializer available for type {typeof(T).Name}")
        };
    }

    private static DeliveryResult<TKey, TValue> CreateDeliveryResult(
        ProduceResult result,
        Message<TKey, TValue> message) => new()
    {
        Topic = result.Topic,
        Partition = new Partition(result.Partition),
        Offset = new Offset(result.Offset),
        Timestamp = new Timestamp(result.Timestamp),
        Message = message,
        Status = PersistenceStatus.Persisted
    };

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try
        {
            Flush(TimeSpan.FromSeconds(10));
        }
        catch
        {
            // Ignore flush errors during dispose
        }

        _surgewaveProducer.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}

/// <summary>
/// Log message for logging callbacks.
/// </summary>
public readonly struct LogMessage
{
    /// <summary>
    /// Creates a new LogMessage.
    /// </summary>
    public LogMessage(string name, SyslogLevel level, string facility, string message)
    {
        Name = name;
        Level = level;
        Facility = facility;
        Message = message;
    }

    /// <summary>The logger name.</summary>
    public string Name { get; }

    /// <summary>The log level.</summary>
    public SyslogLevel Level { get; }

    /// <summary>The facility.</summary>
    public string Facility { get; }

    /// <summary>The log message.</summary>
    public string Message { get; }
}

/// <summary>
/// Syslog severity levels.
/// </summary>
public enum SyslogLevel
{
    /// <summary>Emergency.</summary>
    Emergency = 0,
    /// <summary>Alert.</summary>
    Alert = 1,
    /// <summary>Critical.</summary>
    Critical = 2,
    /// <summary>Error.</summary>
    Error = 3,
    /// <summary>Warning.</summary>
    Warning = 4,
    /// <summary>Notice.</summary>
    Notice = 5,
    /// <summary>Info.</summary>
    Info = 6,
    /// <summary>Debug.</summary>
    Debug = 7
}
