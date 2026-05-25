using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using SurgewaveAutoOffsetReset = Kuestenlogik.Surgewave.Client.Consumer.AutoOffsetReset;

namespace Confluent.Kafka;

/// <summary>
/// Builder for creating Confluent.Kafka-compatible consumers.
/// </summary>
/// <typeparam name="TKey">The message key type.</typeparam>
/// <typeparam name="TValue">The message value type.</typeparam>
public sealed class ConsumerBuilder<TKey, TValue>
{
    private readonly ConsumerConfig _config;
    private IDeserializer<TKey>? _keyDeserializer;
    private IDeserializer<TValue>? _valueDeserializer;
    private Action<IConsumer<TKey, TValue>, Error>? _errorHandler;
    private Action<IConsumer<TKey, TValue>, LogMessage>? _logHandler;
    private Action<IConsumer<TKey, TValue>, string>? _statisticsHandler;
    private Action<IConsumer<TKey, TValue>, List<TopicPartition>>? _partitionsAssignedHandler;
    private Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>>? _partitionsRevokedHandler;
    private Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>>? _partitionsLostHandler;
    private Action<IConsumer<TKey, TValue>, CommittedOffsets>? _offsetsCommittedHandler;

    /// <summary>
    /// Creates a new ConsumerBuilder with the specified configuration.
    /// </summary>
    /// <param name="config">The consumer configuration.</param>
    public ConsumerBuilder(IEnumerable<KeyValuePair<string, string>> config)
    {
        _config = config as ConsumerConfig ?? new ConsumerConfig(config);
    }

    /// <summary>
    /// Set the key deserializer.
    /// </summary>
    public ConsumerBuilder<TKey, TValue> SetKeyDeserializer(IDeserializer<TKey> deserializer)
    {
        _keyDeserializer = deserializer;
        return this;
    }

    /// <summary>
    /// Set the value deserializer.
    /// </summary>
    public ConsumerBuilder<TKey, TValue> SetValueDeserializer(IDeserializer<TValue> deserializer)
    {
        _valueDeserializer = deserializer;
        return this;
    }

    /// <summary>
    /// Set the error handler callback.
    /// </summary>
    public ConsumerBuilder<TKey, TValue> SetErrorHandler(Action<IConsumer<TKey, TValue>, Error> errorHandler)
    {
        _errorHandler = errorHandler;
        return this;
    }

    /// <summary>
    /// Set the log handler callback.
    /// </summary>
    public ConsumerBuilder<TKey, TValue> SetLogHandler(Action<IConsumer<TKey, TValue>, LogMessage> logHandler)
    {
        _logHandler = logHandler;
        return this;
    }

    /// <summary>
    /// Set the statistics handler callback.
    /// </summary>
    public ConsumerBuilder<TKey, TValue> SetStatisticsHandler(Action<IConsumer<TKey, TValue>, string> statisticsHandler)
    {
        _statisticsHandler = statisticsHandler;
        return this;
    }

    /// <summary>
    /// Set the partitions assigned handler.
    /// </summary>
    public ConsumerBuilder<TKey, TValue> SetPartitionsAssignedHandler(
        Action<IConsumer<TKey, TValue>, List<TopicPartition>> handler)
    {
        _partitionsAssignedHandler = handler;
        return this;
    }

    /// <summary>
    /// Set the partitions revoked handler.
    /// </summary>
    public ConsumerBuilder<TKey, TValue> SetPartitionsRevokedHandler(
        Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> handler)
    {
        _partitionsRevokedHandler = handler;
        return this;
    }

    /// <summary>
    /// Set the partitions lost handler.
    /// </summary>
    public ConsumerBuilder<TKey, TValue> SetPartitionsLostHandler(
        Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> handler)
    {
        _partitionsLostHandler = handler;
        return this;
    }

    /// <summary>
    /// Set the offsets committed handler.
    /// </summary>
    public ConsumerBuilder<TKey, TValue> SetOffsetsCommittedHandler(
        Action<IConsumer<TKey, TValue>, CommittedOffsets> handler)
    {
        _offsetsCommittedHandler = handler;
        return this;
    }

    /// <summary>
    /// Build the consumer.
    /// </summary>
    /// <returns>A new consumer instance.</returns>
    public IConsumer<TKey, TValue> Build()
    {
        var bootstrapServers = _config.BootstrapServers
            ?? throw new ArgumentException("BootstrapServers must be set");

        // Determine protocol
        var protocol = _config["surgewave.protocol"]?.ToLowerInvariant() switch
        {
            "surgewave" => ProtocolType.SurgewaveNative,
            "kafka" => ProtocolType.Kafka,
            _ => ProtocolType.Auto
        };

        // Build Surgewave client
        var builder = SurgewaveClient.Create(bootstrapServers);

        if (_config.ClientId is not null)
            builder.WithClientId(_config.ClientId);

        builder = protocol switch
        {
            ProtocolType.SurgewaveNative => builder.UseSurgewaveProtocol(),
            ProtocolType.Kafka => builder.UseKafkaProtocol(),
            _ => builder.UseAutoDetect()
        };

        var client = builder.Build();

        // Create Surgewave consumer
        var surgewaveConsumer = client.CreateConsumer<byte[], byte[]>(options =>
        {
            if (_config.GroupId is not null)
                options.GroupId = _config.GroupId;

            if (_config.AutoOffsetReset.HasValue)
            {
                options.AutoOffsetReset = _config.AutoOffsetReset.Value switch
                {
                    AutoOffsetReset.Earliest => SurgewaveAutoOffsetReset.Earliest,
                    AutoOffsetReset.Latest => SurgewaveAutoOffsetReset.Latest,
                    _ => SurgewaveAutoOffsetReset.Latest
                };
            }

            if (_config.EnableAutoCommit.HasValue)
                options.EnableAutoCommit = _config.EnableAutoCommit.Value;

            if (_config.AutoCommitIntervalMs.HasValue)
                options.AutoCommitIntervalMs = _config.AutoCommitIntervalMs.Value;

            if (_config.MaxPollIntervalMs.HasValue)
                options.MaxPollIntervalMs = _config.MaxPollIntervalMs.Value;

            if (_config.SessionTimeoutMs.HasValue)
                options.SessionTimeoutMs = _config.SessionTimeoutMs.Value;

            if (_config.IsolationLevel.HasValue)
            {
                options.IsolationLevel = _config.IsolationLevel.Value switch
                {
                    IsolationLevel.ReadCommitted => Kuestenlogik.Surgewave.Client.Consumer.IsolationLevel.ReadCommitted,
                    IsolationLevel.ReadUncommitted => Kuestenlogik.Surgewave.Client.Consumer.IsolationLevel.ReadUncommitted,
                    _ => Kuestenlogik.Surgewave.Client.Consumer.IsolationLevel.ReadUncommitted
                };
            }
        });

        // Create wrapper with handlers that capture the consumer reference
        IConsumer<TKey, TValue>? consumer = null;

        Action<Error>? errorHandler = _errorHandler is not null
            ? e => _errorHandler(consumer!, e)
            : null;

        Action<LogMessage>? logHandler = _logHandler is not null
            ? m => _logHandler(consumer!, m)
            : null;

        Action<string>? statisticsHandler = _statisticsHandler is not null
            ? s => _statisticsHandler(consumer!, s)
            : null;

        Action<IEnumerable<TopicPartition>>? partitionsAssignedHandler = _partitionsAssignedHandler is not null
            ? p => _partitionsAssignedHandler(consumer!, p.ToList())
            : null;

        Action<IEnumerable<TopicPartitionOffset>>? partitionsRevokedHandler = null;
        if (_partitionsRevokedHandler is not null || _partitionsLostHandler is not null)
        {
            partitionsRevokedHandler = p =>
            {
                var list = p.ToList();
                _partitionsRevokedHandler?.Invoke(consumer!, list);
                _partitionsLostHandler?.Invoke(consumer!, list);
            };
        }

        Action<CommittedOffsets>? offsetsCommittedHandler = _offsetsCommittedHandler is not null
            ? o => _offsetsCommittedHandler(consumer!, o)
            : null;

        consumer = new Consumer<TKey, TValue>(
            client,
            surgewaveConsumer,
            _keyDeserializer,
            _valueDeserializer,
            _config.GroupId,
            errorHandler,
            logHandler,
            statisticsHandler,
            partitionsAssignedHandler,
            partitionsRevokedHandler,
            offsetsCommittedHandler);

        return consumer;
    }
}
