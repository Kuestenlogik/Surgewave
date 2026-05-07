using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;

namespace Confluent.Kafka;

/// <summary>
/// Builder for creating Confluent.Kafka-compatible producers.
/// </summary>
/// <typeparam name="TKey">The message key type.</typeparam>
/// <typeparam name="TValue">The message value type.</typeparam>
public sealed class ProducerBuilder<TKey, TValue>
{
    private readonly ProducerConfig _config;
    private ISerializer<TKey>? _keySerializer;
    private ISerializer<TValue>? _valueSerializer;
    private Action<IProducer<TKey, TValue>, Error>? _errorHandler;
    private Action<IProducer<TKey, TValue>, LogMessage>? _logHandler;
    private Action<IProducer<TKey, TValue>, string>? _statisticsHandler;

    /// <summary>
    /// Creates a new ProducerBuilder with the specified configuration.
    /// </summary>
    /// <param name="config">The producer configuration.</param>
    public ProducerBuilder(IEnumerable<KeyValuePair<string, string>> config)
    {
        _config = config as ProducerConfig ?? new ProducerConfig(config);
    }

    /// <summary>
    /// Set the key serializer.
    /// </summary>
    public ProducerBuilder<TKey, TValue> SetKeySerializer(ISerializer<TKey> serializer)
    {
        _keySerializer = serializer;
        return this;
    }

    /// <summary>
    /// Set the value serializer.
    /// </summary>
    public ProducerBuilder<TKey, TValue> SetValueSerializer(ISerializer<TValue> serializer)
    {
        _valueSerializer = serializer;
        return this;
    }

    /// <summary>
    /// Set the error handler callback.
    /// </summary>
    public ProducerBuilder<TKey, TValue> SetErrorHandler(Action<IProducer<TKey, TValue>, Error> errorHandler)
    {
        _errorHandler = errorHandler;
        return this;
    }

    /// <summary>
    /// Set the log handler callback.
    /// </summary>
    public ProducerBuilder<TKey, TValue> SetLogHandler(Action<IProducer<TKey, TValue>, LogMessage> logHandler)
    {
        _logHandler = logHandler;
        return this;
    }

    /// <summary>
    /// Set the statistics handler callback.
    /// </summary>
    public ProducerBuilder<TKey, TValue> SetStatisticsHandler(Action<IProducer<TKey, TValue>, string> statisticsHandler)
    {
        _statisticsHandler = statisticsHandler;
        return this;
    }

    /// <summary>
    /// Build the producer.
    /// </summary>
    /// <returns>A new producer instance.</returns>
    public IProducer<TKey, TValue> Build()
    {
        var bootstrapServers = _config.BootstrapServers
            ?? throw new ArgumentException("BootstrapServers must be set");

        // Determine protocol
        var protocol = _config.SurgewaveProtocol?.ToLowerInvariant() switch
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

        // Create Surgewave producer
        var surgewaveProducer = client.CreateProducer<byte[], byte[]>(options =>
        {
            if (_config.LingerMs.HasValue)
                options.LingerMs = (int)_config.LingerMs.Value;

            if (_config.BatchNumMessages.HasValue)
                options.BatchSize = _config.BatchNumMessages.Value;

            if (_config.RequestTimeoutMs.HasValue)
                options.RequestTimeoutMs = _config.RequestTimeoutMs.Value;

            if (_config.Acks.HasValue)
            {
                options.RequiredAcks = _config.Acks.Value switch
                {
                    Acks.None => 0,
                    Acks.Leader => 1,
                    Acks.All => -1,
                    _ => 1
                };
            }
        });

        // Create wrapper with error handler that captures the producer reference
        IProducer<TKey, TValue>? producer = null;
        Action<Error>? errorHandler = _errorHandler is not null
            ? e => _errorHandler(producer!, e)
            : null;
        Action<LogMessage>? logHandler = _logHandler is not null
            ? m => _logHandler(producer!, m)
            : null;
        Action<string>? statisticsHandler = _statisticsHandler is not null
            ? s => _statisticsHandler(producer!, s)
            : null;

        producer = new Producer<TKey, TValue>(
            client,
            surgewaveProducer,
            _keySerializer,
            _valueSerializer,
            errorHandler,
            logHandler,
            statisticsHandler);

        return producer;
    }
}
