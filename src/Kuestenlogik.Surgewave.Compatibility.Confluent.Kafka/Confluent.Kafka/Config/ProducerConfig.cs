namespace Confluent.Kafka;

/// <summary>
/// Configuration for Kafka producers.
/// </summary>
public class ProducerConfig : ClientConfig
{
    /// <summary>
    /// Creates an empty producer configuration.
    /// </summary>
    public ProducerConfig() { }

    /// <summary>
    /// Creates a producer configuration from existing properties.
    /// </summary>
    public ProducerConfig(IEnumerable<KeyValuePair<string, string>> config) : base(config) { }

    /// <summary>
    /// Required acknowledgments (acks).
    /// </summary>
    public Acks? Acks
    {
        get => GetOrDefault("acks") switch
        {
            "0" => Confluent.Kafka.Acks.None,
            "1" => Confluent.Kafka.Acks.Leader,
            "-1" or "all" => Confluent.Kafka.Acks.All,
            _ => null
        };
        set => Set("acks", value switch
        {
            Confluent.Kafka.Acks.None => "0",
            Confluent.Kafka.Acks.Leader => "1",
            Confluent.Kafka.Acks.All => "-1",
            _ => null
        });
    }

    /// <summary>
    /// Time to wait for additional messages before sending a batch (milliseconds).
    /// </summary>
    public double? LingerMs
    {
        get => double.TryParse(GetOrDefault("linger.ms"), out var v) ? v : null;
        set => Set("linger.ms", value?.ToString());
    }

    /// <summary>
    /// Maximum number of messages batched per partition.
    /// </summary>
    public int? BatchNumMessages
    {
        get => int.TryParse(GetOrDefault("batch.num.messages"), out var v) ? v : null;
        set => Set("batch.num.messages", value?.ToString());
    }

    /// <summary>
    /// Maximum total message size in a batch (bytes).
    /// </summary>
    public int? BatchSize
    {
        get => int.TryParse(GetOrDefault("batch.size"), out var v) ? v : null;
        set => Set("batch.size", value?.ToString());
    }

    /// <summary>
    /// Maximum size of a message (bytes).
    /// </summary>
    public int? MessageMaxBytes
    {
        get => int.TryParse(GetOrDefault("message.max.bytes"), out var v) ? v : null;
        set => Set("message.max.bytes", value?.ToString());
    }

    /// <summary>
    /// Compression codec.
    /// </summary>
    public CompressionType? CompressionType
    {
        get => GetOrDefault("compression.type") switch
        {
            "none" => Confluent.Kafka.CompressionType.None,
            "gzip" => Confluent.Kafka.CompressionType.Gzip,
            "snappy" => Confluent.Kafka.CompressionType.Snappy,
            "lz4" => Confluent.Kafka.CompressionType.Lz4,
            "zstd" => Confluent.Kafka.CompressionType.Zstd,
            _ => null
        };
        set => Set("compression.type", value switch
        {
            Confluent.Kafka.CompressionType.None => "none",
            Confluent.Kafka.CompressionType.Gzip => "gzip",
            Confluent.Kafka.CompressionType.Snappy => "snappy",
            Confluent.Kafka.CompressionType.Lz4 => "lz4",
            Confluent.Kafka.CompressionType.Zstd => "zstd",
            _ => null
        });
    }

    /// <summary>
    /// Request timeout (milliseconds).
    /// </summary>
    public int? RequestTimeoutMs
    {
        get => int.TryParse(GetOrDefault("request.timeout.ms"), out var v) ? v : null;
        set => Set("request.timeout.ms", value?.ToString());
    }

    /// <summary>
    /// Message delivery timeout (milliseconds).
    /// </summary>
    public int? MessageTimeoutMs
    {
        get => int.TryParse(GetOrDefault("message.timeout.ms"), out var v) ? v : null;
        set => Set("message.timeout.ms", value?.ToString());
    }

    /// <summary>
    /// Number of retries on transient errors.
    /// </summary>
    public int? MessageSendMaxRetries
    {
        get => int.TryParse(GetOrDefault("message.send.max.retries"), out var v) ? v : null;
        set => Set("message.send.max.retries", value?.ToString());
    }

    /// <summary>
    /// Time to wait before retrying (milliseconds).
    /// </summary>
    public int? RetryBackoffMs
    {
        get => int.TryParse(GetOrDefault("retry.backoff.ms"), out var v) ? v : null;
        set => Set("retry.backoff.ms", value?.ToString());
    }

    /// <summary>
    /// Maximum number of in-flight requests per connection.
    /// </summary>
    public int? MaxInFlight
    {
        get => int.TryParse(GetOrDefault("max.in.flight.requests.per.connection"), out var v) ? v : null;
        set => Set("max.in.flight.requests.per.connection", value?.ToString());
    }

    /// <summary>
    /// Enable idempotent producer for exactly-once semantics.
    /// </summary>
    public bool? EnableIdempotence
    {
        get => bool.TryParse(GetOrDefault("enable.idempotence"), out var v) ? v : null;
        set => Set("enable.idempotence", value?.ToString().ToLowerInvariant());
    }

    /// <summary>
    /// Transactional ID for exactly-once processing.
    /// </summary>
    public string? TransactionalId
    {
        get => GetOrDefault("transactional.id");
        set => Set("transactional.id", value);
    }

    /// <summary>
    /// Transaction timeout (milliseconds).
    /// </summary>
    public int? TransactionTimeoutMs
    {
        get => int.TryParse(GetOrDefault("transaction.timeout.ms"), out var v) ? v : null;
        set => Set("transaction.timeout.ms", value?.ToString());
    }

    /// <summary>
    /// Partitioner strategy.
    /// </summary>
    public Partitioner? Partitioner
    {
        get => GetOrDefault("partitioner") switch
        {
            "random" => Confluent.Kafka.Partitioner.Random,
            "consistent" => Confluent.Kafka.Partitioner.Consistent,
            "consistent_random" => Confluent.Kafka.Partitioner.ConsistentRandom,
            "murmur2" => Confluent.Kafka.Partitioner.Murmur2,
            "murmur2_random" => Confluent.Kafka.Partitioner.Murmur2Random,
            _ => null
        };
        set => Set("partitioner", value switch
        {
            Confluent.Kafka.Partitioner.Random => "random",
            Confluent.Kafka.Partitioner.Consistent => "consistent",
            Confluent.Kafka.Partitioner.ConsistentRandom => "consistent_random",
            Confluent.Kafka.Partitioner.Murmur2 => "murmur2",
            Confluent.Kafka.Partitioner.Murmur2Random => "murmur2_random",
            _ => null
        });
    }
}

/// <summary>
/// Compression types for messages.
/// </summary>
public enum CompressionType
{
    /// <summary>No compression.</summary>
    None,

    /// <summary>Gzip compression.</summary>
    Gzip,

    /// <summary>Snappy compression.</summary>
    Snappy,

    /// <summary>LZ4 compression.</summary>
    Lz4,

    /// <summary>Zstandard compression.</summary>
    Zstd
}

/// <summary>
/// Partitioner strategy.
/// </summary>
public enum Partitioner
{
    /// <summary>Random partitioning.</summary>
    Random,

    /// <summary>Consistent partitioning based on key.</summary>
    Consistent,

    /// <summary>Consistent partitioning with random fallback for null keys.</summary>
    ConsistentRandom,

    /// <summary>Murmur2 hash partitioning (compatible with Java client).</summary>
    Murmur2,

    /// <summary>Murmur2 with random fallback for null keys.</summary>
    Murmur2Random
}
