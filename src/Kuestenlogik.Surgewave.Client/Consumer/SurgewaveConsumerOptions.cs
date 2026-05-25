using Kuestenlogik.Surgewave.Client.Serialization;
using Kuestenlogik.Surgewave.Transport;

namespace Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// Configuration options for SurgewaveConsumer.
/// </summary>
public sealed class SurgewaveConsumerOptions<TKey, TValue>
{
    /// <summary>
    /// Bootstrap servers (host:port). Required.
    /// </summary>
    public string? BootstrapServers { get; set; }

    /// <summary>
    /// Consumer group ID (optional for simple consumers).
    /// </summary>
    public string? GroupId { get; set; }

    /// <summary>
    /// Client ID for identification.
    /// </summary>
    public string? ClientId { get; set; }

    /// <summary>
    /// Key deserializer. Defaults based on type.
    /// </summary>
    public IDeserializer<TKey> KeyDeserializer { get; set; } = GetDefaultDeserializer<TKey>();

    /// <summary>
    /// Value deserializer. Defaults based on type.
    /// </summary>
    public IDeserializer<TValue> ValueDeserializer { get; set; } = GetDefaultDeserializer<TValue>();

    /// <summary>
    /// Async key deserializer for schema registry integration. Takes precedence over KeyDeserializer if set.
    /// </summary>
    public IAsyncDeserializer<TKey>? AsyncKeyDeserializer { get; set; }

    /// <summary>
    /// Async value deserializer for schema registry integration. Takes precedence over ValueDeserializer if set.
    /// </summary>
    public IAsyncDeserializer<TValue>? AsyncValueDeserializer { get; set; }

    /// <summary>
    /// Where to start consuming if no offset is stored.
    /// </summary>
    public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Latest;

    /// <summary>
    /// Enable auto-commit of offsets.
    /// </summary>
    public bool EnableAutoCommit { get; set; } = true;

    /// <summary>
    /// Auto-commit interval in milliseconds.
    /// </summary>
    public int AutoCommitIntervalMs { get; set; } = 5000;

    /// <summary>
    /// Maximum poll interval in milliseconds.
    /// </summary>
    public int MaxPollIntervalMs { get; set; } = 300000;

    /// <summary>
    /// Session timeout in milliseconds. If no heartbeat is received within this time,
    /// the consumer is considered dead and a rebalance is triggered.
    /// </summary>
    public int SessionTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Heartbeat interval in milliseconds. The consumer sends heartbeats at this interval
    /// to keep the session alive.
    /// </summary>
    public int HeartbeatIntervalMs { get; set; } = 3000;

    /// <summary>
    /// Maximum bytes to fetch per request.
    /// </summary>
    public int FetchMaxBytes { get; set; } = 1024 * 1024;

    /// <summary>
    /// Enable automatic reconnection on connection loss.
    /// Default: true.
    /// </summary>
    public bool EnableAutoReconnect { get; set; } = true;

    /// <summary>
    /// Maximum number of reconnection attempts before giving up.
    /// Default: 10.
    /// </summary>
    public int MaxReconnectAttempts { get; set; } = 10;

    /// <summary>
    /// Initial backoff time in milliseconds between reconnection attempts.
    /// Default: 100ms.
    /// </summary>
    public int ReconnectBackoffMs { get; set; } = 100;

    /// <summary>
    /// Maximum backoff time in milliseconds between reconnection attempts.
    /// Default: 10000ms (10 seconds).
    /// </summary>
    public int ReconnectBackoffMaxMs { get; set; } = 10000;

    /// <summary>
    /// Isolation level for transactional reads.
    /// Default: ReadUncommitted.
    /// </summary>
    public IsolationLevel IsolationLevel { get; set; } = IsolationLevel.ReadUncommitted;

    /// <summary>
    /// Transport type to use for connecting to the broker.
    /// Auto (default) uses SharedMemory for local brokers, TCP for remote.
    /// </summary>
    public SurgewaveTransportType Transport { get; set; } = SurgewaveTransportType.Auto;

    /// <summary>
    /// Maximum time in milliseconds to wait for subscribed topics to become available.
    /// Set to -1 for infinite waiting (default). Set to 0 to fail immediately if topic doesn't exist.
    /// </summary>
    public int TopicDiscoveryTimeoutMs { get; set; } = -1;

    /// <summary>
    /// Interval in milliseconds between topic discovery retry attempts.
    /// Default: 1000ms (1 second).
    /// </summary>
    public int TopicDiscoveryRetryIntervalMs { get; set; } = 1000;

    internal void Validate()
    {
        Validation.Guard.ValidBootstrapServers(BootstrapServers);
        Validation.Guard.ValidClientId(ClientId);
        Validation.Guard.ValidTimeoutMs(AutoCommitIntervalMs, maxTimeoutMs: 3600000); // Max 1 hour
        Validation.Guard.ValidTimeoutMs(MaxPollIntervalMs, maxTimeoutMs: 3600000);
        Validation.Guard.ValidTimeoutMs(SessionTimeoutMs, maxTimeoutMs: 3600000);
        Validation.Guard.ValidTimeoutMs(HeartbeatIntervalMs, maxTimeoutMs: SessionTimeoutMs);
        Validation.Guard.GreaterThan(FetchMaxBytes, 0);
        Validation.Guard.GreaterThanOrEqual(MaxReconnectAttempts, 0);
        Validation.Guard.GreaterThan(ReconnectBackoffMs, 0);
        Validation.Guard.GreaterThanOrEqual(ReconnectBackoffMaxMs, ReconnectBackoffMs);

        // Heartbeat should be less than session timeout (typically 1/3)
        if (HeartbeatIntervalMs >= SessionTimeoutMs)
        {
            throw new InvalidConfigurationException(
                nameof(HeartbeatIntervalMs),
                HeartbeatIntervalMs,
                $"must be less than {nameof(SessionTimeoutMs)} ({SessionTimeoutMs}ms)");
        }
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
}
