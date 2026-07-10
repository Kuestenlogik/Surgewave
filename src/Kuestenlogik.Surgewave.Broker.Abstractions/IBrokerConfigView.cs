namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Protocol-neutral read-only view over the static broker configuration that the
/// Kafka config handler surfaces via DescribeConfigs. Implemented by the broker's
/// <c>BrokerConfig</c>: the directly-read scalars are satisfied implicitly by its
/// public getters, while the nested <c>Security.*</c> / <c>Quotas.*</c> reads are
/// flattened here and provided as explicit interface implementations so they stay
/// off <c>BrokerConfig</c>'s public surface (#59 b4-tier2).
/// </summary>
public interface IBrokerConfigView
{
    // Core broker identity
    int BrokerId { get; }

    // Network settings
    string Host { get; }
    int Port { get; }
    int MaxConnectionsPerIp { get; }
    int SocketSendBufferBytes { get; }
    int SocketReceiveBufferBytes { get; }
    int MaxRequestSize { get; }

    // Log / storage settings
    string DataDirectory { get; }
    long LogSegmentBytes { get; }
    int LogRetentionHours { get; }
    long LogRetentionBytes { get; }

    // Topic defaults
    bool AutoCreateTopics { get; }
    short DefaultReplicationFactor { get; }
    int DefaultNumPartitions { get; }
    int MinInSyncReplicas { get; }

    // Replication settings
    int ReplicaLagTimeMaxMs { get; }
    long ReplicaLagMaxMessages { get; }
    int ReplicaFetchMaxBytes { get; }
    int ReplicaFetchWaitMaxMs { get; }

    // Cluster settings
    string? Rack { get; }
    string? ClusterId { get; }

    // Controller / leader settings
    bool AllowAutoLeaderRebalance { get; }
    int LeaderImbalanceCheckIntervalSeconds { get; }
    int ControlledShutdownMaxRetries { get; }

    // Flattened Security.* reads
    bool SaslEnabled { get; }
    IReadOnlyList<string> SaslMechanisms { get; }

    // Flattened Quotas.* reads
    long ProducerQuotaBytesPerSecond { get; }
    long ConsumerQuotaBytesPerSecond { get; }
}
