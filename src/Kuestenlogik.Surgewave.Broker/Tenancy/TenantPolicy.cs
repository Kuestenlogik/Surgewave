namespace Kuestenlogik.Surgewave.Broker.Tenancy;

public sealed class TenantPolicy
{
    /// <summary>Maximum number of topics this tenant can create. -1 = unlimited.</summary>
    public int MaxTopics { get; init; } = -1;

    /// <summary>Maximum partitions across all topics. -1 = unlimited.</summary>
    public int MaxPartitions { get; init; } = -1;

    /// <summary>Maximum consumer groups. -1 = unlimited.</summary>
    public int MaxConsumerGroups { get; init; } = -1;

    /// <summary>Maximum produce rate in bytes/sec. -1 = unlimited.</summary>
    public long MaxProduceBytesPerSecond { get; init; } = -1;

    /// <summary>Maximum fetch rate in bytes/sec. -1 = unlimited.</summary>
    public long MaxFetchBytesPerSecond { get; init; } = -1;

    /// <summary>Maximum message size in bytes. -1 = broker default.</summary>
    public int MaxMessageBytes { get; init; } = -1;

    /// <summary>Maximum retention period. null = broker default.</summary>
    public TimeSpan? MaxRetentionPeriod { get; init; }

    /// <summary>Maximum total storage bytes across all topics. -1 = unlimited.</summary>
    public long MaxStorageBytes { get; init; } = -1;

    /// <summary>Maximum connections from this tenant. -1 = unlimited.</summary>
    public int MaxConnections { get; init; } = -1;

    /// <summary>Default replication factor for new topics. 0 = broker default.</summary>
    public short DefaultReplicationFactor { get; init; }

    /// <summary>Allowed topic name patterns (regex). Empty = all allowed.</summary>
    public List<string> AllowedTopicPatterns { get; init; } = [];
}
