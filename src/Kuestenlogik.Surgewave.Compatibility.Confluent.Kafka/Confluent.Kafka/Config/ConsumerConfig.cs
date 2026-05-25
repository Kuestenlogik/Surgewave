namespace Confluent.Kafka;

/// <summary>
/// Configuration for Kafka consumers.
/// </summary>
public class ConsumerConfig : ClientConfig
{
    /// <summary>
    /// Creates an empty consumer configuration.
    /// </summary>
    public ConsumerConfig() { }

    /// <summary>
    /// Creates a consumer configuration from existing properties.
    /// </summary>
    public ConsumerConfig(IEnumerable<KeyValuePair<string, string>> config) : base(config) { }

    /// <summary>
    /// Consumer group ID.
    /// </summary>
    public string? GroupId
    {
        get => GetOrDefault("group.id");
        set => Set("group.id", value);
    }

    /// <summary>
    /// What to do when there is no initial offset or offset is out of range.
    /// </summary>
    public AutoOffsetReset? AutoOffsetReset
    {
        get => GetOrDefault("auto.offset.reset") switch
        {
            "earliest" => Confluent.Kafka.AutoOffsetReset.Earliest,
            "latest" => Confluent.Kafka.AutoOffsetReset.Latest,
            "error" => Confluent.Kafka.AutoOffsetReset.Error,
            _ => null
        };
        set => Set("auto.offset.reset", value switch
        {
            Confluent.Kafka.AutoOffsetReset.Earliest => "earliest",
            Confluent.Kafka.AutoOffsetReset.Latest => "latest",
            Confluent.Kafka.AutoOffsetReset.Error => "error",
            _ => null
        });
    }

    /// <summary>
    /// Enable automatic offset commit.
    /// </summary>
    public bool? EnableAutoCommit
    {
        get => bool.TryParse(GetOrDefault("enable.auto.commit"), out var v) ? v : null;
        set => Set("enable.auto.commit", value?.ToString().ToLowerInvariant());
    }

    /// <summary>
    /// Auto commit interval (milliseconds).
    /// </summary>
    public int? AutoCommitIntervalMs
    {
        get => int.TryParse(GetOrDefault("auto.commit.interval.ms"), out var v) ? v : null;
        set => Set("auto.commit.interval.ms", value?.ToString());
    }

    /// <summary>
    /// Enable automatic offset store.
    /// </summary>
    public bool? EnableAutoOffsetStore
    {
        get => bool.TryParse(GetOrDefault("enable.auto.offset.store"), out var v) ? v : null;
        set => Set("enable.auto.offset.store", value?.ToString().ToLowerInvariant());
    }

    /// <summary>
    /// Maximum records returned per poll.
    /// </summary>
    public int? MaxPollRecords
    {
        get => int.TryParse(GetOrDefault("max.poll.records"), out var v) ? v : null;
        set => Set("max.poll.records", value?.ToString());
    }

    /// <summary>
    /// Maximum time between poll calls before consumer is considered dead (milliseconds).
    /// </summary>
    public int? MaxPollIntervalMs
    {
        get => int.TryParse(GetOrDefault("max.poll.interval.ms"), out var v) ? v : null;
        set => Set("max.poll.interval.ms", value?.ToString());
    }

    /// <summary>
    /// Session timeout for consumer group membership (milliseconds).
    /// </summary>
    public int? SessionTimeoutMs
    {
        get => int.TryParse(GetOrDefault("session.timeout.ms"), out var v) ? v : null;
        set => Set("session.timeout.ms", value?.ToString());
    }

    /// <summary>
    /// Heartbeat interval (milliseconds).
    /// </summary>
    public int? HeartbeatIntervalMs
    {
        get => int.TryParse(GetOrDefault("heartbeat.interval.ms"), out var v) ? v : null;
        set => Set("heartbeat.interval.ms", value?.ToString());
    }

    /// <summary>
    /// Minimum bytes to return from a fetch request.
    /// </summary>
    public int? FetchMinBytes
    {
        get => int.TryParse(GetOrDefault("fetch.min.bytes"), out var v) ? v : null;
        set => Set("fetch.min.bytes", value?.ToString());
    }

    /// <summary>
    /// Maximum bytes per partition to return.
    /// </summary>
    public int? MaxPartitionFetchBytes
    {
        get => int.TryParse(GetOrDefault("max.partition.fetch.bytes"), out var v) ? v : null;
        set => Set("max.partition.fetch.bytes", value?.ToString());
    }

    /// <summary>
    /// Maximum wait time for a fetch (milliseconds).
    /// </summary>
    public int? FetchWaitMaxMs
    {
        get => int.TryParse(GetOrDefault("fetch.wait.max.ms"), out var v) ? v : null;
        set => Set("fetch.wait.max.ms", value?.ToString());
    }

    /// <summary>
    /// Transaction isolation level.
    /// </summary>
    public IsolationLevel? IsolationLevel
    {
        get => GetOrDefault("isolation.level") switch
        {
            "read_uncommitted" => Confluent.Kafka.IsolationLevel.ReadUncommitted,
            "read_committed" => Confluent.Kafka.IsolationLevel.ReadCommitted,
            _ => null
        };
        set => Set("isolation.level", value switch
        {
            Confluent.Kafka.IsolationLevel.ReadUncommitted => "read_uncommitted",
            Confluent.Kafka.IsolationLevel.ReadCommitted => "read_committed",
            _ => null
        });
    }

    /// <summary>
    /// Group instance ID for static membership.
    /// </summary>
    public string? GroupInstanceId
    {
        get => GetOrDefault("group.instance.id");
        set => Set("group.instance.id", value);
    }

    /// <summary>
    /// Partition assignment strategy.
    /// </summary>
    public PartitionAssignmentStrategy? PartitionAssignmentStrategy
    {
        get => GetOrDefault("partition.assignment.strategy") switch
        {
            "range" => Confluent.Kafka.PartitionAssignmentStrategy.Range,
            "roundrobin" => Confluent.Kafka.PartitionAssignmentStrategy.RoundRobin,
            "cooperative-sticky" => Confluent.Kafka.PartitionAssignmentStrategy.CooperativeSticky,
            _ => null
        };
        set => Set("partition.assignment.strategy", value switch
        {
            Confluent.Kafka.PartitionAssignmentStrategy.Range => "range",
            Confluent.Kafka.PartitionAssignmentStrategy.RoundRobin => "roundrobin",
            Confluent.Kafka.PartitionAssignmentStrategy.CooperativeSticky => "cooperative-sticky",
            _ => null
        });
    }

    /// <summary>
    /// Enable checking CRC32 of consumed messages.
    /// </summary>
    public bool? CheckCrcs
    {
        get => bool.TryParse(GetOrDefault("check.crcs"), out var v) ? v : null;
        set => Set("check.crcs", value?.ToString().ToLowerInvariant());
    }
}

/// <summary>
/// Transaction isolation level.
/// </summary>
public enum IsolationLevel
{
    /// <summary>Read all messages including uncommitted.</summary>
    ReadUncommitted,

    /// <summary>Only read committed messages.</summary>
    ReadCommitted
}

/// <summary>
/// Partition assignment strategy for consumer groups.
/// </summary>
public enum PartitionAssignmentStrategy
{
    /// <summary>Range assignment.</summary>
    Range,

    /// <summary>Round-robin assignment.</summary>
    RoundRobin,

    /// <summary>Cooperative sticky assignment.</summary>
    CooperativeSticky
}
