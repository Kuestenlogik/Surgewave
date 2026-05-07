namespace Kuestenlogik.Surgewave.Core.Monitoring;

/// <summary>
/// Information about consumer lag for a consumer group.
/// </summary>
public sealed record ConsumerGroupLagInfo
{
    /// <summary>
    /// Consumer group ID.
    /// </summary>
    public required string GroupId { get; init; }

    /// <summary>
    /// Current state of the consumer group.
    /// </summary>
    public required string State { get; init; }

    /// <summary>
    /// Total lag across all partitions.
    /// </summary>
    public long TotalLag { get; init; }

    /// <summary>
    /// Number of partitions with committed offsets.
    /// </summary>
    public int PartitionCount { get; init; }

    /// <summary>
    /// Number of active members in the group.
    /// </summary>
    public int MemberCount { get; init; }

    /// <summary>
    /// Per-topic lag information.
    /// </summary>
    public required IReadOnlyList<TopicLagInfo> Topics { get; init; }

    /// <summary>
    /// Timestamp when lag was calculated.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
}

/// <summary>
/// Lag information for a specific topic within a consumer group.
/// </summary>
public sealed record TopicLagInfo
{
    /// <summary>
    /// Topic name.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Total lag for this topic across all partitions.
    /// </summary>
    public long TotalLag { get; init; }

    /// <summary>
    /// Per-partition lag information.
    /// </summary>
    public required IReadOnlyList<PartitionLagInfo> Partitions { get; init; }
}

/// <summary>
/// Lag information for a specific partition.
/// </summary>
public sealed record PartitionLagInfo
{
    /// <summary>
    /// Partition ID.
    /// </summary>
    public int Partition { get; init; }

    /// <summary>
    /// Current committed offset for this partition.
    /// </summary>
    public long CommittedOffset { get; init; }

    /// <summary>
    /// High watermark (latest offset) for this partition.
    /// </summary>
    public long HighWatermark { get; init; }

    /// <summary>
    /// Lag (high watermark - committed offset).
    /// </summary>
    public long Lag { get; init; }

    /// <summary>
    /// Log start offset for this partition.
    /// </summary>
    public long LogStartOffset { get; init; }

    /// <summary>
    /// Consumer ID assigned to this partition, if any.
    /// </summary>
    public string? AssignedConsumer { get; init; }
}

/// <summary>
/// Summary of lag across all consumer groups.
/// </summary>
public sealed record LagSummary
{
    /// <summary>
    /// Total number of consumer groups.
    /// </summary>
    public int GroupCount { get; init; }

    /// <summary>
    /// Number of groups with lag above warning threshold.
    /// </summary>
    public int GroupsWithHighLag { get; init; }

    /// <summary>
    /// Total lag across all groups.
    /// </summary>
    public long TotalLag { get; init; }

    /// <summary>
    /// Maximum lag among all groups.
    /// </summary>
    public long MaxLag { get; init; }

    /// <summary>
    /// Group with the maximum lag.
    /// </summary>
    public string? MaxLagGroup { get; init; }

    /// <summary>
    /// Per-group lag information.
    /// </summary>
    public required IReadOnlyList<ConsumerGroupLagInfo> Groups { get; init; }

    /// <summary>
    /// Timestamp when summary was calculated.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
}

/// <summary>
/// Configuration for lag alerts.
/// </summary>
public sealed class LagAlertConfig
{
    /// <summary>
    /// Lag threshold for warning alerts (default: 1000).
    /// </summary>
    public long WarningThreshold { get; set; } = 1000;

    /// <summary>
    /// Lag threshold for critical alerts (default: 10000).
    /// </summary>
    public long CriticalThreshold { get; set; } = 10000;

    /// <summary>
    /// How often to check lag (default: 30 seconds).
    /// </summary>
    public TimeSpan CheckInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Whether lag monitoring is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;
}
