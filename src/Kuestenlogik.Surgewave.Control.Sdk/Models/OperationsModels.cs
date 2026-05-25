namespace Kuestenlogik.Surgewave.Control.Models;

// --- Consumer Lag Models ---

/// <summary>
/// Consumer lag information for a single consumer group.
/// </summary>
public sealed record ConsumerGroupLag(
    string GroupId,
    string State,
    long TotalLag,
    IReadOnlyList<TopicPartitionLag> Partitions);

/// <summary>
/// Per-partition lag information within a consumer group.
/// </summary>
public sealed record TopicPartitionLag(
    string Topic,
    int Partition,
    long CurrentOffset,
    long EndOffset,
    long Lag,
    string? ConsumerId);

// --- Alerting Models ---

/// <summary>
/// An alert rule that triggers notifications when conditions are met.
/// </summary>
public sealed class AlertRule
{
    public string Id { get; set; } = Guid.NewGuid().ToString("N")[..8];
    public string Name { get; set; } = "";
    public AlertRuleType Type { get; set; } = AlertRuleType.ConsumerLag;
    public AlertSeverity Severity { get; set; } = AlertSeverity.Warning;
    public bool Enabled { get; set; } = true;
    public string? Target { get; set; }
    public double Threshold { get; set; }
    public string? Condition { get; set; }
    public int CooldownMinutes { get; set; } = 5;
    public List<string> NotificationChannels { get; set; } = [];
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// Types of alert rules.
/// </summary>
public enum AlertRuleType
{
    ConsumerLag,
    ErrorRate,
    BrokerDown,
    DiskUsage,
    HighLatency,
    UnderReplicatedPartitions,
    LowThroughput
}

/// <summary>
/// Alert severity levels.
/// </summary>
public enum AlertSeverity
{
    Info,
    Warning,
    Critical
}

/// <summary>
/// A triggered alert instance.
/// </summary>
public sealed class AlertEvent
{
    public string Id { get; set; } = Guid.NewGuid().ToString("N")[..8];
    public string RuleId { get; set; } = "";
    public string RuleName { get; set; } = "";
    public AlertRuleType Type { get; set; }
    public AlertSeverity Severity { get; set; }
    public string Message { get; set; } = "";
    public double CurrentValue { get; set; }
    public double Threshold { get; set; }
    public DateTime FiredAt { get; set; } = DateTime.UtcNow;
    public DateTime? AcknowledgedAt { get; set; }
    public string? AcknowledgedBy { get; set; }
    public bool IsResolved { get; set; }
    public DateTime? ResolvedAt { get; set; }
}

/// <summary>
/// Notification channel configuration.
/// </summary>
public sealed class NotificationChannel
{
    public string Id { get; set; } = Guid.NewGuid().ToString("N")[..8];
    public string Name { get; set; } = "";
    public NotificationChannelType Type { get; set; }
    public bool Enabled { get; set; } = true;
    public Dictionary<string, string> Config { get; set; } = [];
}

/// <summary>
/// Types of notification channels.
/// </summary>
public enum NotificationChannelType
{
    Slack,
    Email,
    PagerDuty,
    MicrosoftTeams,
    Webhook
}

// --- Performance Advisor Models ---

/// <summary>
/// A performance recommendation from the advisor.
/// </summary>
public sealed class PerformanceRecommendation
{
    public string Id { get; set; } = Guid.NewGuid().ToString("N")[..8];
    public RecommendationType Type { get; set; }
    public RecommendationSeverity Severity { get; set; }
    public string Title { get; set; } = "";
    public string Description { get; set; } = "";
    public string? ActionableAdvice { get; set; }
    public string? AffectedResource { get; set; }
    public double? CurrentValue { get; set; }
    public double? RecommendedValue { get; set; }
    public DateTime DetectedAt { get; set; } = DateTime.UtcNow;
    public bool IsDismissed { get; set; }
}

/// <summary>
/// Types of performance recommendations.
/// </summary>
public enum RecommendationType
{
    HotPartition,
    UnderReplicatedPartition,
    PartitionSkew,
    ThroughputBottleneck,
    HighLatency,
    ConsumerLag,
    UnbalancedConsumerGroup,
    InactiveConsumerGroup,
    TopicOverProvisioned,
    TopicUnderProvisioned
}

/// <summary>
/// Severity of a recommendation.
/// </summary>
public enum RecommendationSeverity
{
    Info,
    Warning,
    Critical
}
