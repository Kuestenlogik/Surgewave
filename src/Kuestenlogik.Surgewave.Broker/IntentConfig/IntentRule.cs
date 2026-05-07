namespace Kuestenlogik.Surgewave.Broker.IntentConfig;

/// <summary>
/// A single intent rule that maps keywords to configuration values.
/// When keywords from a rule are found in the user's intent description,
/// the rule's configuration is applied to the resolved result.
/// </summary>
public sealed class IntentRule
{
    /// <summary>
    /// Unique name identifying this rule (e.g., "high-availability", "gdpr-compliance").
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Trigger keywords (English and German) that activate this rule.
    /// Matching is case-insensitive and checks for substring containment.
    /// </summary>
    public required List<string> Keywords { get; init; }

    /// <summary>
    /// Topic configuration key-value pairs to apply when this rule matches.
    /// Uses Kafka-compatible config keys (e.g., "compression.type", "retention.ms").
    /// </summary>
    public required Dictionary<string, string> Config { get; init; }

    /// <summary>
    /// Optional partition count override. When set, overrides the default partition count.
    /// If multiple rules set partitions, the highest value wins.
    /// </summary>
    public int? Partitions { get; init; }

    /// <summary>
    /// Optional replication factor override. When set, overrides the default replication factor.
    /// If multiple rules set replication, the highest value wins.
    /// </summary>
    public int? ReplicationFactor { get; init; }

    /// <summary>
    /// Human-readable description of what this rule configures.
    /// </summary>
    public string Description { get; init; } = "";

    /// <summary>
    /// Priority level for conflict resolution. Higher priority rules take precedence.
    /// </summary>
    public IntentRulePriority Priority { get; init; } = IntentRulePriority.Normal;
}

/// <summary>
/// Priority levels for intent rules, used for conflict resolution when multiple rules match.
/// </summary>
public enum IntentRulePriority
{
    /// <summary>Low priority, easily overridden by other rules.</summary>
    Low,

    /// <summary>Normal priority (default for most rules).</summary>
    Normal,

    /// <summary>High priority, overrides normal rules.</summary>
    High,

    /// <summary>Critical priority, always takes precedence (e.g., compliance rules).</summary>
    Critical
}
