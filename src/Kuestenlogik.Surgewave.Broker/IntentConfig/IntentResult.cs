namespace Kuestenlogik.Surgewave.Broker.IntentConfig;

/// <summary>
/// The resolved configuration result from an intent description.
/// Contains concrete topic settings, applied rules, and confidence information.
/// </summary>
public sealed class IntentResult
{
    /// <summary>
    /// Topic name (from the intent or auto-generated).
    /// </summary>
    public required string TopicName { get; init; }

    /// <summary>
    /// Resolved topic configuration key-value pairs (Kafka-compatible config keys).
    /// </summary>
    public required Dictionary<string, string> TopicConfig { get; init; }

    /// <summary>
    /// Number of partitions resolved for the topic.
    /// </summary>
    public required int Partitions { get; init; }

    /// <summary>
    /// Replication factor resolved for the topic.
    /// </summary>
    public required int ReplicationFactor { get; init; }

    /// <summary>
    /// Human-readable explanation of how the configuration was derived.
    /// </summary>
    public string Explanation { get; init; } = "";

    /// <summary>
    /// List of rules that were matched and applied to produce this configuration.
    /// </summary>
    public List<IntentRuleMatch> AppliedRules { get; init; } = [];

    /// <summary>
    /// Confidence score from 0.0 to 1.0.
    /// 1.0 = exact keyword match, lower values indicate fuzzy/partial matches.
    /// </summary>
    public double Confidence { get; init; } = 1.0;

    /// <summary>
    /// Warnings about potential issues or recommendations for the resolved configuration.
    /// </summary>
    public List<string> Warnings { get; init; } = [];
}

/// <summary>
/// A record of a single rule match applied during intent resolution.
/// </summary>
/// <param name="RuleName">Unique identifier of the matched rule.</param>
/// <param name="Description">Human-readable description of what the rule does.</param>
/// <param name="ConfigKey">The configuration key that was set (or "partitions"/"replication.factor").</param>
/// <param name="Value">The value that was applied.</param>
public sealed record IntentRuleMatch(string RuleName, string Description, string ConfigKey, string Value);
