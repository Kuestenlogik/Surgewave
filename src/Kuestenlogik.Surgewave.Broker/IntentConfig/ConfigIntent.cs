namespace Kuestenlogik.Surgewave.Broker.IntentConfig;

/// <summary>
/// Represents a user's intent for topic configuration.
/// Users express what they need (e.g., "high availability", "GDPR compliance")
/// and the engine resolves this to concrete broker/topic configuration.
/// </summary>
public sealed class ConfigIntent
{
    /// <summary>
    /// Free-form description or keyword(s) describing the intended use case.
    /// Examples: "high-availability", "IoT sensors with 1000 devices", "GDPR compliant payment processing".
    /// </summary>
    public required string Description { get; init; }

    /// <summary>
    /// Optional topic name. If not provided, a name will be suggested based on the intent.
    /// </summary>
    public string? TopicName { get; init; }

    /// <summary>
    /// Additional context to refine configuration (device count, message rate, environment, etc.).
    /// </summary>
    public IntentContext Context { get; init; } = new();
}

/// <summary>
/// Contextual information that influences intent-based configuration decisions.
/// All properties are optional; when provided, they enable context-aware rule adjustments.
/// </summary>
public sealed class IntentContext
{
    /// <summary>
    /// Expected number of devices/producers sending data.
    /// Used to adjust partition count (e.g., >100 devices → more partitions).
    /// </summary>
    public int? ExpectedDeviceCount { get; init; }

    /// <summary>
    /// Expected messages per second across all producers.
    /// Used to trigger high-throughput configuration when >10,000 msg/s.
    /// </summary>
    public int? ExpectedMessagesPerSec { get; init; }

    /// <summary>
    /// Expected average message size in bytes.
    /// Influences compression and batch size decisions.
    /// </summary>
    public int? ExpectedMessageSizeBytes { get; init; }

    /// <summary>
    /// Data classification level: "public", "internal", "confidential", "pii".
    /// "pii" triggers GDPR compliance rules automatically.
    /// </summary>
    public string? DataClassification { get; init; }

    /// <summary>
    /// Deployment environment: "dev", "staging", "production".
    /// "production" enforces higher replication and durability settings.
    /// </summary>
    public string? Environment { get; init; }

    /// <summary>
    /// Number of brokers in the cluster.
    /// Used to cap replication factor to available broker count.
    /// </summary>
    public int? BrokerCount { get; init; }
}
