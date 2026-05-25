using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Control.Models;

/// <summary>
/// Single audit event.
/// </summary>
public sealed class AuditEventModel
{
    [JsonPropertyName("eventId")]
    public string EventId { get; set; } = "";

    [JsonPropertyName("eventType")]
    public string EventType { get; set; } = "";

    [JsonPropertyName("timestamp")]
    public long Timestamp { get; set; }

    [JsonPropertyName("principal")]
    public string? Principal { get; set; }

    [JsonPropertyName("clientAddress")]
    public string? ClientAddress { get; set; }

    [JsonPropertyName("clientId")]
    public string? ClientId { get; set; }

    [JsonPropertyName("brokerId")]
    public int BrokerId { get; set; }

    [JsonPropertyName("resourceType")]
    public string? ResourceType { get; set; }

    [JsonPropertyName("resourceName")]
    public string? ResourceName { get; set; }

    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("errorMessage")]
    public string? ErrorMessage { get; set; }

    [JsonPropertyName("details")]
    public Dictionary<string, string>? Details { get; set; }

    [JsonPropertyName("correlationId")]
    public string? CorrelationId { get; set; }

    public DateTime TimestampUtc => DateTimeOffset.FromUnixTimeMilliseconds(Timestamp).UtcDateTime;
}

/// <summary>
/// Query result for audit events.
/// </summary>
public sealed class AuditQueryResult
{
    [JsonPropertyName("events")]
    public List<AuditEventModel> Events { get; set; } = [];

    [JsonPropertyName("totalCount")]
    public int TotalCount { get; set; }

    [JsonPropertyName("hasMore")]
    public bool HasMore { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }
}

/// <summary>
/// Audit statistics.
/// </summary>
public sealed class AuditStatsModel
{
    [JsonPropertyName("enabled")]
    public bool Enabled { get; set; }

    [JsonPropertyName("brokerId")]
    public int BrokerId { get; set; }

    [JsonPropertyName("recentEventCount")]
    public int RecentEventCount { get; set; }
}

/// <summary>
/// Audit configuration.
/// </summary>
public sealed class AuditConfigModel
{
    [JsonPropertyName("enabled")]
    public bool Enabled { get; set; }

    [JsonPropertyName("partitions")]
    public int Partitions { get; set; }

    [JsonPropertyName("replicationFactor")]
    public short ReplicationFactor { get; set; }

    [JsonPropertyName("retentionMs")]
    public long RetentionMs { get; set; }

    [JsonPropertyName("excludeInternalTopics")]
    public bool ExcludeInternalTopics { get; set; }

    [JsonPropertyName("logSuccessfulAuthentication")]
    public bool LogSuccessfulAuthentication { get; set; }

    [JsonPropertyName("logAuthorizationChecks")]
    public bool LogAuthorizationChecks { get; set; }

    [JsonPropertyName("includeEventTypes")]
    public List<string> IncludeEventTypes { get; set; } = [];

    [JsonPropertyName("excludeEventTypes")]
    public List<string> ExcludeEventTypes { get; set; } = [];
}
