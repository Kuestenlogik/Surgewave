using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Broker.Audit;

/// <summary>
/// Represents an audit event that is logged for compliance and monitoring.
/// </summary>
public sealed class AuditEvent
{
    /// <summary>
    /// Unique identifier for this event.
    /// </summary>
    public required string EventId { get; init; }

    /// <summary>
    /// Type of the audit event.
    /// </summary>
    public required AuditEventType EventType { get; init; }

    /// <summary>
    /// Timestamp when the event occurred (Unix milliseconds).
    /// </summary>
    public long Timestamp { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>
    /// Principal (user) who initiated the action.
    /// May be null for anonymous actions.
    /// </summary>
    public string? Principal { get; init; }

    /// <summary>
    /// Client IP address from which the action was initiated.
    /// </summary>
    public string? ClientAddress { get; init; }

    /// <summary>
    /// Client ID if provided in the request.
    /// </summary>
    public string? ClientId { get; init; }

    /// <summary>
    /// Broker ID that processed the request.
    /// </summary>
    public int BrokerId { get; init; }

    /// <summary>
    /// Type of resource being acted upon (e.g., "topic", "acl", "connector").
    /// </summary>
    public string? ResourceType { get; init; }

    /// <summary>
    /// Name of the resource being acted upon.
    /// </summary>
    public string? ResourceName { get; init; }

    /// <summary>
    /// Whether the operation was successful.
    /// </summary>
    public bool Success { get; init; } = true;

    /// <summary>
    /// Error message if the operation failed.
    /// </summary>
    public string? ErrorMessage { get; init; }

    /// <summary>
    /// Additional details about the event as key-value pairs.
    /// </summary>
    public Dictionary<string, string>? Details { get; init; }

    /// <summary>
    /// Correlation ID for tracing related events.
    /// </summary>
    public string? CorrelationId { get; init; }
}

/// <summary>
/// Audit event with timestamp range for querying.
/// </summary>
public sealed class AuditEventQuery
{
    /// <summary>
    /// Start timestamp (inclusive, Unix milliseconds).
    /// </summary>
    public long? StartTime { get; init; }

    /// <summary>
    /// End timestamp (exclusive, Unix milliseconds).
    /// </summary>
    public long? EndTime { get; init; }

    /// <summary>
    /// Filter by event type.
    /// </summary>
    public AuditEventType? EventType { get; init; }

    /// <summary>
    /// Filter by principal.
    /// </summary>
    public string? Principal { get; init; }

    /// <summary>
    /// Filter by resource type.
    /// </summary>
    public string? ResourceType { get; init; }

    /// <summary>
    /// Filter by resource name.
    /// </summary>
    public string? ResourceName { get; init; }

    /// <summary>
    /// Filter by success/failure.
    /// </summary>
    public bool? Success { get; init; }

    /// <summary>
    /// Maximum number of events to return.
    /// </summary>
    public int Limit { get; init; } = 100;

    /// <summary>
    /// Offset for pagination.
    /// </summary>
    public int Offset { get; init; } = 0;
}

/// <summary>
/// Result of an audit query.
/// </summary>
public sealed class AuditQueryResult
{
    /// <summary>
    /// The matching audit events.
    /// </summary>
    public required IReadOnlyList<AuditEvent> Events { get; init; }

    /// <summary>
    /// Total count of matching events (for pagination).
    /// </summary>
    public int TotalCount { get; init; }

    /// <summary>
    /// Whether there are more events available.
    /// </summary>
    public bool HasMore { get; init; }
}
