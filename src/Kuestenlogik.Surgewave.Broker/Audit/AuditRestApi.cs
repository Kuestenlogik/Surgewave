using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Kuestenlogik.Surgewave.Broker.Audit;

/// <summary>
/// REST API for querying audit events.
/// </summary>
public static class AuditRestApi
{
    /// <summary>
    /// Maps Audit REST API endpoints.
    /// </summary>
    public static IEndpointRouteBuilder MapSurgewaveAudit(this IEndpointRouteBuilder app, AuditLogger auditLogger)
    {
        var group = app.MapGroup("/admin/audit")
            .WithTags("Audit");

        // Query audit events
        group.MapGet("/", (
            long? start,
            long? end,
            string? type,
            string? principal,
            string? resourceType,
            string? resourceName,
            bool? success,
            int? limit,
            int? offset,
            bool? fromFile) => QueryAuditEvents(
                auditLogger, start, end, type, principal, resourceType, resourceName, success, limit, offset, fromFile))
            .WithName("QueryAuditEvents")
            .WithSummary("Query audit events")
            .WithDescription("Query audit events with optional filters. By default queries in-memory recent events. Use fromFile=true to query from the audit log file.")
            .Produces<AuditQueryResultResponse>()
            .ProducesValidationProblem();

        // Get recent events count
        group.MapGet("/stats", () => GetAuditStats(auditLogger))
            .WithName("GetAuditStats")
            .WithSummary("Get audit statistics")
            .Produces<AuditStatsResponse>();

        // Get audit configuration
        group.MapGet("/config", (BrokerConfig config) => GetAuditConfig(config))
            .WithName("GetAuditConfig")
            .WithSummary("Get audit configuration")
            .Produces<AuditConfigResponse>();

        return app;
    }

    private static async Task<IResult> QueryAuditEvents(
        AuditLogger auditLogger,
        long? start,
        long? end,
        string? type,
        string? principal,
        string? resourceType,
        string? resourceName,
        bool? success,
        int? limit,
        int? offset,
        bool? fromFile)
    {
        if (!auditLogger.IsEnabled)
        {
            return Results.Ok(new AuditQueryResultResponse
            {
                Events = [],
                TotalCount = 0,
                HasMore = false,
                Message = "Audit logging is disabled"
            });
        }

        // Parse event type if provided
        AuditEventType? eventType = null;
        if (!string.IsNullOrEmpty(type))
        {
            if (!Enum.TryParse<AuditEventType>(type, ignoreCase: true, out var parsedType))
            {
                return Results.ValidationProblem(new Dictionary<string, string[]>
                {
                    ["type"] = [$"Invalid event type '{type}'. Valid types: {string.Join(", ", Enum.GetNames<AuditEventType>())}"]
                });
            }
            eventType = parsedType;
        }

        var query = new AuditEventQuery
        {
            StartTime = start,
            EndTime = end,
            EventType = eventType,
            Principal = principal,
            ResourceType = resourceType,
            ResourceName = resourceName,
            Success = success,
            Limit = limit ?? 100,
            Offset = offset ?? 0
        };

        AuditQueryResult result;

        if (fromFile == true)
        {
            result = await auditLogger.QueryFromFileAsync(query);
        }
        else
        {
            result = auditLogger.QueryRecent(query);
        }

        return Results.Ok(new AuditQueryResultResponse
        {
            Events = result.Events.Select(e => new AuditEventResponse
            {
                EventId = e.EventId,
                EventType = e.EventType.ToString(),
                Timestamp = e.Timestamp,
                Principal = e.Principal,
                ClientAddress = e.ClientAddress,
                ClientId = e.ClientId,
                BrokerId = e.BrokerId,
                ResourceType = e.ResourceType,
                ResourceName = e.ResourceName,
                Success = e.Success,
                ErrorMessage = e.ErrorMessage,
                Details = e.Details,
                CorrelationId = e.CorrelationId
            }).ToList(),
            TotalCount = result.TotalCount,
            HasMore = result.HasMore
        });
    }

    private static AuditStatsResponse GetAuditStats(AuditLogger auditLogger)
    {
        return new AuditStatsResponse
        {
            Enabled = auditLogger.IsEnabled,
            BrokerId = auditLogger.BrokerId,
            RecentEventCount = auditLogger.RecentEventCount
        };
    }

    private static AuditConfigResponse GetAuditConfig(BrokerConfig config)
    {
        return new AuditConfigResponse
        {
            Enabled = config.Audit.Enabled,
            Partitions = config.Audit.Partitions,
            ReplicationFactor = config.Audit.ReplicationFactor,
            RetentionMs = config.Audit.RetentionMs,
            ExcludeInternalTopics = config.Audit.ExcludeInternalTopics,
            LogSuccessfulAuthentication = config.Audit.LogSuccessfulAuthentication,
            LogAuthorizationChecks = config.Audit.LogAuthorizationChecks,
            IncludeEventTypes = config.Audit.IncludeEventTypes.Select(t => t.ToString()).ToList(),
            ExcludeEventTypes = config.Audit.ExcludeEventTypes.Select(t => t.ToString()).ToList()
        };
    }
}

/// <summary>
/// Response model for audit query results.
/// </summary>
public sealed class AuditQueryResultResponse
{
    /// <summary>
    /// Audit events matching the query.
    /// </summary>
    public required IReadOnlyList<AuditEventResponse> Events { get; init; }

    /// <summary>
    /// Total count of matching events.
    /// </summary>
    public int TotalCount { get; init; }

    /// <summary>
    /// Whether there are more events available.
    /// </summary>
    public bool HasMore { get; init; }

    /// <summary>
    /// Optional message (e.g., when audit is disabled).
    /// </summary>
    public string? Message { get; init; }
}

/// <summary>
/// Response model for a single audit event.
/// </summary>
public sealed class AuditEventResponse
{
    /// <summary>
    /// Unique event identifier.
    /// </summary>
    public required string EventId { get; init; }

    /// <summary>
    /// Type of event.
    /// </summary>
    public required string EventType { get; init; }

    /// <summary>
    /// Unix timestamp in milliseconds.
    /// </summary>
    public long Timestamp { get; init; }

    /// <summary>
    /// Principal (user) who performed the action.
    /// </summary>
    public string? Principal { get; init; }

    /// <summary>
    /// Client IP address.
    /// </summary>
    public string? ClientAddress { get; init; }

    /// <summary>
    /// Client identifier.
    /// </summary>
    public string? ClientId { get; init; }

    /// <summary>
    /// Broker ID where the event occurred.
    /// </summary>
    public int BrokerId { get; init; }

    /// <summary>
    /// Type of resource affected.
    /// </summary>
    public string? ResourceType { get; init; }

    /// <summary>
    /// Name of the resource affected.
    /// </summary>
    public string? ResourceName { get; init; }

    /// <summary>
    /// Whether the operation was successful.
    /// </summary>
    public bool Success { get; init; }

    /// <summary>
    /// Error message if the operation failed.
    /// </summary>
    public string? ErrorMessage { get; init; }

    /// <summary>
    /// Additional details about the event.
    /// </summary>
    public Dictionary<string, string>? Details { get; init; }

    /// <summary>
    /// Correlation ID for tracing related events.
    /// </summary>
    public string? CorrelationId { get; init; }
}

/// <summary>
/// Response model for audit statistics.
/// </summary>
public sealed class AuditStatsResponse
{
    /// <summary>
    /// Whether audit logging is enabled.
    /// </summary>
    public bool Enabled { get; init; }

    /// <summary>
    /// Broker ID.
    /// </summary>
    public int BrokerId { get; init; }

    /// <summary>
    /// Number of events in the recent events buffer.
    /// </summary>
    public int RecentEventCount { get; init; }
}

/// <summary>
/// Response model for audit configuration.
/// </summary>
public sealed class AuditConfigResponse
{
    /// <summary>
    /// Whether audit logging is enabled.
    /// </summary>
    public bool Enabled { get; init; }

    /// <summary>
    /// Number of partitions for the audit topic.
    /// </summary>
    public int Partitions { get; init; }

    /// <summary>
    /// Replication factor for the audit topic.
    /// </summary>
    public short ReplicationFactor { get; init; }

    /// <summary>
    /// Retention period in milliseconds.
    /// </summary>
    public long RetentionMs { get; init; }

    /// <summary>
    /// Whether to exclude internal topics from auditing.
    /// </summary>
    public bool ExcludeInternalTopics { get; init; }

    /// <summary>
    /// Whether to log successful authentication attempts.
    /// </summary>
    public bool LogSuccessfulAuthentication { get; init; }

    /// <summary>
    /// Whether to log authorization checks.
    /// </summary>
    public bool LogAuthorizationChecks { get; init; }

    /// <summary>
    /// Event types to include (empty means all).
    /// </summary>
    public IReadOnlyList<string> IncludeEventTypes { get; init; } = [];

    /// <summary>
    /// Event types to exclude.
    /// </summary>
    public IReadOnlyList<string> ExcludeEventTypes { get; init; } = [];
}
