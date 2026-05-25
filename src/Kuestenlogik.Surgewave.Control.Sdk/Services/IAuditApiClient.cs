using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Client for Audit REST API.
/// </summary>
public interface IAuditApiClient
{
    /// <summary>
    /// Query audit events.
    /// </summary>
    Task<AuditQueryResult> QueryEventsAsync(
        DateTimeOffset? start = null,
        DateTimeOffset? end = null,
        string? eventType = null,
        string? principal = null,
        string? resourceType = null,
        string? resourceName = null,
        bool? success = null,
        int limit = 100,
        int offset = 0,
        bool fromFile = false,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get audit statistics.
    /// </summary>
    Task<AuditStatsModel?> GetStatsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Get audit configuration.
    /// </summary>
    Task<AuditConfigModel?> GetConfigAsync(CancellationToken cancellationToken = default);
}
