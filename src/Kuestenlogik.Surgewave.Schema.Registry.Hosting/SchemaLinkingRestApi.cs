using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Kuestenlogik.Surgewave.Schema.Registry.Linking;

/// <summary>
/// REST API endpoints for schema linking management.
/// </summary>
public static class SchemaLinkingRestApi
{
    /// <summary>
    /// Maps schema linking REST API endpoints.
    /// </summary>
    public static IEndpointRouteBuilder MapSurgewaveSchemaLinking(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/schema-linking")
            .WithTags("Schema Linking");

        group.MapGet("/status", GetLinkingStatus)
            .WithName("GetSchemaLinkingStatus")
            .WithSummary("Get overall schema linking status")
            .Produces<SchemaLinkingStatusResponse>();

        group.MapGet("/links", GetAllLinks)
            .WithName("GetSchemaLinks")
            .WithSummary("List all schema links across clusters")
            .Produces<IReadOnlyList<SchemaLink>>();

        group.MapGet("/links/{subject}", GetLinksForSubject)
            .WithName("GetSchemaLinksForSubject")
            .WithSummary("Get all links for a specific subject")
            .Produces<IReadOnlyList<SchemaLink>>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPost("/sync", ForceSync)
            .WithName("ForceSchemaLinkingSync")
            .WithSummary("Force an immediate schema linking sync cycle")
            .Produces<SchemaLinkingSyncResponse>();

        group.MapGet("/metrics", GetMetrics)
            .WithName("GetSchemaLinkingMetrics")
            .WithSummary("Get schema linking sync metrics")
            .Produces<SchemaLinkingMetrics>();

        group.MapGet("/conflicts", GetConflicts)
            .WithName("GetSchemaLinkingConflicts")
            .WithSummary("List all unresolved schema linking conflicts")
            .Produces<IReadOnlyList<SchemaLink>>();

        group.MapPost("/conflicts/{clusterId}/{subject}/resolve", ResolveConflict)
            .WithName("ResolveSchemaLinkingConflict")
            .WithSummary("Resolve a schema linking conflict")
            .Produces<SchemaLinkingResolveResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        return app;
    }

    private static SchemaLinkingStatusResponse GetLinkingStatus(SchemaLinkingService service)
    {
        var state = service.State;
        var metrics = service.Metrics;
        var allLinks = state.GetAllLinks();

        return new SchemaLinkingStatusResponse
        {
            Enabled = true,
            TotalLinks = allLinks.Count,
            SyncedLinks = allLinks.Count(l => l.Status == SchemaSyncStatus.Synced),
            PendingLinks = allLinks.Count(l => l.Status == SchemaSyncStatus.Pending),
            ConflictLinks = allLinks.Count(l => l.Status == SchemaSyncStatus.Conflict),
            FailedLinks = allLinks.Count(l => l.Status == SchemaSyncStatus.Failed),
            LastSyncAt = metrics.LastSyncAt,
            TotalSchemasSynced = metrics.SchemasSynced,
            TotalErrors = metrics.SyncErrors
        };
    }

    private static IReadOnlyList<SchemaLink> GetAllLinks(SchemaLinkingService service)
    {
        return service.State.GetAllLinks();
    }

    private static IResult GetLinksForSubject(string subject, SchemaLinkingService service)
    {
        var links = service.State.GetLinksForSubject(subject);
        if (links.Count == 0)
        {
            return Results.NotFound(new { ErrorCode = 40401, Message = $"No links found for subject '{subject}'" });
        }
        return Results.Ok(links);
    }

    private static async Task<SchemaLinkingSyncResponse> ForceSync(
        SchemaLinkingService service,
        CancellationToken ct)
    {
        await service.ForceSyncAsync(ct);
        var metrics = service.Metrics;

        return new SchemaLinkingSyncResponse
        {
            Success = true,
            SchemasSynced = metrics.SchemasSynced,
            Errors = metrics.SyncErrors,
            SyncedAt = metrics.LastSyncAt ?? DateTimeOffset.UtcNow
        };
    }

    private static SchemaLinkingMetrics GetMetrics(SchemaLinkingService service)
    {
        return service.Metrics;
    }

    private static IReadOnlyList<SchemaLink> GetConflicts(SchemaLinkingService service)
    {
        return service.State.GetConflicts();
    }

    private static IResult ResolveConflict(
        string clusterId,
        string subject,
        SchemaLinkingResolveRequest request,
        SchemaLinkingService service)
    {
        var link = service.State.GetLink(clusterId, subject);
        if (link is null)
        {
            return Results.NotFound(new
            {
                ErrorCode = 40401,
                Message = $"No link found for cluster '{clusterId}' and subject '{subject}'"
            });
        }

        if (link.Status != SchemaSyncStatus.Conflict)
        {
            return Results.BadRequest(new
            {
                ErrorCode = 40901,
                Message = $"Link for '{subject}' is not in conflict state (current: {link.Status})"
            });
        }

        service.ResolveConflict(clusterId, subject, request.UseLocal);

        return Results.Ok(new SchemaLinkingResolveResponse
        {
            Subject = subject,
            ClusterId = clusterId,
            Resolved = true,
            ChosenSide = request.UseLocal ? "local" : "remote"
        });
    }
}

/// <summary>
/// Overall schema linking status response.
/// </summary>
public sealed class SchemaLinkingStatusResponse
{
    /// <summary>Whether schema linking is enabled.</summary>
    public bool Enabled { get; init; }

    /// <summary>Total number of tracked schema links.</summary>
    public int TotalLinks { get; init; }

    /// <summary>Number of links in synced state.</summary>
    public int SyncedLinks { get; init; }

    /// <summary>Number of links in pending state.</summary>
    public int PendingLinks { get; init; }

    /// <summary>Number of links with conflicts.</summary>
    public int ConflictLinks { get; init; }

    /// <summary>Number of links in failed state.</summary>
    public int FailedLinks { get; init; }

    /// <summary>Timestamp of last sync cycle.</summary>
    public DateTimeOffset? LastSyncAt { get; init; }

    /// <summary>Total schemas synced since startup.</summary>
    public long TotalSchemasSynced { get; init; }

    /// <summary>Total errors since startup.</summary>
    public long TotalErrors { get; init; }
}

/// <summary>
/// Response after forcing a sync cycle.
/// </summary>
public sealed class SchemaLinkingSyncResponse
{
    /// <summary>Whether the sync completed successfully.</summary>
    public bool Success { get; init; }

    /// <summary>Total schemas synced (cumulative).</summary>
    public long SchemasSynced { get; init; }

    /// <summary>Total errors (cumulative).</summary>
    public long Errors { get; init; }

    /// <summary>Timestamp of the sync.</summary>
    public DateTimeOffset SyncedAt { get; init; }
}

/// <summary>
/// Request to resolve a schema linking conflict.
/// </summary>
public sealed class SchemaLinkingResolveRequest
{
    /// <summary>If true, resolve conflict in favor of local; if false, in favor of remote.</summary>
    public bool UseLocal { get; set; } = true;
}

/// <summary>
/// Response after resolving a conflict.
/// </summary>
public sealed class SchemaLinkingResolveResponse
{
    /// <summary>The subject that was resolved.</summary>
    public required string Subject { get; init; }

    /// <summary>The cluster ID involved.</summary>
    public required string ClusterId { get; init; }

    /// <summary>Whether resolution was successful.</summary>
    public bool Resolved { get; init; }

    /// <summary>Which side was chosen ("local" or "remote").</summary>
    public required string ChosenSide { get; init; }
}
