using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Clustering.GeoReplication;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// REST management API for geo-replication cluster links.
/// Exposes the <see cref="ClusterLinkManager"/> lifecycle over HTTP: create,
/// inspect, pause, resume and remove links, plus mirror-topic promotion
/// (planned migration) and failover (emergency cutover). Consumed by the
/// Control UI and available to any HTTP tooling.
/// </summary>
public static class ClusterLinkRestApi
{
    /// <summary>Default time to wait for zero replication lag during a planned promote.</summary>
    private const int DefaultPromoteTimeoutSeconds = 60;

    /// <summary>
    /// Registers the /api/cluster-links route group on <paramref name="app"/>.
    /// </summary>
    public static IEndpointRouteBuilder MapSurgewaveClusterLinks(
        this IEndpointRouteBuilder app,
        ClusterLinkManager linkManager)
    {
        var group = app.MapGroup("/api/cluster-links")
            .WithTags("Cluster Links");

        // GET /api/cluster-links
        group.MapGet("/", () => ListLinks(linkManager))
            .WithName("ListClusterLinks")
            .WithSummary("List all cluster links with their replication status")
            .Produces<ClusterLinksResponse>();

        // POST /api/cluster-links
        group.MapPost("/", (CreateClusterLinkRequest request, CancellationToken ct) =>
                CreateLink(linkManager, request, ct))
            .WithName("CreateClusterLink")
            .WithSummary("Create and connect a new cluster link (returns the link in Error state if the remote is unreachable)")
            .Produces<ClusterLinkStatusResponse>(StatusCodes.Status201Created)
            .ProducesProblem(StatusCodes.Status400BadRequest)
            .ProducesProblem(StatusCodes.Status409Conflict);

        // GET /api/cluster-links/{linkId}
        group.MapGet("/{linkId}", (string linkId) => GetLink(linkManager, linkId))
            .WithName("GetClusterLink")
            .WithSummary("Get status of a single cluster link")
            .Produces<ClusterLinkStatusResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // DELETE /api/cluster-links/{linkId}
        group.MapDelete("/{linkId}", (string linkId) => RemoveLink(linkManager, linkId))
            .WithName("RemoveClusterLink")
            .WithSummary("Remove a cluster link and stop all associated replication services")
            .Produces<RemoveClusterLinkResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // POST /api/cluster-links/{linkId}/pause
        group.MapPost("/{linkId}/pause", (string linkId) => PauseLink(linkManager, linkId))
            .WithName("PauseClusterLink")
            .WithSummary("Pause a cluster link (stops fetching, keeps the connection)")
            .Produces<ClusterLinkStatusResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // POST /api/cluster-links/{linkId}/resume
        group.MapPost("/{linkId}/resume", (string linkId, CancellationToken ct) =>
                ResumeLink(linkManager, linkId, ct))
            .WithName("ResumeClusterLink")
            .WithSummary("Resume a paused cluster link (reconnects if necessary)")
            .Produces<ClusterLinkStatusResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound)
            .ProducesProblem(StatusCodes.Status502BadGateway);

        // POST /api/cluster-links/{linkId}/mirror-topics/{topic}/promote
        group.MapPost("/{linkId}/mirror-topics/{topic}/promote",
                (string linkId, string topic, int? timeoutSeconds, CancellationToken ct) =>
                    PromoteMirrorTopic(linkManager, linkId, topic, timeoutSeconds, ct))
            .WithName("PromoteMirrorTopic")
            .WithSummary("Promote a mirror topic to writable (planned migration — waits up to timeoutSeconds for zero lag)")
            .Produces<MirrorTopicActionResponse>()
            .ProducesProblem(StatusCodes.Status400BadRequest)
            .ProducesProblem(StatusCodes.Status404NotFound)
            .ProducesProblem(StatusCodes.Status409Conflict)
            .ProducesProblem(StatusCodes.Status500InternalServerError);

        // POST /api/cluster-links/{linkId}/mirror-topics/{topic}/failover
        group.MapPost("/{linkId}/mirror-topics/{topic}/failover",
                (string linkId, string topic, CancellationToken ct) =>
                    FailoverMirrorTopic(linkManager, linkId, topic, ct))
            .WithName("FailoverMirrorTopic")
            .WithSummary("Failover a mirror topic to writable immediately (emergency — possible data loss)")
            .Produces<MirrorTopicActionResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound)
            .ProducesProblem(StatusCodes.Status409Conflict)
            .ProducesProblem(StatusCodes.Status500InternalServerError);

        return app;
    }

    private static IResult ListLinks(ClusterLinkManager linkManager)
    {
        var links = linkManager.GetAllLinks();
        return Results.Ok(new ClusterLinksResponse([.. links.Select(ToResponse)]));
    }

    private static async Task<IResult> CreateLink(
        ClusterLinkManager linkManager,
        CreateClusterLinkRequest request,
        CancellationToken ct)
    {
        var config = new ClusterLinkConfig
        {
            LinkId = request.LinkId ?? "",
            RemoteBootstrapServers = request.RemoteBootstrapServers ?? "",
            TopicFilter = string.IsNullOrWhiteSpace(request.TopicFilter) ? ".*" : request.TopicFilter,
            SyncConsumerOffsets = request.SyncConsumerOffsets ?? true,
            SyncTopicConfigs = request.SyncTopicConfigs ?? true
        };

        var errors = config.Validate();
        if (errors.Count > 0)
        {
            return Results.BadRequest(new { message = string.Join(" ", errors) });
        }

        // Der TopicFilter wird spaeter als Regex ausgewertet — ungueltige Muster hier
        // abfangen, statt sie erst still in der Topic-Discovery scheitern zu lassen.
        try
        {
            _ = new Regex(config.TopicFilter);
        }
        catch (ArgumentException ex)
        {
            return Results.BadRequest(new { message = $"topicFilter is not a valid regex: {ex.Message}" });
        }

        if (linkManager.GetLinkStatusOrNull(config.LinkId) is not null)
        {
            return Results.Conflict(new { message = $"Cluster link '{config.LinkId}' already exists" });
        }

        try
        {
            // CreateLinkAsync reports connect failures via the returned status
            // (State=Error + errorMessage) — the link exists either way, so 201 is honest.
            var status = await linkManager.CreateLinkAsync(config, ct);
            return Results.Created($"/api/cluster-links/{config.LinkId}", ToResponse(status));
        }
        catch (InvalidOperationException ex)
        {
            // A concurrent create raced us — CreateLinkAsync only throws for duplicate link ids.
            return Results.Conflict(new { message = ex.Message });
        }
    }

    private static IResult GetLink(ClusterLinkManager linkManager, string linkId)
    {
        var status = linkManager.GetLinkStatusOrNull(linkId);
        return status is null
            ? Results.NotFound(new { message = $"Cluster link '{linkId}' not found" })
            : Results.Ok(ToResponse(status));
    }

    private static async Task<IResult> RemoveLink(ClusterLinkManager linkManager, string linkId)
    {
        try
        {
            await linkManager.RemoveLinkAsync(linkId);
            return Results.Ok(new RemoveClusterLinkResponse(linkId, "Cluster link removed"));
        }
        catch (InvalidOperationException)
        {
            return Results.NotFound(new { message = $"Cluster link '{linkId}' not found" });
        }
    }

    private static async Task<IResult> PauseLink(ClusterLinkManager linkManager, string linkId)
    {
        try
        {
            await linkManager.PauseLinkAsync(linkId);
        }
        catch (InvalidOperationException)
        {
            return Results.NotFound(new { message = $"Cluster link '{linkId}' not found" });
        }

        return GetLink(linkManager, linkId);
    }

    private static async Task<IResult> ResumeLink(
        ClusterLinkManager linkManager,
        string linkId,
        CancellationToken ct)
    {
        if (linkManager.GetLinkStatusOrNull(linkId) is null)
        {
            return Results.NotFound(new { message = $"Cluster link '{linkId}' not found" });
        }

        try
        {
            await linkManager.ResumeLinkAsync(linkId, ct);
        }
        catch (InvalidOperationException)
        {
            return Results.NotFound(new { message = $"Cluster link '{linkId}' not found" });
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Reconnecting to the remote cluster failed — report the upstream error honestly.
            return Results.Problem(
                detail: $"Failed to resume cluster link '{linkId}': {ex.Message}",
                statusCode: StatusCodes.Status502BadGateway);
        }

        return GetLink(linkManager, linkId);
    }

    private static async Task<IResult> PromoteMirrorTopic(
        ClusterLinkManager linkManager,
        string linkId,
        string topic,
        int? timeoutSeconds,
        CancellationToken ct)
    {
        if (timeoutSeconds is < 1)
        {
            return Results.BadRequest(new { message = "timeoutSeconds must be >= 1" });
        }

        var validation = ValidateMirrorTopicRequest(linkManager, linkId, topic);
        if (validation is not null) return validation;

        var timeout = TimeSpan.FromSeconds(timeoutSeconds ?? DefaultPromoteTimeoutSeconds);

        try
        {
            var promoted = await linkManager.PromoteMirrorTopicAsync(topic, timeout, ct);
            return promoted
                ? Results.Ok(new MirrorTopicActionResponse(
                    linkId, topic, "promote", "Mirror topic promoted to writable"))
                : Results.Conflict(new { message = $"Topic '{topic}' is not (or no longer) a mirror topic" });
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return Results.Problem(
                detail: $"Promote of mirror topic '{topic}' failed: {ex.Message}",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    private static async Task<IResult> FailoverMirrorTopic(
        ClusterLinkManager linkManager,
        string linkId,
        string topic,
        CancellationToken ct)
    {
        var validation = ValidateMirrorTopicRequest(linkManager, linkId, topic);
        if (validation is not null) return validation;

        try
        {
            var failedOver = await linkManager.FailoverMirrorTopicAsync(topic, ct);
            return failedOver
                ? Results.Ok(new MirrorTopicActionResponse(
                    linkId, topic, "failover", "Mirror topic failed over to writable (possible data loss if replication was behind)"))
                : Results.Conflict(new { message = $"Topic '{topic}' is not (or no longer) a mirror topic" });
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return Results.Problem(
                detail: $"Failover of mirror topic '{topic}' failed: {ex.Message}",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    /// <summary>
    /// Shared 404 checks for promote/failover: the link must exist and the topic
    /// must be a mirror topic that is fed by exactly this link.
    /// </summary>
    private static IResult? ValidateMirrorTopicRequest(
        ClusterLinkManager linkManager,
        string linkId,
        string topic)
    {
        if (linkManager.GetLinkStatusOrNull(linkId) is null)
        {
            return Results.NotFound(new { message = $"Cluster link '{linkId}' not found" });
        }

        var state = linkManager.MirrorTopicManager.GetMirrorTopicState(topic);
        if (state is null)
        {
            return Results.NotFound(new { message = $"Topic '{topic}' is not a mirror topic" });
        }

        if (!string.Equals(state.LinkId, linkId, StringComparison.Ordinal))
        {
            return Results.NotFound(new { message = $"Mirror topic '{topic}' belongs to link '{state.LinkId}', not '{linkId}'" });
        }

        return null;
    }

    private static ClusterLinkStatusResponse ToResponse(ClusterLinkStatus status) => new(
        status.LinkId,
        status.State.ToString(),
        status.RemoteClusterId,
        status.MirroredTopicCount,
        status.TotalLagMessages,
        status.LastFetchTimestamp,
        status.ErrorMessage);
}

/// <summary>Response listing all cluster links.</summary>
public sealed record ClusterLinksResponse(IReadOnlyList<ClusterLinkStatusResponse> Links);

/// <summary>Replication status of one cluster link.</summary>
public sealed record ClusterLinkStatusResponse(
    string LinkId,
    string State,
    string? RemoteClusterId,
    int MirroredTopicCount,
    long TotalLagMessages,
    DateTimeOffset? LastFetchTimestamp,
    string? ErrorMessage);

/// <summary>
/// Request to create a new cluster link. <c>topicFilter</c> defaults to <c>.*</c>,
/// the sync flags default to <c>true</c> (matching <see cref="Kuestenlogik.Surgewave.Clustering.GeoReplication.ClusterLinkConfig"/>).
/// </summary>
public sealed record CreateClusterLinkRequest(
    string? LinkId,
    string? RemoteBootstrapServers,
    string? TopicFilter,
    bool? SyncConsumerOffsets,
    bool? SyncTopicConfigs);

/// <summary>Result of removing a cluster link.</summary>
public sealed record RemoveClusterLinkResponse(string LinkId, string Message);

/// <summary>Result of a promote or failover action on a mirror topic.</summary>
public sealed record MirrorTopicActionResponse(
    string LinkId,
    string Topic,
    string Action,
    string Message);
