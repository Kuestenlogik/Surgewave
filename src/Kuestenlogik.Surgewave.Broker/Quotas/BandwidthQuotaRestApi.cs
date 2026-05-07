namespace Kuestenlogik.Surgewave.Broker.Quotas;

/// <summary>
/// REST API endpoints for bandwidth quota management.
/// </summary>
public static class BandwidthQuotaRestApi
{
    public static IEndpointRouteBuilder MapSurgewaveBandwidthQuota(
        this IEndpointRouteBuilder app,
        BandwidthQuotaManager manager)
    {
        var group = app.MapGroup("/api/quotas/bandwidth")
            .WithTags("Bandwidth Quota Management");

        group.MapGet("/", () => ListAll(manager))
            .WithName("ListBandwidthQuotas")
            .WithSummary("List all bandwidth quotas and current usage")
            .Produces<BandwidthQuotaListResponse>();

        group.MapGet("/{clientId}", (string clientId) => GetClient(manager, clientId))
            .WithName("GetBandwidthQuotaClient")
            .WithSummary("Get bandwidth usage for a specific client")
            .Produces<BandwidthUsageResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPut("/client/{clientId}", (string clientId, SetBandwidthQuotaRequest request) =>
                SetClientQuota(manager, clientId, request))
            .WithName("SetClientBandwidthQuota")
            .WithSummary("Set or update bandwidth quota for a client")
            .Produces<BandwidthQuotaSetResponse>();

        group.MapPut("/user/{user}", (string user, SetBandwidthQuotaRequest request) =>
                SetUserQuota(manager, user, request))
            .WithName("SetUserBandwidthQuota")
            .WithSummary("Set or update bandwidth quota for a user")
            .Produces<BandwidthQuotaSetResponse>();

        group.MapDelete("/client/{clientId}", (string clientId) => RemoveClientQuota(manager, clientId))
            .WithName("RemoveClientBandwidthQuota")
            .WithSummary("Remove client-specific bandwidth quota override")
            .Produces<BandwidthQuotaRemovedResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapGet("/metrics", () => GetMetrics(manager))
            .WithName("GetBandwidthQuotaMetrics")
            .WithSummary("Get aggregate bandwidth throttling metrics")
            .Produces<BandwidthQuotaMetricsResponse>();

        group.MapGet("/config", () => GetConfig(manager))
            .WithName("GetBandwidthQuotaConfig")
            .WithSummary("Get current bandwidth quota configuration")
            .Produces<BandwidthQuotaConfigResponse>();

        return app;
    }

    private static IResult ListAll(BandwidthQuotaManager manager)
    {
        var usage = manager.GetAllUsage()
            .OrderByDescending(u => u.ProduceBytesPerSec + u.ConsumeBytesPerSec)
            .Select(MapUsage)
            .ToList();

        return Results.Ok(new BandwidthQuotaListResponse(usage));
    }

    private static IResult GetClient(BandwidthQuotaManager manager, string clientId)
    {
        var usage = manager.GetClientUsage(clientId);
        if (usage == null)
            return Results.NotFound(new { message = $"Client '{clientId}' not found" });

        return Results.Ok(MapUsage(usage));
    }

    private static IResult SetClientQuota(BandwidthQuotaManager manager, string clientId, SetBandwidthQuotaRequest request)
    {
        var quota = new ClientBandwidthQuota
        {
            ProduceBytesPerSec = request.ProduceBytesPerSec,
            ConsumeBytesPerSec = request.ConsumeBytesPerSec
        };
        manager.SetClientQuota(clientId, quota);

        return Results.Ok(new BandwidthQuotaSetResponse(
            clientId,
            "client",
            request.ProduceBytesPerSec,
            request.ConsumeBytesPerSec));
    }

    private static IResult SetUserQuota(BandwidthQuotaManager manager, string user, SetBandwidthQuotaRequest request)
    {
        var quota = new ClientBandwidthQuota
        {
            ProduceBytesPerSec = request.ProduceBytesPerSec,
            ConsumeBytesPerSec = request.ConsumeBytesPerSec
        };
        manager.SetUserQuota(user, quota);

        return Results.Ok(new BandwidthQuotaSetResponse(
            user,
            "user",
            request.ProduceBytesPerSec,
            request.ConsumeBytesPerSec));
    }

    private static IResult RemoveClientQuota(BandwidthQuotaManager manager, string clientId)
    {
        if (!manager.RemoveClientQuota(clientId))
            return Results.NotFound(new { message = $"No custom quota found for client '{clientId}'" });

        return Results.Ok(new BandwidthQuotaRemovedResponse(clientId));
    }

    private static IResult GetMetrics(BandwidthQuotaManager manager)
    {
        var metrics = manager.GetMetrics();

        return Results.Ok(new BandwidthQuotaMetricsResponse(
            metrics.TotalClientsTracked,
            metrics.TotalClientsThrottled,
            metrics.TotalBytesThrottled,
            metrics.TotalThrottleEvents));
    }

    private static IResult GetConfig(BandwidthQuotaManager manager)
    {
        var config = manager.Config;

        return Results.Ok(new BandwidthQuotaConfigResponse(
            config.Enabled,
            config.DefaultProduceBytesPerSec,
            config.DefaultConsumeBytesPerSec,
            config.EnforcementWindowMs,
            config.ThrottleDelayFactor,
            manager.ClientOverrides.ToDictionary(
                kv => kv.Key,
                kv => new BandwidthQuotaOverrideResponse(kv.Value.ProduceBytesPerSec, kv.Value.ConsumeBytesPerSec)),
            manager.UserOverrides.ToDictionary(
                kv => kv.Key,
                kv => new BandwidthQuotaOverrideResponse(kv.Value.ProduceBytesPerSec, kv.Value.ConsumeBytesPerSec))));
    }

    private static BandwidthUsageResponse MapUsage(BandwidthUsage u) =>
        new(u.ClientId,
            u.ProduceBytesPerSec,
            u.ConsumeBytesPerSec,
            u.ProduceLimitBytesPerSec,
            u.ConsumeLimitBytesPerSec,
            Math.Round(u.ProduceUtilizationPercent, 1),
            Math.Round(u.ConsumeUtilizationPercent, 1),
            u.IsThrottled,
            u.LastActivityAt);
}

// --- Response/Request Records ---

/// <summary>
/// Response containing bandwidth quota list.
/// </summary>
public sealed record BandwidthQuotaListResponse(IReadOnlyList<BandwidthUsageResponse> Clients);

/// <summary>
/// Bandwidth usage for a single client.
/// </summary>
public sealed record BandwidthUsageResponse(
    string ClientId,
    long ProduceBytesPerSec,
    long ConsumeBytesPerSec,
    long ProduceLimitBytesPerSec,
    long ConsumeLimitBytesPerSec,
    double ProduceUtilizationPercent,
    double ConsumeUtilizationPercent,
    bool IsThrottled,
    DateTimeOffset LastActivityAt);

/// <summary>
/// Request to set bandwidth quota limits.
/// </summary>
public sealed record SetBandwidthQuotaRequest(long ProduceBytesPerSec, long ConsumeBytesPerSec);

/// <summary>
/// Response after setting a bandwidth quota.
/// </summary>
public sealed record BandwidthQuotaSetResponse(string Entity, string EntityType, long ProduceBytesPerSec, long ConsumeBytesPerSec);

/// <summary>
/// Response after removing a bandwidth quota.
/// </summary>
public sealed record BandwidthQuotaRemovedResponse(string ClientId);

/// <summary>
/// Aggregate bandwidth quota metrics response.
/// </summary>
public sealed record BandwidthQuotaMetricsResponse(
    long TotalClientsTracked,
    long TotalClientsThrottled,
    long TotalBytesThrottled,
    long TotalThrottleEvents);

/// <summary>
/// Bandwidth quota configuration response.
/// </summary>
public sealed record BandwidthQuotaConfigResponse(
    bool Enabled,
    long DefaultProduceBytesPerSec,
    long DefaultConsumeBytesPerSec,
    int EnforcementWindowMs,
    double ThrottleDelayFactor,
    Dictionary<string, BandwidthQuotaOverrideResponse> ClientOverrides,
    Dictionary<string, BandwidthQuotaOverrideResponse> UserOverrides);

/// <summary>
/// A bandwidth quota override entry for a client or user.
/// </summary>
public sealed record BandwidthQuotaOverrideResponse(long ProduceBytesPerSec, long ConsumeBytesPerSec);
