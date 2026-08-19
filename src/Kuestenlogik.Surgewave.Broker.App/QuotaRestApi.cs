namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// REST API endpoints for Quota management.
/// </summary>
public static class QuotaRestApi
{
    public static IEndpointRouteBuilder MapSurgewaveQuota(this IEndpointRouteBuilder app, QuotaManager quotaManager)
    {
        var group = app.MapGroup("/admin/quotas")
            .WithTags("Quota Management");

        group.MapGet("/config", () => GetConfig(quotaManager))
            .WithName("GetQuotaConfig")
            .WithSummary("Get current quota configuration")
            .Produces<QuotaConfigResponse>();

        group.MapPut("/config", (UpdateQuotaConfigRequest request) => UpdateConfig(quotaManager, request))
            .WithName("UpdateQuotaConfig")
            .WithSummary("Update quota configuration")
            .Produces<QuotaConfigResponse>()
            .ProducesValidationProblem();

        group.MapGet("/clients", () => ListClientStats(quotaManager))
            .WithName("ListClientQuotas")
            .WithSummary("List all clients with their quota statistics")
            .Produces<IReadOnlyList<ClientQuotaStatsResponse>>();

        group.MapGet("/clients/{clientId}", (string clientId) => GetClientStats(quotaManager, clientId))
            .WithName("GetClientQuota")
            .WithSummary("Get quota statistics for a specific client")
            .Produces<ClientQuotaStatsResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        return app;
    }

    private static IResult GetConfig(QuotaManager quotaManager)
    {
        var config = quotaManager.Config;
        return Results.Ok(new QuotaConfigResponse(
            config.Enabled,
            config.ProducerBytesPerSecond,
            config.ProducerBurstBytes,
            config.ConsumerBytesPerSecond,
            config.ConsumerBurstBytes,
            config.MaxThrottleTimeMs,
            config.ClientInactivityTimeoutMinutes));
    }

    private static IResult UpdateConfig(QuotaManager quotaManager, UpdateQuotaConfigRequest request)
    {
        quotaManager.UpdateConfig(
            enabled: request.Enabled,
            producerBytesPerSecond: request.ProducerBytesPerSecond,
            producerBurstBytes: request.ProducerBurstBytes,
            consumerBytesPerSecond: request.ConsumerBytesPerSecond,
            consumerBurstBytes: request.ConsumerBurstBytes,
            maxThrottleTimeMs: request.MaxThrottleTimeMs,
            clientInactivityTimeoutMinutes: request.ClientInactivityTimeoutMinutes);

        var config = quotaManager.Config;
        return Results.Ok(new QuotaConfigResponse(
            config.Enabled,
            config.ProducerBytesPerSecond,
            config.ProducerBurstBytes,
            config.ConsumerBytesPerSecond,
            config.ConsumerBurstBytes,
            config.MaxThrottleTimeMs,
            config.ClientInactivityTimeoutMinutes));
    }

    private static IResult ListClientStats(QuotaManager quotaManager)
    {
        var stats = quotaManager.GetAllClientStats()
            .Select(item => new ClientQuotaStatsResponse(
                item.ClientId,
                item.Stats.TotalProducedBytes,
                item.Stats.TotalFetchedBytes,
                item.Stats.ProduceThrottleCount,
                item.Stats.FetchThrottleCount,
                item.Stats.AvailableProduceTokens,
                item.Stats.AvailableFetchTokens,
                item.Stats.LastActivity))
            .OrderByDescending(s => s.LastActivity)
            .ToList();

        return Results.Ok(stats);
    }

    private static IResult GetClientStats(QuotaManager quotaManager, string clientId)
    {
        var stats = quotaManager.GetClientStats(clientId);
        if (stats == null)
        {
            return Results.NotFound(new { message = $"Client '{clientId}' not found" });
        }

        return Results.Ok(new ClientQuotaStatsResponse(
            clientId,
            stats.TotalProducedBytes,
            stats.TotalFetchedBytes,
            stats.ProduceThrottleCount,
            stats.FetchThrottleCount,
            stats.AvailableProduceTokens,
            stats.AvailableFetchTokens,
            stats.LastActivity));
    }
}

/// <summary>
/// Response representing quota configuration.
/// </summary>
public sealed record QuotaConfigResponse(
    bool Enabled,
    long ProducerBytesPerSecond,
    long ProducerBurstBytes,
    long ConsumerBytesPerSecond,
    long ConsumerBurstBytes,
    int MaxThrottleTimeMs,
    int ClientInactivityTimeoutMinutes);

/// <summary>
/// Request to update quota configuration.
/// </summary>
public sealed record UpdateQuotaConfigRequest(
    bool? Enabled = null,
    long? ProducerBytesPerSecond = null,
    long? ProducerBurstBytes = null,
    long? ConsumerBytesPerSecond = null,
    long? ConsumerBurstBytes = null,
    int? MaxThrottleTimeMs = null,
    int? ClientInactivityTimeoutMinutes = null);

/// <summary>
/// Response representing client quota statistics.
/// </summary>
public sealed record ClientQuotaStatsResponse(
    string ClientId,
    long TotalProducedBytes,
    long TotalFetchedBytes,
    int ProduceThrottleCount,
    int FetchThrottleCount,
    long AvailableProduceTokens,
    long AvailableFetchTokens,
    DateTime LastActivity);
