using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Kuestenlogik.Surgewave.Broker.CruiseControl;

/// <summary>
/// REST API endpoints for the Cruise Control (auto-balance) system.
/// </summary>
public static class CruiseControlRestApi
{
    /// <summary>
    /// Maps Cruise Control REST API endpoints to the application.
    /// </summary>
    /// <param name="app">The endpoint route builder.</param>
    /// <param name="cruiseControlService">The Cruise Control service instance.</param>
    /// <returns>The endpoint route builder for chaining.</returns>
    public static IEndpointRouteBuilder MapCruiseControl(this IEndpointRouteBuilder app, CruiseControlService cruiseControlService)
    {
        var group = app.MapGroup("/api/cruise-control")
            .WithTags("Cruise Control");

        group.MapGet("/status", () => GetStatus(cruiseControlService))
            .WithName("GetCruiseControlStatus")
            .WithSummary("Get current cluster balance report")
            .Produces<ClusterBalanceReport>();

        group.MapGet("/history", (int? count) => GetHistory(cruiseControlService, count ?? 10))
            .WithName("GetCruiseControlHistory")
            .WithSummary("Get historical balance reports")
            .Produces<IReadOnlyList<ClusterBalanceReport>>();

        group.MapPost("/analyze", async (CancellationToken ct) => await AnalyzeNow(cruiseControlService, ct))
            .WithName("CruiseControlAnalyzeNow")
            .WithSummary("Force an immediate balance analysis")
            .Produces<ClusterBalanceReport>();

        group.MapPost("/apply", async (CancellationToken ct) => await ApplySuggestion(cruiseControlService, ct))
            .WithName("CruiseControlApplySuggestion")
            .WithSummary("Apply the most recent suggested rebalance plan")
            .Produces(StatusCodes.Status200OK)
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapGet("/config", () => GetConfig(cruiseControlService))
            .WithName("GetCruiseControlConfig")
            .WithSummary("Get current Cruise Control configuration")
            .Produces<CruiseControlConfigResponse>();

        group.MapPut("/config", (CruiseControlConfigUpdate update) => UpdateConfig(cruiseControlService, update))
            .WithName("UpdateCruiseControlConfig")
            .WithSummary("Update Cruise Control configuration (mode, goals, thresholds)")
            .Produces<CruiseControlConfigResponse>();

        return app;
    }

    private static IResult GetStatus(CruiseControlService service)
    {
        var report = service.GetLatestReport();
        return report is not null
            ? Results.Ok(report)
            : Results.Ok(new ClusterBalanceReport());
    }

    private static IResult GetHistory(CruiseControlService service, int count)
    {
        return Results.Ok(service.GetHistory(count));
    }

    private static async Task<IResult> AnalyzeNow(CruiseControlService service, CancellationToken ct)
    {
        var report = await service.AnalyzeNowAsync(ct);
        return Results.Ok(report);
    }

    private static async Task<IResult> ApplySuggestion(CruiseControlService service, CancellationToken ct)
    {
        var report = service.GetLatestReport();
        if (report?.SuggestedPlan is null || report.SuggestedPlan.Assignments.Count == 0)
        {
            return Results.NotFound(new { message = "No suggested rebalance plan available" });
        }

        await service.ApplySuggestionAsync(ct);
        return Results.Ok(new { message = "Rebalance plan submitted for execution", moves = report.SuggestedPlan.Assignments.Count });
    }

    private static IResult GetConfig(CruiseControlService service)
    {
        var config = service.Config;
        return Results.Ok(new CruiseControlConfigResponse(
            Enabled: config.Enabled,
            Mode: config.Mode.ToString(),
            AnalysisIntervalSeconds: config.AnalysisIntervalSeconds,
            ThrottleRateBytesPerSec: config.ThrottleRateBytesPerSec,
            CooldownMinutes: config.CooldownMinutes,
            Goals: config.Goals));
    }

    private static IResult UpdateConfig(CruiseControlService service, CruiseControlConfigUpdate update)
    {
        var config = service.Config;

        if (update.Mode is not null)
        {
            if (Enum.TryParse<CruiseControlMode>(update.Mode, ignoreCase: true, out var mode))
                config.Mode = mode;
        }

        if (update.AnalysisIntervalSeconds.HasValue)
            config.AnalysisIntervalSeconds = update.AnalysisIntervalSeconds.Value;

        if (update.ThrottleRateBytesPerSec.HasValue)
            config.ThrottleRateBytesPerSec = update.ThrottleRateBytesPerSec.Value;

        if (update.CooldownMinutes.HasValue)
            config.CooldownMinutes = update.CooldownMinutes.Value;

        if (update.Goals is not null)
        {
            if (update.Goals.MaxPartitionImbalancePercent.HasValue)
                config.Goals.MaxPartitionImbalancePercent = update.Goals.MaxPartitionImbalancePercent.Value;
            if (update.Goals.MaxDiskImbalancePercent.HasValue)
                config.Goals.MaxDiskImbalancePercent = update.Goals.MaxDiskImbalancePercent.Value;
            if (update.Goals.MaxLeaderImbalancePercent.HasValue)
                config.Goals.MaxLeaderImbalancePercent = update.Goals.MaxLeaderImbalancePercent.Value;
            if (update.Goals.MaxNetworkImbalancePercent.HasValue)
                config.Goals.MaxNetworkImbalancePercent = update.Goals.MaxNetworkImbalancePercent.Value;
            if (update.Goals.MinPartitionsToRebalance.HasValue)
                config.Goals.MinPartitionsToRebalance = update.Goals.MinPartitionsToRebalance.Value;
        }

        return GetConfig(service);
    }
}

/// <summary>
/// Response containing Cruise Control configuration.
/// </summary>
public sealed record CruiseControlConfigResponse(
    bool Enabled,
    string Mode,
    int AnalysisIntervalSeconds,
    int ThrottleRateBytesPerSec,
    int CooldownMinutes,
    BalanceGoals Goals);

/// <summary>
/// Request to update Cruise Control configuration. All fields are optional.
/// </summary>
public sealed class CruiseControlConfigUpdate
{
    /// <summary>
    /// New operating mode (SuggestOnly or AutoRebalance).
    /// </summary>
    public string? Mode { get; set; }

    /// <summary>
    /// New analysis interval in seconds.
    /// </summary>
    public int? AnalysisIntervalSeconds { get; set; }

    /// <summary>
    /// New throttle rate in bytes per second.
    /// </summary>
    public int? ThrottleRateBytesPerSec { get; set; }

    /// <summary>
    /// New cooldown period in minutes.
    /// </summary>
    public int? CooldownMinutes { get; set; }

    /// <summary>
    /// Updated balance goals.
    /// </summary>
    public BalanceGoalsUpdate? Goals { get; set; }
}

/// <summary>
/// Partial update for balance goals. All fields are optional.
/// </summary>
public sealed class BalanceGoalsUpdate
{
    public double? MaxPartitionImbalancePercent { get; set; }
    public double? MaxDiskImbalancePercent { get; set; }
    public double? MaxLeaderImbalancePercent { get; set; }
    public double? MaxNetworkImbalancePercent { get; set; }
    public int? MinPartitionsToRebalance { get; set; }
}
