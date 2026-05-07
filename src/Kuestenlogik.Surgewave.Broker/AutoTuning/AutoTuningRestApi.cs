using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Kuestenlogik.Surgewave.Broker.AutoTuning;

/// <summary>
/// REST API endpoints for the adaptive auto-tuning system.
/// </summary>
public static class AutoTuningRestApi
{
    /// <summary>
    /// Maps auto-tuning REST API endpoints to the application.
    /// </summary>
    /// <param name="app">The endpoint route builder.</param>
    /// <param name="autoTuningService">The auto-tuning service instance.</param>
    /// <returns>The endpoint route builder for chaining.</returns>
    public static IEndpointRouteBuilder MapAutoTuning(this IEndpointRouteBuilder app, AutoTuningService autoTuningService)
    {
        var group = app.MapGroup("/api/auto-tuning")
            .WithTags("Auto-Tuning");

        group.MapGet("/status", () => GetStatus(autoTuningService))
            .WithName("GetAutoTuningStatus")
            .WithSummary("Get current auto-tuning status and active recommendations")
            .Produces<AutoTuningStatusResponse>();

        group.MapGet("/history", () => GetHistory(autoTuningService))
            .WithName("GetAutoTuningHistory")
            .WithSummary("Get history of all auto-tuning recommendations")
            .Produces<IReadOnlyList<AutoTuningRecommendation>>();

        group.MapPost("/apply/{ruleId}", (string ruleId) => ApplyRecommendation(autoTuningService, ruleId))
            .WithName("ApplyAutoTuningRecommendation")
            .WithSummary("Manually apply a specific auto-tuning recommendation")
            .Produces<AutoTuningRecommendation>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        return app;
    }

    private static IResult GetStatus(AutoTuningService autoTuningService)
    {
        var config = autoTuningService.Config;
        var response = new AutoTuningStatusResponse(
            Enabled: config.Enabled,
            Mode: config.Mode.ToString(),
            AnalysisIntervalSeconds: config.AnalysisIntervalSeconds,
            ActiveRecommendations: autoTuningService.ActiveRecommendations,
            DisabledRules: config.DisabledRules);

        return Results.Ok(response);
    }

    private static IResult GetHistory(AutoTuningService autoTuningService)
    {
        return Results.Ok(autoTuningService.History);
    }

    private static IResult ApplyRecommendation(AutoTuningService autoTuningService, string ruleId)
    {
        var result = autoTuningService.ApplyRecommendation(ruleId);
        return result is not null
            ? Results.Ok(result)
            : Results.NotFound(new { message = $"No active recommendation found for rule '{ruleId}'" });
    }
}

/// <summary>
/// Response containing auto-tuning status and active recommendations.
/// </summary>
public sealed record AutoTuningStatusResponse(
    bool Enabled,
    string Mode,
    int AnalysisIntervalSeconds,
    IReadOnlyList<AutoTuningRecommendation> ActiveRecommendations,
    List<string> DisabledRules);
