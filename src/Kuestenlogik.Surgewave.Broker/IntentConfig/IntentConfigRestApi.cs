using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Broker.IntentConfig;

/// <summary>
/// REST API endpoints for Intent-Based Configuration.
/// Allows users to resolve natural-language intents into concrete topic configurations
/// and optionally create topics from those resolved configurations.
/// </summary>
public static class IntentConfigRestApi
{
    /// <summary>
    /// Maps the intent configuration REST API endpoints to the application.
    /// </summary>
    public static IEndpointRouteBuilder MapIntentConfig(this IEndpointRouteBuilder app, IntentConfigEngine engine)
    {
        var group = app.MapGroup("/api/intent")
            .WithTags("Intent-Based Configuration");

        group.MapPost("/resolve", (ConfigIntent intent) => Resolve(engine, intent))
            .WithName("ResolveIntent")
            .WithSummary("Resolve an intent description to concrete topic configuration (dry-run, no creation)")
            .Produces<IntentResult>()
            .ProducesValidationProblem();

        group.MapPost("/create", async (ConfigIntent intent, LogManager logManager) =>
                await CreateTopic(engine, intent, logManager))
            .WithName("ResolveAndCreateTopic")
            .WithSummary("Resolve intent AND create the topic with the resolved configuration")
            .Produces<IntentResult>(StatusCodes.Status201Created)
            .ProducesValidationProblem()
            .ProducesProblem(StatusCodes.Status500InternalServerError);

        group.MapGet("/keywords", () => GetKeywords(engine))
            .WithName("GetIntentKeywords")
            .WithSummary("List all available intent keywords (English and German)")
            .Produces<IReadOnlyList<string>>();

        group.MapGet("/rules", () => GetRules(engine))
            .WithName("GetIntentRules")
            .WithSummary("List all built-in intent rules with their keywords and configurations")
            .Produces<IReadOnlyList<IntentRuleSummary>>();

        return app;
    }

    private static IResult Resolve(IntentConfigEngine engine, ConfigIntent intent)
    {
        if (string.IsNullOrWhiteSpace(intent.Description))
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                ["Description"] = ["Intent description is required."]
            });
        }

        var result = engine.Resolve(intent);
        return Results.Ok(result);
    }

    private static async Task<IResult> CreateTopic(
        IntentConfigEngine engine,
        ConfigIntent intent,
        LogManager logManager)
    {
        if (string.IsNullOrWhiteSpace(intent.Description))
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                ["Description"] = ["Intent description is required."]
            });
        }

        var result = engine.Resolve(intent);

        try
        {
            await logManager.CreateTopicAsync(
                result.TopicName,
                result.Partitions,
                (short)result.ReplicationFactor,
                result.TopicConfig);

            return Results.Created($"/api/intent/resolve", result);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("already exists"))
        {
            return Results.Conflict(new { message = ex.Message, result });
        }
        catch (Exception ex)
        {
            return Results.Problem(
                detail: $"Failed to create topic '{result.TopicName}': {ex.Message}",
                statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    private static IResult GetKeywords(IntentConfigEngine engine)
    {
        return Results.Ok(engine.GetAvailableKeywords());
    }

    private static IResult GetRules(IntentConfigEngine engine)
    {
        var rules = engine.GetRules().Select(r => new IntentRuleSummary(
            r.Name,
            r.Description,
            r.Keywords,
            r.Config,
            r.Partitions,
            r.ReplicationFactor,
            r.Priority.ToString()
        )).ToList();

        return Results.Ok(rules);
    }
}

/// <summary>
/// Summary of an intent rule for API responses.
/// </summary>
public sealed record IntentRuleSummary(
    string Name,
    string Description,
    List<string> Keywords,
    Dictionary<string, string> Config,
    int? Partitions,
    int? ReplicationFactor,
    string Priority);
