using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Kuestenlogik.Surgewave.Cdc;

/// <summary>
/// REST API endpoints for CDC source management.
/// </summary>
public static class CdcRestApi
{
    /// <summary>
    /// Maps CDC REST API endpoints to the application.
    /// </summary>
    /// <param name="app">The endpoint route builder.</param>
    /// <param name="cdcService">The CDC service instance.</param>
    /// <returns>The endpoint route builder for chaining.</returns>
    public static IEndpointRouteBuilder MapSurgewaveCdc(this IEndpointRouteBuilder app, CdcService cdcService)
    {
        var group = app.MapGroup("/api/cdc/sources")
            .WithTags("CDC Management");

        group.MapPost("/", (CreateCdcSourceRequest request) => CreateSource(cdcService, request))
            .WithName("CreateCdcSource")
            .WithSummary("Create a new CDC source")
            .Produces<CdcSourceStatus>(StatusCodes.Status201Created)
            .ProducesValidationProblem()
            .Produces(StatusCodes.Status409Conflict);

        group.MapGet("/", () => ListSources(cdcService))
            .WithName("ListCdcSources")
            .WithSummary("List all CDC sources")
            .Produces<IReadOnlyList<CdcSourceStatus>>();

        group.MapGet("/{id}/status", (string id) => GetSourceStatus(cdcService, id))
            .WithName("GetCdcSourceStatus")
            .WithSummary("Get status of a specific CDC source")
            .Produces<CdcSourceStatus>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapDelete("/{id}", async (string id) => await RemoveSource(cdcService, id))
            .WithName("RemoveCdcSource")
            .WithSummary("Remove a CDC source")
            .Produces(StatusCodes.Status204NoContent)
            .ProducesProblem(StatusCodes.Status404NotFound);

        return app;
    }

    private static IResult CreateSource(CdcService cdcService, CreateCdcSourceRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.ConnectionString))
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                ["connectionString"] = ["Connection string is required"]
            });
        }

        var id = request.Id ?? Guid.NewGuid().ToString("N")[..8];

        var config = new CdcConfig
        {
            ConnectionString = request.ConnectionString,
            SlotName = request.SlotName ?? $"surgewave_cdc_{id}",
            PublicationName = request.PublicationName ?? $"surgewave_publication_{id}",
            Tables = request.Tables ?? [],
            TopicPrefix = request.TopicPrefix ?? "cdc.",
            IncludeSchema = request.IncludeSchema ?? true,
            SnapshotOnStart = request.SnapshotOnStart ?? false,
            Enabled = true
        };

        if (!cdcService.AddSource(id, config))
        {
            return Results.Conflict(new { message = $"CDC source with id '{id}' already exists" });
        }

        var status = cdcService.GetSourceStatus(id);
        return Results.Created($"/api/cdc/sources/{id}/status", status);
    }

    private static IResult ListSources(CdcService cdcService)
    {
        return Results.Ok(cdcService.GetAllSourceStatuses());
    }

    private static IResult GetSourceStatus(CdcService cdcService, string id)
    {
        var status = cdcService.GetSourceStatus(id);
        return status is not null
            ? Results.Ok(status)
            : Results.NotFound(new { message = $"CDC source '{id}' not found" });
    }

    private static async Task<IResult> RemoveSource(CdcService cdcService, string id)
    {
        var removed = await cdcService.RemoveSourceAsync(id);
        return removed
            ? Results.NoContent()
            : Results.NotFound(new { message = $"CDC source '{id}' not found" });
    }
}

/// <summary>
/// Request body for creating a new CDC source.
/// </summary>
public sealed record CreateCdcSourceRequest
{
    /// <summary>
    /// Optional identifier for the source. If not provided, a random ID is generated.
    /// </summary>
    public string? Id { get; init; }

    /// <summary>
    /// PostgreSQL connection string. Must include replication=database for logical replication.
    /// </summary>
    public string ConnectionString { get; init; } = "";

    /// <summary>
    /// Name of the replication slot to use or create.
    /// </summary>
    public string? SlotName { get; init; }

    /// <summary>
    /// Name of the PostgreSQL publication to use or create.
    /// </summary>
    public string? PublicationName { get; init; }

    /// <summary>
    /// List of tables to capture (empty = all tables).
    /// </summary>
    public List<string>? Tables { get; init; }

    /// <summary>
    /// Prefix for Surgewave topic names.
    /// </summary>
    public string? TopicPrefix { get; init; }

    /// <summary>
    /// Include schema name in topic name.
    /// </summary>
    public bool? IncludeSchema { get; init; }

    /// <summary>
    /// Snapshot existing data on start.
    /// </summary>
    public bool? SnapshotOnStart { get; init; }
}
