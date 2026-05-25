using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// REST API endpoints for managing WASM plugins.
/// Mounted under <c>/api/wasm</c>.
/// </summary>
public static class WasmRestApi
{
    /// <summary>
    /// Maps the WASM plugin REST API endpoints.
    /// </summary>
    public static IEndpointRouteBuilder MapWasmPluginApi(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/wasm")
            .WithTags("WASM Plugins");

        group.MapGet("/plugins", ListPlugins)
            .WithName("ListWasmPlugins")
            .WithSummary("List all WASM plugins with status")
            .Produces<IReadOnlyList<WasmPluginStatus>>();

        group.MapPost("/plugins/load", LoadPlugin)
            .WithName("LoadWasmPlugin")
            .WithSummary("Load a WASM plugin by its plugin ID")
            .Produces<WasmPluginStatus>(StatusCodes.Status201Created)
            .ProducesProblem(StatusCodes.Status400BadRequest);

        group.MapPost("/plugins/{id}/reload", ReloadPlugin)
            .WithName("ReloadWasmPlugin")
            .WithSummary("Hot-reload a WASM plugin")
            .Produces(StatusCodes.Status204NoContent)
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPost("/plugins/{id}/stop", StopPlugin)
            .WithName("StopWasmPlugin")
            .WithSummary("Stop a running WASM plugin")
            .Produces(StatusCodes.Status204NoContent)
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapDelete("/plugins/{id}", UnloadPlugin)
            .WithName("UnloadWasmPlugin")
            .WithSummary("Unload and remove a WASM plugin")
            .Produces(StatusCodes.Status204NoContent)
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapGet("/plugins/{id}/metrics", GetPluginMetrics)
            .WithName("GetWasmPluginMetrics")
            .WithSummary("Get metrics for a WASM plugin")
            .Produces<WasmPluginStatus>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapGet("/plugins/discover", DiscoverPlugins)
            .WithName("DiscoverWasmPlugins")
            .WithSummary("Scan the WASM plugins directory for available plugins")
            .Produces<IReadOnlyList<WasmPluginManifest>>();

        return app;
    }

    private static IReadOnlyList<WasmPluginStatus> ListPlugins(WasmPluginManager manager)
    {
        return manager.GetStatus();
    }

    private static async Task<IResult> LoadPlugin(
        LoadWasmPluginRequest request,
        WasmPluginManager manager)
    {
        if (string.IsNullOrWhiteSpace(request.PluginId))
        {
            return Results.BadRequest(new { error = "pluginId is required" });
        }

        try
        {
            var instance = await manager.LoadAndStartAsync(request.PluginId).ConfigureAwait(false);
            return Results.Created($"/api/wasm/plugins/{request.PluginId}", instance.GetStatus());
        }
        catch (FileNotFoundException ex)
        {
            return Results.NotFound(new { error = ex.Message });
        }
        catch (InvalidOperationException ex)
        {
            return Results.BadRequest(new { error = ex.Message });
        }
    }

    private static async Task<IResult> ReloadPlugin(string id, WasmPluginManager manager)
    {
        var status = manager.GetPluginStatus(id);
        if (status is null)
            return Results.NotFound(new { error = $"Plugin '{id}' not found" });

        await manager.ReloadAsync(id).ConfigureAwait(false);
        return Results.NoContent();
    }

    private static async Task<IResult> StopPlugin(string id, WasmPluginManager manager)
    {
        var status = manager.GetPluginStatus(id);
        if (status is null)
            return Results.NotFound(new { error = $"Plugin '{id}' not found" });

        await manager.StopAsync(id).ConfigureAwait(false);
        return Results.NoContent();
    }

    private static async Task<IResult> UnloadPlugin(string id, WasmPluginManager manager)
    {
        var status = manager.GetPluginStatus(id);
        if (status is null)
            return Results.NotFound(new { error = $"Plugin '{id}' not found" });

        await manager.StopAsync(id).ConfigureAwait(false);
        return Results.NoContent();
    }

    private static IResult GetPluginMetrics(string id, WasmPluginManager manager)
    {
        var status = manager.GetPluginStatus(id);
        if (status is null)
            return Results.NotFound(new { error = $"Plugin '{id}' not found" });

        return Results.Ok(status);
    }

    private static async Task<IResult> DiscoverPlugins(WasmPluginManager manager)
    {
        var manifests = await manager.DiscoverPluginsAsync().ConfigureAwait(false);
        return Results.Ok(manifests);
    }
}

/// <summary>
/// Request body for loading a WASM plugin.
/// </summary>
public sealed class LoadWasmPluginRequest
{
    /// <summary>The plugin ID (from manifest) to load.</summary>
    public string PluginId { get; set; } = "";
}
