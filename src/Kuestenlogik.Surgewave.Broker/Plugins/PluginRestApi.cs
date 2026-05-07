using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Broker.Plugins;

/// <summary>
/// REST API endpoints for plugin management â€” upload, download, and listing.
/// </summary>
public static class PluginRestApi
{
    /// <summary>
    /// Maps Surgewave plugin management endpoints under /api/plugins.
    /// </summary>
    /// <param name="verifier">Optional signature verifier. When non-null, uploads are verified
    /// against this signer's trust store before the package is installed.</param>
    /// <param name="requireSigned">When <c>true</c>, unsigned uploads are rejected at install time.</param>
    public static IEndpointRouteBuilder MapSurgewavePlugins(
        this IEndpointRouteBuilder app,
        PluginPackageManager packageManager,
        string pluginsDir,
        string packageCacheDir,
        PluginDiscovery? pluginDiscovery = null,
        ISppSigner? verifier = null,
        bool requireSigned = false)
    {
        Directory.CreateDirectory(packageCacheDir);

        var group = app.MapGroup("/api/plugins").WithTags("Plugins");

        // POST /api/plugins/upload â€” upload and install a .swpkg package
        group.MapPost("/upload", async (HttpRequest request, ILogger<PluginPackageManager> logger) =>
        {
            try
            {
                var form = await request.ReadFormAsync();
                var file = form.Files.GetFile("file");

                if (file == null || file.Length == 0)
                {
                    return Results.BadRequest(new { error = "No .swpkg file provided" });
                }

                if (!file.FileName.EndsWith(".swpkg", StringComparison.OrdinalIgnoreCase))
                {
                    return Results.BadRequest(new { error = "File must have .swpkg extension" });
                }

                // Save to cache directory
                var tempPath = Path.Combine(packageCacheDir, file.FileName);
                await using (var stream = new FileStream(tempPath, FileMode.Create))
                {
                    await file.CopyToAsync(stream);
                }

                // Install
                // Unload existing plugin before install (hot-swap)
                var pluginId = file.FileName.Replace(".swpkg", "");
                var existingPluginDir = Path.Combine(pluginsDir, pluginId);

                // Try to extract plugin ID from manifest before install
                try
                {
                    using var peekArchive = System.IO.Compression.ZipFile.OpenRead(tempPath);
                    var manifestEntry = peekArchive.GetEntry("plugin.json");
                    if (manifestEntry != null)
                    {
                        await using var ms = manifestEntry.Open();
                        var manifest = await System.Text.Json.JsonSerializer.DeserializeAsync<PluginManifest>(ms);
                        if (manifest != null)
                        {
                            pluginId = manifest.Id;
                            existingPluginDir = Path.Combine(pluginsDir, pluginId);
                        }
                    }
                }
                catch { /* fallback to filename-based ID */ }

                // Unload the old plugin assemblies if hot-swapping
                if (pluginDiscovery != null && Directory.Exists(existingPluginDir))
                {
                    foreach (var dll in Directory.GetFiles(existingPluginDir, "*.dll"))
                    {
                        pluginDiscovery.GetPluginLoader().UnloadPlugin(dll);
                    }
                    logger.LogInformation("Unloaded existing plugin for hot-swap: {PluginId}", pluginId);
                }

                var result = await packageManager.InstallAsync(
                    tempPath,
                    pluginsDir,
                    force: true,
                    verifier: verifier,
                    requireSigned: requireSigned);

                if (!result.Success)
                {
                    return Results.UnprocessableEntity(new
                    {
                        error = result.Error,
                        fileName = file.FileName
                    });
                }

                // Re-discover the newly installed plugin
                var connectorCount = 0;
                if (pluginDiscovery != null && result.InstallPath != null)
                {
                    connectorCount = pluginDiscovery.HotSwapPlugin(result.InstallPath);
                    logger.LogInformation("Hot-swapped plugin {PluginId}: {Count} connectors loaded",
                        result.Manifest?.Id, connectorCount);
                }

                return Results.Ok(new
                {
                    pluginId = result.Manifest?.Id,
                    version = result.Manifest?.Version,
                    wasUpgrade = result.WasUpgrade,
                    previousVersion = result.PreviousVersion,
                    installPath = result.InstallPath,
                    connectorCount
                });
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Plugin upload failed");
                return Results.Problem(detail: ex.Message, statusCode: 500);
            }
        })
        .DisableAntiforgery()
        .Produces(200)
        .Produces(400)
        .Produces(422);

        // POST /api/plugins/{id}/restart-pipelines â€” restart all running pipelines using this plugin
        group.MapPost("/{id}/restart-pipelines", async (string id, ILogger<PluginPackageManager> logger, CancellationToken ct) =>
        {
            var orchestrator = Kuestenlogik.Surgewave.Connect.Pipelines.PipelineOrchestratorHolder.Instance;
            if (orchestrator == null)
                return Results.Problem("Pipeline orchestrator not available", statusCode: 503);

            var allPipelines = orchestrator.GetAll();
            var affected = allPipelines
                .Where(p => p.Status == Kuestenlogik.Surgewave.Connect.Pipelines.PipelineStatus.Running &&
                    p.Nodes.Any(n => n.ConnectorType.Contains(id, StringComparison.OrdinalIgnoreCase)))
                .ToList();

            if (affected.Count == 0)
                return Results.Ok(new { pluginId = id, restartedPipelines = Array.Empty<string>(), message = "No running pipelines use this plugin" });

            var restarted = new List<string>();
            var errors = new List<string>();

            foreach (var pipeline in affected)
            {
                try
                {
                    await orchestrator.StopAsync(pipeline.Id, ct);
                    await orchestrator.StartAsync(pipeline.Id, cancellationToken: ct);
                    restarted.Add(pipeline.Id);
                    logger.LogInformation("Restarted pipeline {PipelineId} after plugin {PluginId} hot-swap", pipeline.Id, id);
                }
                catch (Exception ex)
                {
                    errors.Add($"{pipeline.Id}: {ex.Message}");
                    logger.LogError(ex, "Failed to restart pipeline {PipelineId}", pipeline.Id);
                }
            }

            return Results.Ok(new { pluginId = id, restartedPipelines = restarted, errors });
        });

        // GET /api/plugins/{id}/{version}/download â€” download a cached .swpkg package
        group.MapGet("/{id}/{version}/download", async (string id, string version) =>
        {
            var fileName = $"{id}-{version}.swpkg";
            var filePath = Path.Combine(packageCacheDir, fileName);

            if (!File.Exists(filePath))
            {
                return Results.NotFound(new { error = $"Package {fileName} not found" });
            }

            var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            return Results.File(stream, "application/octet-stream", fileName);
        })
        .Produces(200, contentType: "application/octet-stream")
        .Produces(404);

        // GET /api/plugins â€” list installed plugins
        group.MapGet("/", async (CancellationToken ct) =>
        {
            var plugins = new List<object>();
            await foreach (var plugin in packageManager.GetInstalledPluginsAsync(pluginsDir, ct))
            {
                plugins.Add(new
                {
                    id = plugin.Id,
                    name = plugin.Name,
                    version = plugin.Version,
                    assemblies = plugin.Manifest.Assemblies
                });
            }
            return Results.Ok(plugins);
        });

        return app;
    }
}
