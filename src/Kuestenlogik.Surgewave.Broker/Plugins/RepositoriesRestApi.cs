using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Broker.Plugins;

/// <summary>
/// REST endpoints for plugin repository (a.k.a. source feed) management.
/// Backs the Control UI at <c>/plugins/sources</c> — operator edits the
/// broker's canonical repository list instead of a Control-local JSON file.
/// All endpoints share the broker's <see cref="RepositoryStore"/> singleton,
/// which serialises file-system access.
/// </summary>
public static class RepositoriesRestApi
{
    public static IEndpointRouteBuilder MapSurgewaveRepositories(
        this IEndpointRouteBuilder app,
        RepositoryStore store)
    {
        var group = app.MapGroup("/api/plugins/repositories").WithTags("Plugins-Repositories");

        group.MapGet("/", () => Results.Ok(new
        {
            configPath = store.ConfigPath,
            repositories = store.List(),
        }));

        group.MapGet("/{name}", (string name) =>
        {
            var entry = store.Get(name);
            return entry is null
                ? Results.NotFound(new { error = $"No repository named '{name}'." })
                : Results.Ok(entry);
        });

        group.MapPost("/", (RepositoryEntry entry, ILogger<RepositoryStore> logger) =>
        {
            try
            {
                var added = store.Add(entry);
                logger.LogInformation("Repository '{Name}' added ({Type} {Source})", added.Name, added.Type, added.Source);
                return Results.Created($"/api/plugins/repositories/{added.Name}", added);
            }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
            catch (InvalidOperationException ex) { return Results.Conflict(new { error = ex.Message }); }
        });

        group.MapPut("/{name}", (string name, RepositoryEntry entry, ILogger<RepositoryStore> logger) =>
        {
            try
            {
                var updated = store.Update(name, entry);
                logger.LogInformation("Repository '{Name}' updated", updated.Name);
                return Results.Ok(updated);
            }
            catch (KeyNotFoundException) { return Results.NotFound(new { error = $"No repository named '{name}'." }); }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
        });

        group.MapDelete("/{name}", (string name, ILogger<RepositoryStore> logger) =>
        {
            try
            {
                var removed = store.Remove(name);
                if (!removed) return Results.NotFound(new { error = $"No repository named '{name}'." });
                logger.LogInformation("Repository '{Name}' deleted", name);
                return Results.Ok(new { name, deleted = true });
            }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
        });

        return app;
    }
}
