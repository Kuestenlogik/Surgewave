using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// REST API endpoints for the Interactive Query Service.
/// Exposes registered state stores for read-only HTTP queries.
/// </summary>
public static class InteractiveQueryRestApi
{
    /// <summary>
    /// Maps Interactive Query Service REST API endpoints to the application.
    /// </summary>
    public static IEndpointRouteBuilder MapSurgewaveInteractiveQueries(
        this IEndpointRouteBuilder app,
        IStateStoreRegistry registry,
        StateStoreQueryExecutor executor)
    {
        var group = app.MapGroup("/api/streams/stores")
            .WithTags("Interactive Queries");

        // GET / — list all stores
        group.MapGet("/", () => ListStores(registry))
            .WithName("ListStateStores")
            .WithSummary("List all registered state stores with metadata")
            .Produces<StoreListResponse>();

        // GET /{name} — store metadata
        group.MapGet("/{name}", (string name) => GetStore(name, registry))
            .WithName("GetStateStore")
            .WithSummary("Get metadata for a single state store")
            .Produces<StateStoreInfo>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // GET /{name}/entries — all entries (paginated)
        group.MapGet("/{name}/entries", (string name, int offset = 0, int limit = 100) =>
                GetEntries(name, offset, limit, registry, executor))
            .WithName("GetStateStoreEntries")
            .WithSummary("Get a paginated list of all entries in a state store")
            .Produces<StoreEntriesResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // GET /{name}/entries/{key} — single entry by key
        group.MapGet("/{name}/entries/{key}", (string name, string key) =>
                GetEntry(name, key, registry, executor))
            .WithName("GetStateStoreEntry")
            .WithSummary("Get a single entry from a state store by key")
            .Produces<StoreEntryResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // GET /{name}/count — approximate entry count
        group.MapGet("/{name}/count", (string name) => GetCount(name, registry, executor))
            .WithName("GetStateStoreCount")
            .WithSummary("Get the approximate number of entries in a state store")
            .Produces<StoreCountResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        return app;
    }

    // -----------------------------------------------------------------------
    // Public handler methods — testable without spinning up a web host
    // -----------------------------------------------------------------------

    /// <summary>Returns the list of all registered stores.</summary>
    public static StoreListResponse ListStores(IStateStoreRegistry registry)
    {
        var stores = registry.GetAllStores();
        return new StoreListResponse { Stores = stores };
    }

    /// <summary>Returns metadata for a single store, or null if not found.</summary>
    public static StateStoreInfo? GetStoreInfo(string name, IStateStoreRegistry registry)
        => registry.GetStoreInfo(name);

    /// <summary>
    /// Returns a paginated page of entries for the given store.
    /// Returns null if the store does not exist.
    /// </summary>
    public static StoreEntriesResponse? GetEntries(
        string name,
        int offset,
        int limit,
        IStateStoreRegistry registry,
        StateStoreQueryExecutor executor)
    {
        if (registry.GetStoreInfo(name) is null)
            return null;

        if (offset < 0) offset = 0;
        if (limit <= 0) limit = 100;

        IReadOnlyList<KeyValuePair<string, object?>> pairs;
        try
        {
            pairs = executor.GetAll(name, offset, limit);
        }
        catch (KeyNotFoundException)
        {
            return null;
        }

        var totalCount = executor.GetCount(name);
        var entries = pairs
            .Select(p => new StoreEntryResponse { StoreName = name, Key = p.Key, Value = p.Value })
            .ToList();

        return new StoreEntriesResponse
        {
            StoreName = name,
            Entries = entries,
            Offset = offset,
            Limit = limit,
            TotalCount = totalCount
        };
    }

    /// <summary>
    /// Returns a single entry for the given key, or null if the store or key does not exist.
    /// </summary>
    public static StoreEntryResponse? GetEntry(
        string name,
        string key,
        IStateStoreRegistry registry,
        StateStoreQueryExecutor executor)
    {
        if (registry.GetStoreInfo(name) is null)
            return null;

        object? value;
        try
        {
            value = executor.GetByKey(name, key);
        }
        catch (KeyNotFoundException)
        {
            return null;
        }

        if (value is null)
            return null;

        return new StoreEntryResponse { StoreName = name, Key = key, Value = value };
    }

    /// <summary>
    /// Returns the approximate entry count for the given store, or null if the store does not exist.
    /// </summary>
    public static StoreCountResponse? GetCount(
        string name,
        IStateStoreRegistry registry,
        StateStoreQueryExecutor executor)
    {
        if (registry.GetStoreInfo(name) is null)
            return null;

        long count;
        try
        {
            count = executor.GetCount(name);
        }
        catch (KeyNotFoundException)
        {
            return null;
        }

        return new StoreCountResponse { StoreName = name, ApproximateCount = count };
    }

    // -----------------------------------------------------------------------
    // Private HTTP-result wrappers used by the route handlers
    // -----------------------------------------------------------------------

    private static IResult GetStore(string name, IStateStoreRegistry registry)
    {
        var info = GetStoreInfo(name, registry);
        return info is not null
            ? Results.Ok(info)
            : Results.NotFound(new { message = $"State store '{name}' not found" });
    }
}
