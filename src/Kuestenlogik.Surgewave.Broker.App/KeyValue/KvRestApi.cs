using System.Collections.Concurrent;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Kuestenlogik.Surgewave.Broker.KeyValue;

/// <summary>
/// REST API endpoints for the broker-native Key-Value Store and Object Store.
/// </summary>
public static class KvRestApi
{
    private static readonly ConcurrentDictionary<string, ObjectStore> ObjectStores = new();

    /// <summary>
    /// Maps KV and Object Store REST API endpoints to the application.
    /// </summary>
    public static IEndpointRouteBuilder MapSurgewaveKv(
        this IEndpointRouteBuilder app,
        KvBucketManager bucketManager)
    {
        // -----------------------------------------------------------------------
        // KV Bucket endpoints
        // -----------------------------------------------------------------------
        var kvGroup = app.MapGroup("/api/kv/buckets")
            .WithTags("Key-Value Store");

        // POST /api/kv/buckets — Create bucket
        kvGroup.MapPost("/", async (HttpContext ctx) =>
        {
            var request = await ctx.Request.ReadFromJsonAsync<CreateBucketRequest>();
            if (request is null || string.IsNullOrWhiteSpace(request.Name))
                return Results.BadRequest(new { message = "Bucket name is required" });

            try
            {
                var config = new KvBucketConfig
                {
                    MaxHistoryPerKey = request.MaxHistoryPerKey ?? 1,
                    Ttl = request.TtlSeconds.HasValue ? TimeSpan.FromSeconds(request.TtlSeconds.Value) : null,
                    MaxValueSize = request.MaxValueSize ?? 1024 * 1024,
                    MaxBucketSize = request.MaxBucketSize,
                    Replicas = request.Replicas ?? 1,
                    Description = request.Description,
                };

                var bucket = await bucketManager.CreateBucketAsync(request.Name, config);
                return Results.Created($"/api/kv/buckets/{request.Name}", bucket.GetInfo());
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new { message = ex.Message });
            }
        })
        .WithName("CreateKvBucket")
        .WithSummary("Create a new KV bucket")
        .Produces<KvBucketInfo>(StatusCodes.Status201Created)
        .ProducesProblem(StatusCodes.Status409Conflict);

        // GET /api/kv/buckets — List buckets
        kvGroup.MapGet("/", () =>
        {
            var names = bucketManager.ListBuckets();
            var infos = new List<KvBucketInfo>();
            foreach (var name in names)
            {
                var bucket = bucketManager.GetBucket(name);
                if (bucket is not null)
                    infos.Add(bucket.GetInfo());
            }
            return Results.Ok(infos);
        })
        .WithName("ListKvBuckets")
        .WithSummary("List all KV buckets")
        .Produces<IReadOnlyList<KvBucketInfo>>();

        // GET /api/kv/buckets/{bucket} — Get bucket info
        kvGroup.MapGet("/{bucket}", (string bucket) =>
        {
            var b = bucketManager.GetBucket(bucket);
            return b is not null
                ? Results.Ok(b.GetInfo())
                : Results.NotFound(new { message = $"KV bucket '{bucket}' not found" });
        })
        .WithName("GetKvBucketInfo")
        .WithSummary("Get information about a KV bucket")
        .Produces<KvBucketInfo>()
        .ProducesProblem(StatusCodes.Status404NotFound);

        // DELETE /api/kv/buckets/{bucket} — Delete bucket
        kvGroup.MapDelete("/{bucket}", (string bucket) =>
        {
            return bucketManager.DeleteBucket(bucket)
                ? Results.Ok(new { message = $"Bucket '{bucket}' deleted" })
                : Results.NotFound(new { message = $"KV bucket '{bucket}' not found" });
        })
        .WithName("DeleteKvBucket")
        .WithSummary("Delete a KV bucket")
        .ProducesProblem(StatusCodes.Status404NotFound);

        // GET /api/kv/buckets/{bucket}/keys — List all keys
        kvGroup.MapGet("/{bucket}/keys", (string bucket) =>
        {
            var b = bucketManager.GetBucket(bucket);
            if (b is null)
                return Results.NotFound(new { message = $"KV bucket '{bucket}' not found" });

            return Results.Ok(b.Keys());
        })
        .WithName("ListKvKeys")
        .WithSummary("List all keys in a KV bucket")
        .Produces<IReadOnlyList<string>>()
        .ProducesProblem(StatusCodes.Status404NotFound);

        // GET /api/kv/buckets/{bucket}/keys/{key} — Get value
        kvGroup.MapGet("/{bucket}/keys/{key}", (string bucket, string key) =>
        {
            var b = bucketManager.GetBucket(bucket);
            if (b is null)
                return Results.NotFound(new { message = $"KV bucket '{bucket}' not found" });

            var entry = b.Get(key);
            return entry is not null
                ? Results.Ok(new KvEntryResponse(entry.Key, Convert.ToBase64String(entry.Value),
                    entry.Revision, entry.Created, entry.Operation.ToString()))
                : Results.NotFound(new { message = $"Key '{key}' not found in bucket '{bucket}'" });
        })
        .WithName("GetKvValue")
        .WithSummary("Get the value for a key")
        .Produces<KvEntryResponse>()
        .ProducesProblem(StatusCodes.Status404NotFound);

        // PUT /api/kv/buckets/{bucket}/keys/{key} — Put value (body = raw bytes)
        kvGroup.MapPut("/{bucket}/keys/{key}", async (string bucket, string key, HttpContext ctx) =>
        {
            var b = bucketManager.GetBucket(bucket);
            if (b is null)
                return Results.NotFound(new { message = $"KV bucket '{bucket}' not found" });

            using var ms = new MemoryStream();
            await ctx.Request.Body.CopyToAsync(ms);
            var value = ms.ToArray();

            try
            {
                var entry = await b.PutAsync(key, value);
                return Results.Ok(new KvEntryResponse(entry.Key, Convert.ToBase64String(entry.Value),
                    entry.Revision, entry.Created, entry.Operation.ToString()));
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new { message = ex.Message });
            }
        })
        .WithName("PutKvValue")
        .WithSummary("Put a value for a key (body = raw bytes)")
        .Produces<KvEntryResponse>()
        .ProducesProblem(StatusCodes.Status404NotFound);

        // DELETE /api/kv/buckets/{bucket}/keys/{key} — Delete key
        kvGroup.MapDelete("/{bucket}/keys/{key}", async (string bucket, string key) =>
        {
            var b = bucketManager.GetBucket(bucket);
            if (b is null)
                return Results.NotFound(new { message = $"KV bucket '{bucket}' not found" });

            var entry = await b.DeleteAsync(key);
            return Results.Ok(new KvEntryResponse(entry.Key, Convert.ToBase64String(entry.Value),
                entry.Revision, entry.Created, entry.Operation.ToString()));
        })
        .WithName("DeleteKvKey")
        .WithSummary("Delete a key from a KV bucket")
        .Produces<KvEntryResponse>()
        .ProducesProblem(StatusCodes.Status404NotFound);

        // GET /api/kv/buckets/{bucket}/keys/{key}/history — Get key history
        kvGroup.MapGet("/{bucket}/keys/{key}/history", (string bucket, string key) =>
        {
            var b = bucketManager.GetBucket(bucket);
            if (b is null)
                return Results.NotFound(new { message = $"KV bucket '{bucket}' not found" });

            var history = b.History(key);
            var response = history.Select(e =>
                new KvEntryResponse(e.Key, Convert.ToBase64String(e.Value),
                    e.Revision, e.Created, e.Operation.ToString())).ToList();
            return Results.Ok(response);
        })
        .WithName("GetKvKeyHistory")
        .WithSummary("Get the revision history for a key")
        .Produces<IReadOnlyList<KvEntryResponse>>()
        .ProducesProblem(StatusCodes.Status404NotFound);

        // -----------------------------------------------------------------------
        // Object Store endpoints
        // -----------------------------------------------------------------------
        var objGroup = app.MapGroup("/api/kv/objects")
            .WithTags("Object Store");

        // POST /api/kv/objects/{store} — Create object store
        objGroup.MapPost("/{store}", async (string store) =>
        {
            if (ObjectStores.ContainsKey(store))
                return Results.Conflict(new { message = $"Object store '{store}' already exists" });

            var objStore = new ObjectStore(store, bucketManager);
            if (!ObjectStores.TryAdd(store, objStore))
                return Results.Conflict(new { message = $"Object store '{store}' already exists" });

            await objStore.EnsureCreatedAsync();
            return Results.Created($"/api/kv/objects/{store}", new { name = store, message = "Object store created" });
        })
        .WithName("CreateObjectStore")
        .WithSummary("Create a new object store")
        .Produces(StatusCodes.Status201Created)
        .ProducesProblem(StatusCodes.Status409Conflict);

        // GET /api/kv/objects/{store} — List objects
        objGroup.MapGet("/{store}", (string store) =>
        {
            if (!ObjectStores.TryGetValue(store, out var objStore))
                return Results.NotFound(new { message = $"Object store '{store}' not found" });

            return Results.Ok(objStore.ListObjects());
        })
        .WithName("ListObjects")
        .WithSummary("List all objects in a store")
        .Produces<IReadOnlyList<ObjectInfo>>()
        .ProducesProblem(StatusCodes.Status404NotFound);

        // PUT /api/kv/objects/{store}/{name} — Put object (body = raw bytes)
        objGroup.MapPut("/{store}/{name}", async (string store, string name, HttpContext ctx) =>
        {
            if (!ObjectStores.TryGetValue(store, out var objStore))
                return Results.NotFound(new { message = $"Object store '{store}' not found" });

            var contentType = ctx.Request.ContentType;
            using var ms = new MemoryStream();
            await ctx.Request.Body.CopyToAsync(ms);
            var data = ms.ToArray();

            var info = await objStore.PutObjectAsync(name, data, contentType);
            return Results.Ok(info);
        })
        .WithName("PutObject")
        .WithSummary("Put an object into the store (body = raw bytes)")
        .Produces<ObjectInfo>()
        .ProducesProblem(StatusCodes.Status404NotFound);

        // GET /api/kv/objects/{store}/{name} — Get object
        objGroup.MapGet("/{store}/{name}", (string store, string name) =>
        {
            if (!ObjectStores.TryGetValue(store, out var objStore))
                return Results.NotFound(new { message = $"Object store '{store}' not found" });

            var result = objStore.GetObject(name);
            if (result is null)
                return Results.NotFound(new { message = $"Object '{name}' not found in store '{store}'" });

            return Results.Bytes(result.Data, result.Info.ContentType ?? "application/octet-stream");
        })
        .WithName("GetObject")
        .WithSummary("Get an object from the store")
        .Produces(StatusCodes.Status200OK)
        .ProducesProblem(StatusCodes.Status404NotFound);

        // DELETE /api/kv/objects/{store}/{name} — Delete object
        objGroup.MapDelete("/{store}/{name}", async (string store, string name) =>
        {
            if (!ObjectStores.TryGetValue(store, out var objStore))
                return Results.NotFound(new { message = $"Object store '{store}' not found" });

            var deleted = await objStore.DeleteObjectAsync(name);
            return deleted
                ? Results.Ok(new { message = $"Object '{name}' deleted from store '{store}'" })
                : Results.NotFound(new { message = $"Object '{name}' not found in store '{store}'" });
        })
        .WithName("DeleteObject")
        .WithSummary("Delete an object from the store")
        .ProducesProblem(StatusCodes.Status404NotFound);

        // GET /api/kv/objects/{store}/{name}/info — Get object info
        objGroup.MapGet("/{store}/{name}/info", (string store, string name) =>
        {
            if (!ObjectStores.TryGetValue(store, out var objStore))
                return Results.NotFound(new { message = $"Object store '{store}' not found" });

            var info = objStore.GetObjectInfo(name);
            return info is not null
                ? Results.Ok(info)
                : Results.NotFound(new { message = $"Object '{name}' not found in store '{store}'" });
        })
        .WithName("GetObjectInfo")
        .WithSummary("Get metadata for an object without downloading the data")
        .Produces<ObjectInfo>()
        .ProducesProblem(StatusCodes.Status404NotFound);

        return app;
    }
}

/// <summary>
/// Request body for creating a KV bucket.
/// </summary>
public sealed record CreateBucketRequest
{
    public string Name { get; init; } = "";
    public int? MaxHistoryPerKey { get; init; }
    public double? TtlSeconds { get; init; }
    public int? MaxValueSize { get; init; }
    public long? MaxBucketSize { get; init; }
    public int? Replicas { get; init; }
    public string? Description { get; init; }
}

/// <summary>
/// REST API response for a KV entry.
/// Values are base64-encoded for JSON transport.
/// </summary>
public sealed record KvEntryResponse(
    string Key,
    string ValueBase64,
    long Revision,
    DateTimeOffset Created,
    string Operation);
