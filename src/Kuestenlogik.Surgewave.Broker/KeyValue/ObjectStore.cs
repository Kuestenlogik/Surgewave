using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.KeyValue;

/// <summary>
/// Object store built on top of KV buckets. Supports chunked storage of large binary objects.
/// Object metadata and chunks are stored in a KV bucket named <c>_obj_{store}</c>.
/// </summary>
public sealed class ObjectStore
{
    /// <summary>Default chunk size: 128 KB.</summary>
    private const int DefaultChunkSize = 128 * 1024;

    private readonly string _storeName;
    private readonly KvBucketManager _bucketManager;
    private readonly ILogger? _logger;
    private KvBucket? _bucket;

    public string StoreName => _storeName;

    public ObjectStore(string storeName, KvBucketManager bucketManager, ILogger? logger = null)
    {
        _storeName = storeName;
        _bucketManager = bucketManager;
        _logger = logger;
    }

    /// <summary>
    /// Ensure the backing KV bucket exists.
    /// </summary>
    public async Task EnsureCreatedAsync(CancellationToken cancellationToken = default)
    {
        var bucketName = $"_obj_{_storeName}";
        _bucket = _bucketManager.GetBucket(bucketName);
        if (_bucket is not null)
            return;

        // Large max-value for chunks, high history not needed
        var config = new KvBucketConfig
        {
            MaxHistoryPerKey = 1,
            MaxValueSize = DefaultChunkSize + 4096, // chunk + overhead
        };

        _bucket = await _bucketManager.CreateBucketAsync(bucketName, config, cancellationToken);
    }

    /// <summary>
    /// Put an object into the store. The data is chunked into 128 KB pieces.
    /// </summary>
    public async Task<ObjectInfo> PutObjectAsync(string name, byte[] data, string? contentType = null, CancellationToken cancellationToken = default)
    {
        var bucket = GetBucket();
        var now = DateTimeOffset.UtcNow;

        // Chunk the data
        var chunkCount = (int)Math.Ceiling((double)data.Length / DefaultChunkSize);
        if (chunkCount == 0) chunkCount = 1; // at least one empty chunk for zero-length objects

        for (var i = 0; i < chunkCount; i++)
        {
            var chunkStart = i * DefaultChunkSize;
            var chunkLength = Math.Min(DefaultChunkSize, data.Length - chunkStart);
            var chunk = new byte[chunkLength];
            if (chunkLength > 0)
            {
                Buffer.BlockCopy(data, chunkStart, chunk, 0, chunkLength);
            }

            var chunkKey = $"{name}.chunk.{i}";
            await bucket.PutAsync(chunkKey, chunk, cancellationToken);
        }

        // Store metadata as JSON in a well-known key
        var info = new ObjectInfo(name, data.Length, chunkCount, contentType, now);
        var metadataBytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(info);
        await bucket.PutAsync(name, metadataBytes, cancellationToken);

        _logger?.LogInformation("Stored object {Name} in store {Store}: {Size} bytes, {Chunks} chunks",
            name, _storeName, data.Length, chunkCount);

        return info;
    }

    /// <summary>
    /// Get an object from the store. Returns null if the object does not exist.
    /// </summary>
    public ObjectResult? GetObject(string name)
    {
        var bucket = GetBucket();
        var metadataEntry = bucket.Get(name);
        if (metadataEntry is null)
            return null;

        ObjectInfo info;
        try
        {
            info = System.Text.Json.JsonSerializer.Deserialize<ObjectInfo>(metadataEntry.Value)!;
        }
        catch
        {
            return null;
        }

        // Reassemble chunks
        var data = new byte[info.Size];
        var offset = 0;
        for (var i = 0; i < info.Chunks; i++)
        {
            var chunkKey = $"{name}.chunk.{i}";
            var chunkEntry = bucket.Get(chunkKey);
            if (chunkEntry is null)
                return null; // Incomplete object

            var copyLen = Math.Min(chunkEntry.Value.Length, data.Length - offset);
            if (copyLen > 0)
            {
                Buffer.BlockCopy(chunkEntry.Value, 0, data, offset, copyLen);
            }
            offset += copyLen;
        }

        return new ObjectResult(info, data);
    }

    /// <summary>
    /// Delete an object and all its chunks.
    /// </summary>
    public async Task<bool> DeleteObjectAsync(string name, CancellationToken cancellationToken = default)
    {
        var bucket = GetBucket();
        var metadataEntry = bucket.Get(name);
        if (metadataEntry is null)
            return false;

        ObjectInfo info;
        try
        {
            info = System.Text.Json.JsonSerializer.Deserialize<ObjectInfo>(metadataEntry.Value)!;
        }
        catch
        {
            return false;
        }

        // Delete chunks
        for (var i = 0; i < info.Chunks; i++)
        {
            var chunkKey = $"{name}.chunk.{i}";
            await bucket.DeleteAsync(chunkKey, cancellationToken);
        }

        // Delete metadata
        await bucket.DeleteAsync(name, cancellationToken);

        _logger?.LogInformation("Deleted object {Name} from store {Store}", name, _storeName);
        return true;
    }

    /// <summary>
    /// List all objects in the store (returns metadata entries, not chunk keys).
    /// </summary>
    public IReadOnlyList<ObjectInfo> ListObjects()
    {
        var bucket = GetBucket();
        var keys = bucket.Keys();
        var result = new List<ObjectInfo>();

        foreach (var key in keys)
        {
            // Skip chunk keys
            if (key.Contains(".chunk.", StringComparison.Ordinal))
                continue;

            var entry = bucket.Get(key);
            if (entry is null)
                continue;

            try
            {
                var info = System.Text.Json.JsonSerializer.Deserialize<ObjectInfo>(entry.Value);
                if (info is not null)
                    result.Add(info);
            }
            catch
            {
                // Skip non-metadata entries
            }
        }

        return result;
    }

    /// <summary>
    /// Get object metadata without downloading the full data.
    /// </summary>
    public ObjectInfo? GetObjectInfo(string name)
    {
        var bucket = GetBucket();
        var entry = bucket.Get(name);
        if (entry is null)
            return null;

        try
        {
            return System.Text.Json.JsonSerializer.Deserialize<ObjectInfo>(entry.Value);
        }
        catch
        {
            return null;
        }
    }

    private KvBucket GetBucket()
    {
        return _bucket ?? throw new InvalidOperationException(
            $"Object store '{_storeName}' has not been initialized. Call EnsureCreatedAsync first.");
    }
}
