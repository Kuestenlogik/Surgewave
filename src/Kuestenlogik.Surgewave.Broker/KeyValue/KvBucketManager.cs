using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.KeyValue;

/// <summary>
/// Manages named KV buckets. Each bucket is backed by a compacted Surgewave topic.
/// </summary>
public sealed class KvBucketManager : IDisposable
{
    private readonly LogManager _logManager;
    private readonly ILogger<KvBucketManager>? _logger;
    private readonly ConcurrentDictionary<string, KvBucket> _buckets = new();
    private bool _disposed;

    public KvBucketManager(LogManager logManager, ILogger<KvBucketManager>? logger = null)
    {
        _logManager = logManager;
        _logger = logger;
    }

    /// <summary>
    /// Create a new KV bucket with the given configuration.
    /// </summary>
    public async Task<KvBucket> CreateBucketAsync(string name, KvBucketConfig? config = null, CancellationToken cancellationToken = default)
    {
        config ??= new KvBucketConfig();

        if (_buckets.ContainsKey(name))
            throw new InvalidOperationException($"KV bucket '{name}' already exists");

        var bucket = new KvBucket(name, config, _logManager,
            _logger as ILogger);

        if (!_buckets.TryAdd(name, bucket))
        {
            bucket.Dispose();
            throw new InvalidOperationException($"KV bucket '{name}' already exists");
        }

        await bucket.EnsureBackingTopicAsync(cancellationToken);
        _logger?.LogInformation("Created KV bucket {Bucket}", LogSanitizer.Sanitize(name));

        return bucket;
    }

    /// <summary>
    /// Get an existing KV bucket by name.
    /// </summary>
    public KvBucket? GetBucket(string name)
    {
        _buckets.TryGetValue(name, out var bucket);
        return bucket;
    }

    /// <summary>
    /// Delete a KV bucket. The backing topic is not deleted.
    /// </summary>
    public bool DeleteBucket(string name)
    {
        if (_buckets.TryRemove(name, out var bucket))
        {
            bucket.Dispose();
            _logger?.LogInformation("Deleted KV bucket {Bucket}", LogSanitizer.Sanitize(name));
            return true;
        }
        return false;
    }

    /// <summary>
    /// List all bucket names.
    /// </summary>
    public IReadOnlyList<string> ListBuckets()
    {
        return _buckets.Keys.ToList();
    }

    /// <summary>
    /// Discover and restore buckets from existing _kv_* topics on broker startup.
    /// </summary>
    public async Task RestoreFromTopicsAsync(CancellationToken cancellationToken = default)
    {
        foreach (var topic in _logManager.ListTopics())
        {
            if (!topic.Name.StartsWith("_kv_", StringComparison.Ordinal))
                continue;

            var bucketName = topic.Name["_kv_".Length..];
            if (_buckets.ContainsKey(bucketName))
                continue;

            var config = new KvBucketConfig();
            var bucket = new KvBucket(bucketName, config, _logManager,
                _logger as ILogger);

            if (_buckets.TryAdd(bucketName, bucket))
            {
                await bucket.RestoreAsync(cancellationToken);
                _logger?.LogInformation("Restored KV bucket {Bucket} from topic {Topic}", bucketName, topic.Name);
            }
            else
            {
                bucket.Dispose();
            }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        foreach (var kvp in _buckets)
        {
            kvp.Value.Dispose();
        }
        _buckets.Clear();
    }
}
