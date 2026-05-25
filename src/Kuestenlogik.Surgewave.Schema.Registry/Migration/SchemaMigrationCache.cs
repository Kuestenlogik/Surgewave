using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Schema.Registry.Migration;

/// <summary>
/// LRU cache for compiled schema migrators.
/// Keyed by (subject, fromVersion, toVersion), evicts least-recently-used entries
/// when <see cref="SchemaMigrationConfig.MaxCachedMigrators"/> is exceeded.
/// </summary>
public sealed class SchemaMigrationCache
{
    private readonly int _maxEntries;
    private readonly ConcurrentDictionary<(string Subject, int FromVersion, int ToVersion), CacheEntry> _cache = new();
    private long _accessCounter;
    private long _hits;
    private long _misses;
    private long _evictions;

    /// <summary>
    /// Number of cache hits since startup.
    /// </summary>
    public long Hits => Interlocked.Read(ref _hits);

    /// <summary>
    /// Number of cache misses since startup.
    /// </summary>
    public long Misses => Interlocked.Read(ref _misses);

    /// <summary>
    /// Current number of cached migrators.
    /// </summary>
    public int Count => _cache.Count;

    /// <summary>
    /// Number of evictions since startup.
    /// </summary>
    public long Evictions => Interlocked.Read(ref _evictions);

    public SchemaMigrationCache(int maxEntries = 100)
    {
        _maxEntries = maxEntries;
    }

    /// <summary>
    /// Look up a cached migrator function for the given subject and version range.
    /// </summary>
    /// <returns>The cached migrator, or null if not found.</returns>
    public Func<byte[], byte[]>? GetMigrator(string subject, int fromVersion, int toVersion)
    {
        var key = (subject, fromVersion, toVersion);

        if (_cache.TryGetValue(key, out var entry))
        {
            entry.LastAccess = Interlocked.Increment(ref _accessCounter);
            Interlocked.Increment(ref _hits);
            return entry.Migrator;
        }

        Interlocked.Increment(ref _misses);
        return null;
    }

    /// <summary>
    /// Cache a compiled migrator function for the given subject and version range.
    /// Evicts the least-recently-used entry if the cache is full.
    /// </summary>
    public void CacheMigrator(string subject, int fromVersion, int toVersion, Func<byte[], byte[]> migrator)
    {
        var key = (subject, fromVersion, toVersion);
        var entry = new CacheEntry
        {
            Migrator = migrator,
            LastAccess = Interlocked.Increment(ref _accessCounter),
            CreatedAt = DateTimeOffset.UtcNow
        };

        _cache[key] = entry;

        // Evict if over capacity
        if (_cache.Count > _maxEntries)
        {
            EvictOldest();
        }
    }

    /// <summary>
    /// Clear all cached migrators.
    /// </summary>
    public void Clear()
    {
        _cache.Clear();
    }

    /// <summary>
    /// Get cache statistics.
    /// </summary>
    public SchemaMigrationCacheStats GetStats()
    {
        var hits = Hits;
        var misses = Misses;
        return new SchemaMigrationCacheStats
        {
            Count = Count,
            MaxEntries = _maxEntries,
            Hits = hits,
            Misses = misses,
            Evictions = Evictions,
            HitRate = hits + misses > 0
                ? (double)hits / (hits + misses)
                : 0.0
        };
    }

    private void EvictOldest()
    {
        (string Subject, int FromVersion, int ToVersion)? oldestKey = null;
        long oldestAccess = long.MaxValue;

        foreach (var kvp in _cache)
        {
            if (kvp.Value.LastAccess < oldestAccess)
            {
                oldestAccess = kvp.Value.LastAccess;
                oldestKey = kvp.Key;
            }
        }

        if (oldestKey is not null && _cache.TryRemove(oldestKey.Value, out _))
        {
            Interlocked.Increment(ref _evictions);
        }
    }

    private sealed class CacheEntry
    {
        public required Func<byte[], byte[]> Migrator { get; init; }
        public long LastAccess { get; set; }
        public DateTimeOffset CreatedAt { get; init; }
    }
}

/// <summary>
/// Statistics about the schema migration cache.
/// </summary>
public sealed class SchemaMigrationCacheStats
{
    /// <summary>Current number of cached migrators.</summary>
    public int Count { get; init; }

    /// <summary>Maximum cache capacity.</summary>
    public int MaxEntries { get; init; }

    /// <summary>Total cache hits.</summary>
    public long Hits { get; init; }

    /// <summary>Total cache misses.</summary>
    public long Misses { get; init; }

    /// <summary>Total evictions.</summary>
    public long Evictions { get; init; }

    /// <summary>Cache hit rate (0.0 to 1.0).</summary>
    public double HitRate { get; init; }
}
