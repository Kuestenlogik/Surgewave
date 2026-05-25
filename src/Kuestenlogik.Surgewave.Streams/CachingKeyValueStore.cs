using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Write-behind cache decorator for IKeyValueStore.
/// Buffers put/delete operations in memory and flushes to the underlying store on Flush().
/// Reduces actual store writes dramatically during high-throughput processing.
/// </summary>
public sealed class CachingKeyValueStore<TKey, TValue> : IKeyValueStore<TKey, TValue>
    where TKey : notnull
{
    private readonly IKeyValueStore<TKey, TValue> _underlying;
    private readonly int _maxCacheSize;
    private readonly ConcurrentDictionary<TKey, CacheEntry> _cache = new();
    private long _dirtyCount;

    public string Name => _underlying.Name;
    public bool Persistent => _underlying.Persistent;
    public long ApproximateNumEntries => _underlying.ApproximateNumEntries;

    /// <summary>
    /// Number of dirty (unflushed) entries in the cache.
    /// </summary>
    public long DirtyCount => Interlocked.Read(ref _dirtyCount);

    /// <summary>
    /// Total entries currently in the cache.
    /// </summary>
    public int CacheSize => _cache.Count;

    public CachingKeyValueStore(IKeyValueStore<TKey, TValue> underlying, int maxCacheSize = 10_000)
    {
        _underlying = underlying;
        _maxCacheSize = maxCacheSize;
    }

    public void Init(ProcessorContext context)
    {
        _underlying.Init(context);
    }

    public TValue? Get(TKey key)
    {
        // Read from cache first
        if (_cache.TryGetValue(key, out var entry))
        {
            return entry.IsDeleted ? default : entry.Value;
        }

        // Fall through to underlying store
        return _underlying.Get(key);
    }

    public void Put(TKey key, TValue value)
    {
        var entry = new CacheEntry(value, true);

        _cache.AddOrUpdate(key, _ =>
        {
            Interlocked.Increment(ref _dirtyCount);
            return entry;
        }, (_, old) =>
        {
            if (!old.IsDirty)
                Interlocked.Increment(ref _dirtyCount);
            return entry;
        });

        // Evict if cache is full
        if (_cache.Count > _maxCacheSize)
        {
            FlushInternal();
        }
    }

    public TValue? PutIfAbsent(TKey key, TValue value)
    {
        var existing = Get(key);
        if (existing != null)
            return existing;

        Put(key, value);
        return value;
    }

    public void PutAll(IEnumerable<KeyValue<TKey, TValue>> entries)
    {
        foreach (var entry in entries)
        {
            Put(entry.Key, entry.Value);
        }
    }

    public TValue? Delete(TKey key)
    {
        var existing = Get(key);

        var tombstone = new CacheEntry(default!, true, true);
        _cache.AddOrUpdate(key, _ =>
        {
            Interlocked.Increment(ref _dirtyCount);
            return tombstone;
        }, (_, old) =>
        {
            if (!old.IsDirty)
                Interlocked.Increment(ref _dirtyCount);
            return tombstone;
        });

        return existing;
    }

    public IEnumerable<KeyValue<TKey, TValue>> Range(TKey from, TKey to)
    {
        // Flush cache before range query to ensure consistency
        FlushInternal();
        return _underlying.Range(from, to);
    }

    public IEnumerable<KeyValue<TKey, TValue>> All()
    {
        // Flush cache before full scan
        FlushInternal();
        return _underlying.All();
    }

    public void Flush()
    {
        FlushInternal();
        _underlying.Flush();
    }

    public void Close()
    {
        FlushInternal();
        _underlying.Close();
        _cache.Clear();
    }

    public void Dispose()
    {
        Close();
    }

    private void FlushInternal()
    {
        if (Interlocked.Read(ref _dirtyCount) == 0)
            return;

        foreach (var kvp in _cache)
        {
            if (!kvp.Value.IsDirty)
                continue;

            if (kvp.Value.IsDeleted)
            {
                _underlying.Delete(kvp.Key);
            }
            else
            {
                _underlying.Put(kvp.Key, kvp.Value.Value);
            }

            // Mark as clean
            _cache.TryUpdate(kvp.Key, kvp.Value with { IsDirty = false }, kvp.Value);
        }

        Interlocked.Exchange(ref _dirtyCount, 0);

        // Remove tombstones from cache after flush
        foreach (var kvp in _cache)
        {
            if (kvp.Value.IsDeleted && !kvp.Value.IsDirty)
            {
                _cache.TryRemove(kvp.Key, out _);
            }
        }
    }

    private record struct CacheEntry(TValue Value, bool IsDirty, bool IsDeleted = false);
}
