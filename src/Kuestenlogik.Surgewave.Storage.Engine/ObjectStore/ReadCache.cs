using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;

/// <summary>
/// LRU read cache that caches remote segment data in memory.
/// Evicts least-recently-used entries when cache size exceeds the configured limit.
/// Thread-safe for concurrent access.
/// </summary>
public sealed class ReadCache : IDisposable
{
    private readonly long _maxSizeBytes;
    private readonly string _cacheDirectory;
    private readonly ConcurrentDictionary<string, CacheEntry> _entries = new();
    private readonly LinkedList<string> _accessOrder = new();
    private readonly object _evictionLock = new();
    private long _currentSize;
    private bool _disposed;

    /// <summary>
    /// Current total size of cached data in bytes.
    /// </summary>
    public long CurrentSize => Volatile.Read(ref _currentSize);

    public ReadCache(string cacheDirectory, long maxSizeBytes)
    {
        _cacheDirectory = cacheDirectory;
        _maxSizeBytes = maxSizeBytes;

        if (!string.IsNullOrEmpty(cacheDirectory) && !Directory.Exists(cacheDirectory))
        {
            Directory.CreateDirectory(cacheDirectory);
        }
    }

    /// <summary>
    /// Get cached data for the given key, or null if not cached.
    /// Promotes the entry in the LRU order on hit.
    /// </summary>
    public byte[]? Get(string key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_entries.TryGetValue(key, out var entry))
            return null;

        // Promote in LRU order
        lock (_evictionLock)
        {
            if (entry.Node?.List != null)
            {
                _accessOrder.Remove(entry.Node);
                _accessOrder.AddLast(entry.Node);
            }
        }

        return entry.Data;
    }

    /// <summary>
    /// Store data in the cache. Evicts LRU entries if the cache would exceed its size limit.
    /// </summary>
    public void Put(string key, byte[] data)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(data);

        lock (_evictionLock)
        {
            // Remove existing entry if present
            if (_entries.TryRemove(key, out var existing))
            {
                Interlocked.Add(ref _currentSize, -existing.Data.Length);
                if (existing.Node?.List != null)
                    _accessOrder.Remove(existing.Node);
            }

            // Evict LRU entries while over limit
            while (_currentSize + data.Length > _maxSizeBytes && _accessOrder.Count > 0)
            {
                var lruKey = _accessOrder.First!.Value;
                _accessOrder.RemoveFirst();

                if (_entries.TryRemove(lruKey, out var evicted))
                {
                    Interlocked.Add(ref _currentSize, -evicted.Data.Length);
                }
            }

            // Add new entry
            var node = _accessOrder.AddLast(key);
            var entry = new CacheEntry(data, node);
            _entries[key] = entry;
            Interlocked.Add(ref _currentSize, data.Length);
        }
    }

    /// <summary>
    /// Cache-aside pattern: returns cached data or fetches via the factory and caches the result.
    /// </summary>
    public async Task<byte[]?> GetOrFetchAsync(string key, Func<Task<byte[]?>> fetchFactory)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var cached = Get(key);
        if (cached != null)
            return cached;

        var data = await fetchFactory();
        if (data != null)
        {
            Put(key, data);
        }

        return data;
    }

    /// <summary>
    /// Evict all entries from the cache.
    /// </summary>
    public void Clear()
    {
        lock (_evictionLock)
        {
            _entries.Clear();
            _accessOrder.Clear();
            Volatile.Write(ref _currentSize, 0);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        Clear();
    }

    private sealed record CacheEntry(byte[] Data, LinkedListNode<string> Node);
}
