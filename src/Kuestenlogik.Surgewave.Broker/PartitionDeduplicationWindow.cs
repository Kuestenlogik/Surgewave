using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Per-partition bounded deduplication window tracking content hashes.
/// Thread-safe via ConcurrentDictionary.
/// </summary>
internal sealed class PartitionDeduplicationWindow
{
    private readonly ConcurrentDictionary<ulong, DeduplicationEntry> _entries = new();
    private readonly int _maxEntries;
    private readonly long _windowMs;

    public PartitionDeduplicationWindow(int maxEntries, long windowMs)
    {
        _maxEntries = maxEntries;
        _windowMs = windowMs;
    }

    public int Count => _entries.Count;

    /// <summary>
    /// Check if a hash already exists in the window.
    /// </summary>
    public bool TryCheckDuplicate(ulong hash, out long originalOffset)
    {
        if (_entries.TryGetValue(hash, out var entry))
        {
            originalOffset = entry.Offset;
            return true;
        }

        originalOffset = -1;
        return false;
    }

    /// <summary>
    /// Register a hash with its offset and timestamp.
    /// If the window is full, evicts the oldest entry.
    /// </summary>
    public void Register(ulong hash, long offset, long timestampMs)
    {
        // Evict if at capacity
        if (_entries.Count >= _maxEntries)
        {
            EvictOldest();
        }

        _entries[hash] = new DeduplicationEntry(offset, timestampMs);
    }

    /// <summary>
    /// Remove entries older than the window duration.
    /// Returns number of entries removed.
    /// </summary>
    public int Cleanup(long currentTimeMs)
    {
        var cutoff = currentTimeMs - _windowMs;
        var removed = 0;

        foreach (var kvp in _entries)
        {
            if (kvp.Value.TimestampMs < cutoff)
            {
                if (_entries.TryRemove(kvp.Key, out _))
                {
                    removed++;
                }
            }
        }

        return removed;
    }

    private void EvictOldest()
    {
        // Find and remove the oldest entry
        var oldestKey = default(ulong);
        var oldestTs = long.MaxValue;
        var found = false;

        foreach (var kvp in _entries)
        {
            if (kvp.Value.TimestampMs < oldestTs)
            {
                oldestTs = kvp.Value.TimestampMs;
                oldestKey = kvp.Key;
                found = true;
            }
        }

        if (found)
        {
            _entries.TryRemove(oldestKey, out _);
        }
    }
}

internal readonly record struct DeduplicationEntry(long Offset, long TimestampMs);
