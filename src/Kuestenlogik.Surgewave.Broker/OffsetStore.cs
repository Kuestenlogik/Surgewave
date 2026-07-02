using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Persists consumer group offsets to disk.
/// Uses debounced async writes - updates are batched and flushed periodically.
/// </summary>
public sealed class OffsetStore : IDisposable
{
    private readonly string _offsetsDirectory;
    private readonly ILogger<OffsetStore> _logger;
    private readonly Dictionary<string, GroupOffsets> _groupOffsets = new();
    private readonly HashSet<string> _dirtyGroups = new();
    private readonly Lock _lock = new();
    private readonly Timer _flushTimer;
    private readonly TimeSpan _flushInterval;
    private bool _disposed;

    /// <summary>
    /// Creates a new OffsetStore with debounced writes.
    /// </summary>
    /// <param name="dataDirectory">Base data directory</param>
    /// <param name="logger">Logger instance</param>
    /// <param name="flushIntervalMs">How often to flush dirty groups to disk (default: 1000ms)</param>
    public OffsetStore(string dataDirectory, ILogger<OffsetStore> logger, int flushIntervalMs = 1000)
    {
        _offsetsDirectory = Path.Combine(dataDirectory, ".metadata", "groups");
        _logger = logger;
        _flushInterval = TimeSpan.FromMilliseconds(flushIntervalMs);

        Directory.CreateDirectory(_offsetsDirectory);
        LoadAllOffsets();

        // Start background flush timer
        _flushTimer = new Timer(FlushDirtyGroups, null, _flushInterval, _flushInterval);
    }

    /// <summary>
    /// Commits an offset for a consumer group.
    /// Updates memory immediately; disk write is debounced.
    /// </summary>
    public void CommitOffset(string groupId, string topic, int partition, long offset)
    {
        lock (_lock)
        {
            if (!_groupOffsets.TryGetValue(groupId, out var groupOffsets))
            {
                groupOffsets = new GroupOffsets { GroupId = groupId };
                _groupOffsets[groupId] = groupOffsets;
            }

            // Use value-type key for fast internal lookups (no string allocation)
            var fastKey = new TopicPartitionKey(topic, partition);
            groupOffsets.FastOffsets[fastKey] = offset;

            // Also update string-keyed dictionary for JSON persistence
            var stringKey = string.Concat(topic, ":", partition.ToString());
            groupOffsets.Offsets[stringKey] = offset;
            groupOffsets.LastModified = DateTimeOffset.UtcNow;

            // Mark as dirty - will be flushed by timer
            _dirtyGroups.Add(groupId);
        }
    }

    /// <summary>
    /// Gets the committed offset for a topic partition.
    /// Returns -1 if no offset has been committed.
    /// </summary>
    public long GetCommittedOffset(string groupId, string topic, int partition)
    {
        lock (_lock)
        {
            if (!_groupOffsets.TryGetValue(groupId, out var groupOffsets))
            {
                return -1;
            }

            // Use value-type key for fast lookup (no string allocation)
            var fastKey = new TopicPartitionKey(topic, partition);
            return groupOffsets.FastOffsets.GetValueOrDefault(fastKey, -1);
        }
    }

    /// <summary>
    /// Gets all committed offsets for a consumer group.
    /// </summary>
    public Dictionary<string, long> GetAllOffsets(string groupId)
    {
        lock (_lock)
        {
            if (!_groupOffsets.TryGetValue(groupId, out var groupOffsets))
            {
                return new Dictionary<string, long>();
            }

            return new Dictionary<string, long>(groupOffsets.Offsets);
        }
    }

    /// <summary>
    /// Checks if a consumer group has any committed offsets.
    /// </summary>
    public bool HasCommittedOffsets(string groupId)
    {
        lock (_lock)
        {
            return _groupOffsets.TryGetValue(groupId, out var offsets) && offsets.Offsets.Count > 0;
        }
    }

    /// <summary>
    /// Returns the IDs of all groups that have committed offsets.
    /// </summary>
    public IReadOnlyList<string> GetGroupIds()
    {
        lock (_lock)
        {
            return [.. _groupOffsets.Keys];
        }
    }

    /// <summary>
    /// Deletes a single committed offset for a consumer group.
    /// </summary>
    public void DeleteOffset(string groupId, string topic, int partition)
    {
        lock (_lock)
        {
            if (!_groupOffsets.TryGetValue(groupId, out var groupOffsets))
            {
                return;
            }

            // Remove from both dictionaries
            var fastKey = new TopicPartitionKey(topic, partition);
            var stringKey = string.Concat(topic, ":", partition.ToString());
            if (groupOffsets.FastOffsets.Remove(fastKey) | groupOffsets.Offsets.Remove(stringKey))
            {
                _dirtyGroups.Add(groupId);
            }
        }
    }

    /// <summary>
    /// Deletes all committed offsets for a consumer group and removes the persisted file.
    /// </summary>
    public void DeleteGroup(string groupId)
    {
        lock (_lock)
        {
            _groupOffsets.Remove(groupId);
            _dirtyGroups.Remove(groupId);
        }

        // Delete the persisted file
        try
        {
            var fileName = SanitizeFileName(groupId) + ".json";
            var filePath = Path.Combine(_offsetsDirectory, fileName);
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
                Log.OffsetStoreGroupDeleted(_logger, groupId);
            }
        }
        catch (Exception ex)
        {
            Log.OffsetStoreDeleteError(_logger, groupId, ex);
        }
    }

    /// <summary>
    /// Forces an immediate flush of all dirty groups to disk.
    /// </summary>
    public void Flush()
    {
        FlushDirtyGroups(null);
    }

    private void FlushDirtyGroups(object? state)
    {
        List<(string groupId, GroupOffsets offsets)> toFlush;

        lock (_lock)
        {
            if (_dirtyGroups.Count == 0)
                return;

            // Snapshot dirty groups and their data (inline loop avoids LINQ closure allocations)
            toFlush = new List<(string, GroupOffsets)>(_dirtyGroups.Count);
            foreach (var groupId in _dirtyGroups)
            {
                if (_groupOffsets.TryGetValue(groupId, out var offsets))
                {
                    toFlush.Add((groupId, offsets));
                }
            }

            _dirtyGroups.Clear();
        }

        // Persist outside the lock
        foreach (var (groupId, offsets) in toFlush)
        {
            PersistGroup(groupId, offsets);
        }

        if (toFlush.Count > 0)
        {
            Log.OffsetStoreFlushed(_logger, toFlush.Count);
        }
    }

    private void LoadAllOffsets()
    {
        if (!Directory.Exists(_offsetsDirectory))
        {
            return;
        }

        foreach (var file in Directory.GetFiles(_offsetsDirectory, "*.json"))
        {
            try
            {
                var json = File.ReadAllText(file);
                var groupOffsets = JsonSerializer.Deserialize(json, BrokerJsonContext.Default.GroupOffsets);

                if (groupOffsets != null && !string.IsNullOrEmpty(groupOffsets.GroupId))
                {
                    // Populate FastOffsets from loaded string-keyed offsets
                    foreach (var kvp in groupOffsets.Offsets)
                    {
                        var colonIndex = kvp.Key.LastIndexOf(':');
                        if (colonIndex > 0 && int.TryParse(kvp.Key.AsSpan(colonIndex + 1), out var partition))
                        {
                            var topic = kvp.Key[..colonIndex];
                            groupOffsets.FastOffsets[new TopicPartitionKey(topic, partition)] = kvp.Value;
                        }
                    }

                    _groupOffsets[groupOffsets.GroupId] = groupOffsets;
                    Log.OffsetStoreLoadedGroup(_logger, groupOffsets.GroupId, groupOffsets.Offsets.Count);
                }
            }
            catch (Exception ex)
            {
                Log.OffsetStoreLoadError(_logger, file, ex);
            }
        }

        Log.OffsetStoreInitialized(_logger, _groupOffsets.Count);
    }

    private void PersistGroup(string groupId, GroupOffsets offsets)
    {
        try
        {
            Directory.CreateDirectory(_offsetsDirectory);

            var fileName = SanitizeFileName(groupId) + ".json";
            var filePath = Path.Combine(_offsetsDirectory, fileName);
            var json = JsonSerializer.Serialize(offsets, BrokerJsonContext.Default.GroupOffsets);
            File.WriteAllText(filePath, json);
        }
        catch (Exception ex)
        {
            Log.OffsetStorePersistError(_logger, groupId, ex);
        }
    }

    private static string SanitizeFileName(string name)
    {
        var invalid = Path.GetInvalidFileNameChars();
        return string.Join("_", name.Split(invalid, StringSplitOptions.RemoveEmptyEntries));
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        // Stop the timer
        _flushTimer.Dispose();

        // Final flush of all dirty groups
        FlushDirtyGroups(null);
    }
}

internal sealed class GroupOffsets
{
    public string GroupId { get; set; } = string.Empty;
    public Dictionary<string, long> Offsets { get; set; } = new();
    public DateTimeOffset LastModified { get; set; }

    // Internal fast lookup using value-type keys (avoids string allocation on every lookup)
    [System.Text.Json.Serialization.JsonIgnore]
    internal Dictionary<TopicPartitionKey, long> FastOffsets { get; set; } = new();
}

/// <summary>
/// Value-type key for topic-partition lookups (avoids string allocation).
/// </summary>
internal readonly record struct TopicPartitionKey(string Topic, int Partition);
