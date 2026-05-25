using System.Text.Json;

namespace Kuestenlogik.Surgewave.Edge;

/// <summary>
/// Tracks synchronization state between edge and cloud brokers.
/// Persists to a JSON file so sync progress survives edge restarts.
/// Thread-safe for concurrent access from the sync service and status queries.
/// </summary>
public sealed class EdgeSyncState
{
    private readonly object _lock = new();

    /// <summary>
    /// The edge node identifier.
    /// </summary>
    public string EdgeId { get; set; } = "";

    /// <summary>
    /// Map of topic to partition-offset pairs representing the last successfully synced offset.
    /// Key: topic name, Value: dictionary of partition to offset.
    /// </summary>
    public Dictionary<string, Dictionary<int, long>> SyncedOffsets { get; set; } = [];

    /// <summary>
    /// Timestamp of the last successful sync operation.
    /// </summary>
    public DateTimeOffset LastSyncAt { get; set; }

    /// <summary>
    /// Total number of messages synced since this state was created.
    /// </summary>
    public long TotalMessagesSynced { get; set; }

    /// <summary>
    /// Whether the cloud broker is currently reachable.
    /// </summary>
    public bool IsOnline { get; set; }

    /// <summary>
    /// Number of consecutive sync failures (resets on success).
    /// </summary>
    public int ConsecutiveFailures { get; set; }

    /// <summary>
    /// Gets the last synced offset for a specific topic/partition.
    /// Returns -1 if no offset has been recorded.
    /// </summary>
    public long GetSyncedOffset(string topic, int partition)
    {
        lock (_lock)
        {
            if (SyncedOffsets.TryGetValue(topic, out var partitions) &&
                partitions.TryGetValue(partition, out var offset))
            {
                return offset;
            }
            return -1;
        }
    }

    /// <summary>
    /// Updates the synced offset for a specific topic/partition.
    /// </summary>
    public void SetSyncedOffset(string topic, int partition, long offset)
    {
        lock (_lock)
        {
            if (!SyncedOffsets.TryGetValue(topic, out var partitions))
            {
                partitions = [];
                SyncedOffsets[topic] = partitions;
            }
            partitions[partition] = offset;
        }
    }

    /// <summary>
    /// Records a successful sync of the specified number of messages.
    /// </summary>
    public void RecordSync(int messageCount)
    {
        lock (_lock)
        {
            TotalMessagesSynced += messageCount;
            LastSyncAt = DateTimeOffset.UtcNow;
            ConsecutiveFailures = 0;
        }
    }

    /// <summary>
    /// Records a sync failure.
    /// </summary>
    public void RecordFailure()
    {
        lock (_lock)
        {
            ConsecutiveFailures++;
        }
    }

    /// <summary>
    /// Loads sync state from a JSON file. Returns a new empty state if the file does not exist.
    /// </summary>
    public static EdgeSyncState LoadFromFile(string path)
    {
        if (!File.Exists(path))
        {
            return new EdgeSyncState();
        }

        var json = File.ReadAllText(path);
        return JsonSerializer.Deserialize<EdgeSyncState>(json, SerializerOptions) ?? new EdgeSyncState();
    }

    /// <summary>
    /// Persists the current sync state to a JSON file.
    /// Uses atomic write (temp file + rename) to prevent corruption.
    /// </summary>
    public void SaveToFile(string path)
    {
        lock (_lock)
        {
            var json = JsonSerializer.Serialize(this, SerializerOptions);
            var tempPath = path + ".tmp";
            File.WriteAllText(tempPath, json);
            File.Move(tempPath, path, overwrite: true);
        }
    }

    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };
}
