using System.Collections.Concurrent;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Storage.Tiering;

/// <summary>
/// Tracks metadata about remote segments for a topic-partition.
/// This is stored locally and synced with remote storage.
/// </summary>
public sealed class RemoteLogMetadata : IDisposable
{
    private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };

    private readonly string _metadataPath;
    private readonly ConcurrentDictionary<long, RemoteSegmentState> _segments = new();
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private bool _disposed;

    public RemoteLogMetadata(string metadataPath)
    {
        _metadataPath = metadataPath;
        Load();
    }

    /// <summary>
    /// Mark a segment copy as started (CopySegmentStarted state).
    /// Call this before beginning the upload.
    /// </summary>
    public Guid StartCopy(long baseOffset, long endOffset, long size, long maxTimestampMs, int brokerId, Dictionary<int, long>? leaderEpochs = null)
    {
        var state = new RemoteSegmentState
        {
            SegmentId = Guid.NewGuid(),
            BaseOffset = baseOffset,
            EndOffset = endOffset,
            Size = size,
            MaxTimestampMs = maxTimestampMs,
            BrokerId = brokerId,
            State = RemoteLogSegmentState.CopySegmentStarted,
            SegmentLeaderEpochs = leaderEpochs ?? [],
            CopyStartedAt = DateTimeOffset.UtcNow,
            EventTimestampMs = DateTimeOffset.UtcNow,
            IsRemoteOnly = false
        };
        _segments[baseOffset] = state;
        Save();
        return state.SegmentId;
    }

    /// <summary>
    /// Mark a segment as uploaded to remote storage (CopySegmentFinished state).
    /// </summary>
    public void MarkUploaded(long baseOffset, long size, DateTimeOffset uploadedAt)
    {
        if (_segments.TryGetValue(baseOffset, out var state))
        {
            state.Size = size;
            state.UploadedAt = uploadedAt;
            state.State = RemoteLogSegmentState.CopySegmentFinished;
            state.EventTimestampMs = DateTimeOffset.UtcNow;
        }
        else
        {
            // Legacy path: create new state directly in finished state
            _segments[baseOffset] = new RemoteSegmentState
            {
                BaseOffset = baseOffset,
                Size = size,
                UploadedAt = uploadedAt,
                State = RemoteLogSegmentState.CopySegmentFinished,
                EventTimestampMs = DateTimeOffset.UtcNow,
                IsRemoteOnly = false
            };
        }
        Save();
    }

    /// <summary>
    /// Mark a segment for deletion (DeleteSegmentStarted state).
    /// </summary>
    public bool StartDelete(long baseOffset)
    {
        if (_segments.TryGetValue(baseOffset, out var state))
        {
            if (!state.State.CanTransitionTo(RemoteLogSegmentState.DeleteSegmentStarted))
            {
                return false;
            }
            state.State = RemoteLogSegmentState.DeleteSegmentStarted;
            state.EventTimestampMs = DateTimeOffset.UtcNow;
            Save();
            return true;
        }
        return false;
    }

    /// <summary>
    /// Mark a segment deletion as finished (DeleteSegmentFinished state).
    /// </summary>
    public bool FinishDelete(long baseOffset)
    {
        if (_segments.TryGetValue(baseOffset, out var state))
        {
            if (!state.State.CanTransitionTo(RemoteLogSegmentState.DeleteSegmentFinished))
            {
                return false;
            }
            state.State = RemoteLogSegmentState.DeleteSegmentFinished;
            state.EventTimestampMs = DateTimeOffset.UtcNow;
            Save();
            return true;
        }
        return false;
    }

    /// <summary>
    /// Mark a segment as remote-only (local copy deleted)
    /// </summary>
    public void MarkRemoteOnly(long baseOffset)
    {
        if (_segments.TryGetValue(baseOffset, out var state))
        {
            state.IsRemoteOnly = true;
            state.LocalDeletedAt = DateTimeOffset.UtcNow;
            Save();
        }
    }

    /// <summary>
    /// Mark a segment as locally cached (downloaded from remote)
    /// </summary>
    public void MarkCached(long baseOffset, string cachePath)
    {
        if (_segments.TryGetValue(baseOffset, out var state))
        {
            state.CachePath = cachePath;
            state.CachedAt = DateTimeOffset.UtcNow;
            Save();
        }
    }

    /// <summary>
    /// Clear cache entry for a segment
    /// </summary>
    public void ClearCacheEntry(long baseOffset)
    {
        if (_segments.TryGetValue(baseOffset, out var state))
        {
            state.CachePath = null;
            state.CachedAt = null;
            Save();
        }
    }

    /// <summary>
    /// Remove a segment from tracking (deleted from remote)
    /// </summary>
    public void Remove(long baseOffset)
    {
        _segments.TryRemove(baseOffset, out _);
        Save();
    }

    /// <summary>
    /// Check if a segment exists in remote storage
    /// </summary>
    public bool IsRemote(long baseOffset) => _segments.ContainsKey(baseOffset);

    /// <summary>
    /// Check if a segment is remote-only (local copy deleted)
    /// </summary>
    public bool IsRemoteOnly(long baseOffset) =>
        _segments.TryGetValue(baseOffset, out var state) && state.IsRemoteOnly;

    /// <summary>
    /// Get the cache path for a segment if cached locally
    /// </summary>
    public string? GetCachePath(long baseOffset) =>
        _segments.TryGetValue(baseOffset, out var state) ? state.CachePath : null;

    /// <summary>
    /// Get all remote segments
    /// </summary>
    public IReadOnlyList<RemoteSegmentState> GetAllSegments() =>
        _segments.Values.OrderBy(s => s.BaseOffset).ToList();

    /// <summary>
    /// Get remote-only segments
    /// </summary>
    public IReadOnlyList<RemoteSegmentState> GetRemoteOnlySegments() =>
        _segments.Values.Where(s => s.IsRemoteOnly).OrderBy(s => s.BaseOffset).ToList();

    /// <summary>
    /// Find the segment containing the given offset
    /// </summary>
    public RemoteSegmentState? FindSegmentContaining(long offset)
    {
        var segments = _segments.Values.OrderByDescending(s => s.BaseOffset);
        foreach (var segment in segments)
        {
            if (segment.BaseOffset <= offset)
            {
                return segment;
            }
        }
        return null;
    }

    private void Load()
    {
        if (!File.Exists(_metadataPath))
        {
            return;
        }

        try
        {
            var json = File.ReadAllText(_metadataPath);
            var data = JsonSerializer.Deserialize<MetadataFile>(json);
            if (data?.Segments != null)
            {
                foreach (var segment in data.Segments)
                {
                    _segments[segment.BaseOffset] = segment;
                }
            }
        }
        catch
        {
            // Ignore corrupt metadata files
        }
    }

    private void Save()
    {
        _semaphore.Wait();
        try
        {
            var data = new MetadataFile
            {
                Segments = _segments.Values.ToList()
            };

            var directory = Path.GetDirectoryName(_metadataPath);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var json = JsonSerializer.Serialize(data, s_jsonOptions);
            File.WriteAllText(_metadataPath, json);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private sealed class MetadataFile
    {
        public List<RemoteSegmentState> Segments { get; set; } = [];
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _semaphore.Dispose();
    }
}

/// <summary>
/// State of a segment in tiered storage.
/// Mirrors Kafka's RemoteLogSegmentMetadata (KIP-405).
/// </summary>
public sealed class RemoteSegmentState
{
    /// <summary>
    /// Unique identifier for this segment copy attempt (UUID).
    /// Generated per copy operation for idempotency.
    /// </summary>
    public Guid SegmentId { get; set; } = Guid.NewGuid();

    /// <summary>
    /// Base offset of the segment (inclusive start).
    /// </summary>
    public long BaseOffset { get; set; }

    /// <summary>
    /// End offset of the segment (inclusive end).
    /// </summary>
    public long EndOffset { get; set; }

    /// <summary>
    /// Size of the segment in bytes.
    /// </summary>
    public long Size { get; set; }

    /// <summary>
    /// Maximum timestamp in the segment.
    /// </summary>
    public long MaxTimestampMs { get; set; }

    /// <summary>
    /// Broker ID that uploaded this segment.
    /// </summary>
    public int BrokerId { get; set; }

    /// <summary>
    /// Current state of this segment in the remote storage lifecycle.
    /// </summary>
    public RemoteLogSegmentState State { get; set; } = RemoteLogSegmentState.CopySegmentStarted;

    /// <summary>
    /// Leader epochs covered by this segment.
    /// Maps epoch to the first offset in that epoch within this segment.
    /// </summary>
    public Dictionary<int, long> SegmentLeaderEpochs { get; set; } = [];

    /// <summary>
    /// Custom metadata from the storage provider (max 128 bytes).
    /// </summary>
    public byte[]? CustomMetadata { get; set; }

    /// <summary>
    /// When the copy operation started.
    /// </summary>
    public DateTimeOffset CopyStartedAt { get; set; }

    /// <summary>
    /// When the segment was successfully uploaded.
    /// </summary>
    public DateTimeOffset UploadedAt { get; set; }

    /// <summary>
    /// Whether the local copy has been deleted.
    /// </summary>
    public bool IsRemoteOnly { get; set; }

    /// <summary>
    /// When the local copy was deleted.
    /// </summary>
    public DateTimeOffset? LocalDeletedAt { get; set; }

    /// <summary>
    /// Path to local cache if segment was downloaded.
    /// </summary>
    public string? CachePath { get; set; }

    /// <summary>
    /// When segment was cached locally.
    /// </summary>
    public DateTimeOffset? CachedAt { get; set; }

    /// <summary>
    /// Whether the transaction index is empty for this segment.
    /// </summary>
    public bool TransactionIndexEmpty { get; set; } = true;

    /// <summary>
    /// Timestamp of last state update event.
    /// </summary>
    public DateTimeOffset EventTimestampMs { get; set; }

    /// <summary>
    /// Check if segment is available for reads based on state.
    /// </summary>
    public bool IsReadable => State.IsReadable();
}
