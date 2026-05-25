using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams.Changelog;

/// <summary>
/// Manages periodic checkpointing of state stores.
/// Stores can be checkpointed at regular intervals to enable faster recovery
/// (replay only from last checkpoint rather than full changelog replay).
/// </summary>
public sealed class CheckpointManager : IDisposable
{
    private readonly ConcurrentDictionary<string, StoreCheckpoint> _checkpoints = new();
    private readonly TimeSpan _checkpointInterval;
    private readonly ILogger _logger;
    private long _lastCheckpointTime;
    private bool _disposed;

    /// <summary>
    /// Gets the latest checkpoint for a given store name.
    /// </summary>
    public StoreCheckpoint? GetLatestCheckpoint(string storeName)
    {
        _checkpoints.TryGetValue(storeName, out var checkpoint);
        return checkpoint;
    }

    /// <summary>
    /// Gets all checkpoints.
    /// </summary>
    public IReadOnlyDictionary<string, StoreCheckpoint> Checkpoints => _checkpoints;

    public CheckpointManager(TimeSpan checkpointInterval, ILogger logger)
    {
        _checkpointInterval = checkpointInterval;
        _logger = logger;
        _lastCheckpointTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    /// <summary>
    /// Checks if it is time to checkpoint and performs checkpointing on all eligible stores.
    /// </summary>
    public void MaybeCheckpoint(IEnumerable<IStateStore> stores)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (now - _lastCheckpointTime < _checkpointInterval.TotalMilliseconds)
            return;

        foreach (var store in stores)
        {
            if (store is ICheckpointable checkpointable)
            {
                try
                {
                    var checkpoint = checkpointable.CreateCheckpoint();
                    _checkpoints[store.Name] = checkpoint;
                    _logger.LogDebug(
                        "Checkpointed store {StoreName} at offset {Offset} with {Entries} entries",
                        checkpoint.StoreName, checkpoint.ChangelogOffset, checkpoint.EntryCount);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to checkpoint store {StoreName}", store.Name);
                }
            }
        }

        _lastCheckpointTime = now;
    }

    /// <summary>
    /// Restores a store from its latest checkpoint if one exists.
    /// Returns the changelog offset to resume from, or 0 if no checkpoint exists.
    /// </summary>
    public long RestoreFromCheckpoint(IStateStore store)
    {
        if (store is not ICheckpointable checkpointable)
            return 0;

        if (!_checkpoints.TryGetValue(store.Name, out var checkpoint))
            return 0;

        try
        {
            checkpointable.RestoreFromCheckpoint(checkpoint);
            _logger.LogInformation(
                "Restored store {StoreName} from checkpoint at offset {Offset}",
                store.Name, checkpoint.ChangelogOffset);
            return checkpoint.ChangelogOffset;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to restore store {StoreName} from checkpoint", store.Name);
            return 0;
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _checkpoints.Clear();
        _disposed = true;
    }
}
