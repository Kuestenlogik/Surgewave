using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// Tracks per-partition buffer counts to enable independent backpressure decisions per partition.
/// </summary>
public sealed class PartitionBackpressureTracker
{
    private readonly BackpressureConfig _config;
    private readonly int _highWatermark;
    private readonly int _lowWatermark;
    private readonly ConcurrentDictionary<(string Topic, int Partition), int> _counters = new();

    public PartitionBackpressureTracker(BackpressureConfig config)
    {
        _config = config;
        _highWatermark = (int)(config.MaxBufferedRecords * config.HighWatermarkRatio);
        _lowWatermark = (int)(config.MaxBufferedRecords * config.LowWatermarkRatio);
    }

    /// <summary>
    /// Increments the buffered record count for the given partition.
    /// </summary>
    public void RecordBuffered(string topic, int partition)
    {
        _counters.AddOrUpdate((topic, partition), 1, (_, current) => current + 1);
    }

    /// <summary>
    /// Decrements the buffered record count for the given partition.
    /// </summary>
    public void RecordProcessed(string topic, int partition)
    {
        _counters.AddOrUpdate((topic, partition), 0, (_, current) => current > 0 ? current - 1 : 0);
    }

    /// <summary>
    /// Returns true when the partition's buffer has reached the high watermark.
    /// </summary>
    public bool ShouldPause(string topic, int partition)
    {
        return _counters.TryGetValue((topic, partition), out var count) && count >= _highWatermark;
    }

    /// <summary>
    /// Returns true when the partition's buffer has dropped to or below the low watermark.
    /// </summary>
    public bool ShouldResume(string topic, int partition)
    {
        if (!_counters.TryGetValue((topic, partition), out var count))
            return true;

        return count <= _lowWatermark;
    }

    /// <summary>
    /// Returns the current buffered record count for the given partition.
    /// </summary>
    public int GetCount(string topic, int partition)
    {
        return _counters.GetValueOrDefault((topic, partition), 0);
    }

    /// <summary>
    /// Removes tracking state for a partition (e.g. after revocation).
    /// </summary>
    public void Remove(string topic, int partition)
    {
        _counters.TryRemove((topic, partition), out _);
    }

    /// <summary>
    /// Resets all partition counters.
    /// </summary>
    public void Reset()
    {
        _counters.Clear();
    }
}
