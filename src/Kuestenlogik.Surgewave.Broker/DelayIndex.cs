using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Entry tracking a delayed record batch.
/// </summary>
public readonly record struct DelayedRecord(long DeliverAtMs, long Offset) : IComparable<DelayedRecord>
{
    public int CompareTo(DelayedRecord other)
    {
        var cmp = DeliverAtMs.CompareTo(other.DeliverAtMs);
        return cmp != 0 ? cmp : Offset.CompareTo(other.Offset);
    }

    public static bool operator <(DelayedRecord left, DelayedRecord right) => left.CompareTo(right) < 0;
    public static bool operator >(DelayedRecord left, DelayedRecord right) => left.CompareTo(right) > 0;
    public static bool operator <=(DelayedRecord left, DelayedRecord right) => left.CompareTo(right) <= 0;
    public static bool operator >=(DelayedRecord left, DelayedRecord right) => left.CompareTo(right) >= 0;
}

/// <summary>
/// Per-partition delay index tracking messages with future delivery timestamps.
/// Used to filter delayed messages from fetch responses until their delivery time.
/// </summary>
public sealed class DelayIndex : IDisposable, IDelayIndex
{
    private readonly ConcurrentDictionary<TopicPartition, SortedSet<DelayedRecord>> _delayed = new();
    private readonly DeliveryDelayConfig _config;
    private readonly BrokerMetrics? _metrics;
    private readonly ILogger<DelayIndex> _logger;
    private readonly Timer _sweepTimer;

    public DelayIndex(DeliveryDelayConfig config, BrokerMetrics? metrics, ILogger<DelayIndex> logger)
    {
        _config = config;
        _metrics = metrics;
        _logger = logger;

        _sweepTimer = new Timer(
            OnSweep,
            null,
            config.IndexCleanupIntervalMs,
            config.IndexCleanupIntervalMs);
    }

    /// <summary>
    /// Record a delayed batch in the index.
    /// </summary>
    public void RecordDelayedBatch(TopicPartition partition, long offset, long deliverAtMs)
    {
        // Enforce max delay
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var maxDeliverAt = nowMs + _config.MaxDelayMs;
        if (deliverAtMs > maxDeliverAt)
            deliverAtMs = maxDeliverAt;

        var set = _delayed.GetOrAdd(partition, _ => new SortedSet<DelayedRecord>());
        lock (set)
        {
            set.Add(new DelayedRecord(deliverAtMs, offset));
        }

        _metrics?.RecordDelayedMessage(partition.Topic, partition.Partition);
    }

    /// <summary>
    /// Check if a partition has any delayed records.
    /// Fast O(1) check to avoid unnecessary filtering.
    /// </summary>
    public bool HasDelayedRecords(TopicPartition partition)
    {
        if (!_delayed.TryGetValue(partition, out var set))
            return false;

        lock (set)
        {
            return set.Count > 0;
        }
    }

    /// <summary>
    /// Check if a specific offset is delayed (delivery time is in the future).
    /// </summary>
    public bool IsDelayed(TopicPartition partition, long offset, long currentTimeMs)
    {
        if (!_delayed.TryGetValue(partition, out var set))
            return false;

        lock (set)
        {
            foreach (var record in set)
            {
                if (record.Offset == offset)
                    return record.DeliverAtMs > currentTimeMs;
            }
        }

        return false;
    }

    /// <summary>
    /// Get the total number of pending delayed records.
    /// </summary>
    public long PendingCount
    {
        get
        {
            long total = 0;
            foreach (var kvp in _delayed)
            {
                lock (kvp.Value)
                {
                    total += kvp.Value.Count;
                }
            }
            return total;
        }
    }

    /// <summary>
    /// Sweep expired entries (delivery time has passed).
    /// </summary>
    private void OnSweep(object? state)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var totalRemoved = 0;

        foreach (var kvp in _delayed)
        {
            lock (kvp.Value)
            {
                var toRemove = new List<DelayedRecord>();
                foreach (var record in kvp.Value)
                {
                    if (record.DeliverAtMs <= nowMs)
                        toRemove.Add(record);
                    else
                        break; // SortedSet is ordered by DeliverAtMs, no more expired
                }

                foreach (var record in toRemove)
                {
                    kvp.Value.Remove(record);
                    totalRemoved++;
                }
            }
        }

        if (totalRemoved > 0)
        {
            _logger.LogDebug("Delay index sweep: removed {Count} delivered entries", totalRemoved);
        }
    }

    public void Dispose()
    {
        _sweepTimer.Dispose();
    }
}
