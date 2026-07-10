using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Entry tracking a message with a TTL expiry.
/// </summary>
public readonly record struct TtlRecord(long ExpiryMs, long Offset) : IComparable<TtlRecord>
{
    public int CompareTo(TtlRecord other)
    {
        var cmp = ExpiryMs.CompareTo(other.ExpiryMs);
        return cmp != 0 ? cmp : Offset.CompareTo(other.Offset);
    }

    public static bool operator <(TtlRecord left, TtlRecord right) => left.CompareTo(right) < 0;
    public static bool operator >(TtlRecord left, TtlRecord right) => left.CompareTo(right) > 0;
    public static bool operator <=(TtlRecord left, TtlRecord right) => left.CompareTo(right) <= 0;
    public static bool operator >=(TtlRecord left, TtlRecord right) => left.CompareTo(right) >= 0;
}

/// <summary>
/// Per-partition TTL index tracking messages with expiry timestamps.
/// Used to filter expired messages from fetch responses after their TTL has elapsed.
/// </summary>
public sealed class TtlIndex : IDisposable, ITtlIndex
{
    private readonly ConcurrentDictionary<TopicPartition, SortedSet<TtlRecord>> _expiring = new();
    private readonly TtlConfig _config;
    private readonly BrokerMetrics? _metrics;
    private readonly ILogger<TtlIndex> _logger;
    private readonly Timer _sweepTimer;

    public TtlIndex(TtlConfig config, BrokerMetrics? metrics, ILogger<TtlIndex> logger)
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
    /// Record a batch with TTL in the index.
    /// </summary>
    public void RecordTtlBatch(TopicPartition partition, long offset, long expiryMs)
    {
        // Enforce max TTL
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var maxExpiry = nowMs + _config.MaxTtlMs;
        if (expiryMs > maxExpiry)
            expiryMs = maxExpiry;

        var set = _expiring.GetOrAdd(partition, _ => new SortedSet<TtlRecord>());
        lock (set)
        {
            set.Add(new TtlRecord(expiryMs, offset));
        }

        _metrics?.RecordTtlMessage(partition.Topic, partition.Partition);
    }

    /// <summary>
    /// Check if a partition has any TTL-tracked records.
    /// Fast O(1) check to avoid unnecessary filtering.
    /// </summary>
    public bool HasTtlRecords(TopicPartition partition)
    {
        if (!_expiring.TryGetValue(partition, out var set))
            return false;

        lock (set)
        {
            return set.Count > 0;
        }
    }

    /// <summary>
    /// Check if a specific offset has expired (expiry time is in the past).
    /// </summary>
    public bool IsExpired(TopicPartition partition, long offset, long currentTimeMs)
    {
        if (!_expiring.TryGetValue(partition, out var set))
            return false;

        lock (set)
        {
            foreach (var record in set)
            {
                if (record.Offset == offset)
                    return record.ExpiryMs <= currentTimeMs;
            }
        }

        return false;
    }

    /// <summary>
    /// Get the total number of TTL-tracked records.
    /// </summary>
    public long TrackedCount
    {
        get
        {
            long total = 0;
            foreach (var kvp in _expiring)
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
    /// Sweep entries that have expired (expiry time has passed).
    /// Expired entries are removed from the index since they will be filtered on fetch.
    /// </summary>
    private void OnSweep(object? state)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var totalRemoved = 0;

        foreach (var kvp in _expiring)
        {
            lock (kvp.Value)
            {
                var toRemove = new List<TtlRecord>();
                foreach (var record in kvp.Value)
                {
                    if (record.ExpiryMs <= nowMs)
                        toRemove.Add(record);
                    else
                        break; // SortedSet is ordered by ExpiryMs, no more expired
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
            _logger.LogDebug("TTL index sweep: removed {Count} expired entries", totalRemoved);
        }
    }

    public void Dispose()
    {
        _sweepTimer.Dispose();
    }
}
