using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Util;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Result of a deduplication check.
/// </summary>
public readonly record struct DeduplicationResult(bool IsDuplicate, long OriginalOffset);

/// <summary>
/// Broker-level content deduplication manager.
/// Detects duplicate messages by computing XxHash64 content fingerprints
/// and maintaining a bounded per-partition deduplication window.
/// </summary>
public sealed class DeduplicationManager : IDisposable
{
    private readonly ConcurrentDictionary<TopicPartition, PartitionDeduplicationWindow> _windows = new();
    private readonly DeduplicationConfig _config;
    private readonly BrokerMetrics? _metrics;
    private readonly ILogger<DeduplicationManager> _logger;
    private readonly Timer _cleanupTimer;

    public DeduplicationManager(DeduplicationConfig config, BrokerMetrics? metrics, ILogger<DeduplicationManager> logger)
    {
        _config = config;
        _metrics = metrics;
        _logger = logger;

        _cleanupTimer = new Timer(
            OnCleanup,
            null,
            config.CleanupIntervalMs,
            config.CleanupIntervalMs);
    }

    /// <summary>
    /// Check if a record batch is a duplicate. Does NOT register the hash.
    /// Call <see cref="Register"/> after a successful write.
    /// </summary>
    public DeduplicationResult CheckDuplicate(TopicPartition partition, ReadOnlySpan<byte> recordBatch)
    {
        var hash = RecordBatchHasher.ComputeContentHash(recordBatch);
        if (hash == 0)
            return new DeduplicationResult(false, -1);

        var window = GetOrCreateWindow(partition);
        if (window.TryCheckDuplicate(hash, out var originalOffset))
        {
            return new DeduplicationResult(true, originalOffset);
        }

        return new DeduplicationResult(false, -1);
    }

    /// <summary>
    /// Register a record batch hash after successful write.
    /// </summary>
    public void Register(TopicPartition partition, ReadOnlySpan<byte> recordBatch, long offset)
    {
        var hash = RecordBatchHasher.ComputeContentHash(recordBatch);
        if (hash == 0)
            return;

        var window = GetOrCreateWindow(partition);
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        window.Register(hash, offset, nowMs);
    }

    /// <summary>
    /// Get total number of tracked entries across all partitions.
    /// </summary>
    public long TotalEntries
    {
        get
        {
            long total = 0;
            foreach (var window in _windows.Values)
                total += window.Count;
            return total;
        }
    }

    private PartitionDeduplicationWindow GetOrCreateWindow(TopicPartition partition)
    {
        return _windows.GetOrAdd(partition, _ =>
            new PartitionDeduplicationWindow(_config.MaxEntriesPerPartition, _config.WindowSizeMs));
    }

    private void OnCleanup(object? state)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var totalRemoved = 0;

        foreach (var window in _windows.Values)
        {
            totalRemoved += window.Cleanup(nowMs);
        }

        if (totalRemoved > 0)
        {
            _logger.LogDebug("Deduplication cleanup: removed {Count} expired entries", totalRemoved);
        }
    }

    public void Dispose()
    {
        _cleanupTimer.Dispose();
    }
}
