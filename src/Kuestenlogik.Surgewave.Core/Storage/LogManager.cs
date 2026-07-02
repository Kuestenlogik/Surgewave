using System.Collections.Concurrent;
using System.Diagnostics;
using Kuestenlogik.Surgewave.Core.Configuration;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Pipeline;
using Kuestenlogik.Surgewave.Core.Util;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Manages all topic partition logs with built-in Channel-based pipeline for optimal performance
/// </summary>
public sealed class LogManager : IDisposable
{
    private readonly string _dataDirectory;

    /// <summary>
    /// Absolute or relative path to the data directory holding every topic's
    /// log segments. Surfaced read-only so admin paths (KIP-1106
    /// <c>DescribeLogDirs</c>, ops dashboards) can answer "where is the data"
    /// without each caller threading the value separately.
    /// </summary>
    public string DataDirectory { get; }
    private readonly ILogSegmentFactory _segmentFactory;
    private readonly ConcurrentDictionary<TopicPartition, IPartitionLog> _logs = new();
    private readonly ConcurrentDictionary<TopicPartition, object> _logCreationLocks = new();
    private readonly ConcurrentDictionary<string, TopicMetadata> _topics = new();
    private readonly ConcurrentDictionary<Guid, string> _topicIdToName = new(); // TopicId → TopicName mapping

    // Channel-based write pipeline
    private readonly WriteChannelPipeline _writePipeline;
    private readonly CancellationTokenSource _shutdownCts = new();

    // Retention
    private readonly RetentionPolicy _retentionPolicy;
    private readonly Task? _retentionTask;
    private readonly TimeSpan _retentionCheckInterval;

    // Compaction
    private readonly ConcurrentDictionary<TopicPartition, PartitionCompactionStats> _compactionStats = new();
    private readonly CompactionConfig _compactionConfig;
    private readonly LogCompactor _compactor;
    private readonly Task? _compactionTask;
    private readonly TimeSpan _compactionCheckInterval;

    // Metadata persistence
    private readonly TopicsMetadataPersistence _metadataPersistence;

    // Logging
    private readonly ILogger<LogManager>? _logger;

    // Topic lifecycle hooks (KIP-1010). Stored in a ConcurrentBag so registrations are
    // lock-free and ordering reflects registration order. Hooks fire after the topic
    // mutation has been applied — they observe, they don't veto.
    private readonly ConcurrentBag<ITopicLifecycleHook> _topicHooks = new();

    private bool _disposed;

    public LogManager(
        string dataDirectory,
        ILogSegmentFactory? segmentFactory = null,
        int writeWorkers = 0, // 0 = auto (2x processor count, min 8)
        int writeChannelCapacity = 10000,
        int writeBatchSize = 100, // Batch size for high throughput
        RetentionPolicy? retentionPolicy = null,
        TimeSpan? retentionCheckInterval = null,
        CompactionConfig? compactionConfig = null,
        TimeSpan? compactionCheckInterval = null,
        bool persistTopicsToFile = true,
        ILogger<LogManager>? logger = null,
        StorageBackend? storageBackend = null)
    {
        _dataDirectory = dataDirectory;
        DataDirectory = dataDirectory;
        _segmentFactory = segmentFactory ?? (storageBackend.HasValue
            ? LogSegmentFactories.Create(storageBackend.Value)
            : throw new ArgumentNullException(nameof(segmentFactory),
                "Either segmentFactory or storageBackend must be provided. " +
                "Use FileLogSegmentFactory from Kuestenlogik.Surgewave.Storage.FileSystem or MemoryLogSegmentFactory from Kuestenlogik.Surgewave.Storage.Memory."));
        _logger = logger;
        _retentionPolicy = retentionPolicy ?? RetentionPolicy.Default;
        _retentionCheckInterval = retentionCheckInterval ?? TimeSpan.FromMinutes(5);
        _compactionConfig = compactionConfig ?? CompactionConfig.Default;
        _compactionCheckInterval = compactionCheckInterval ?? TimeSpan.FromMinutes(15);
        _compactor = new LogCompactor(_compactionConfig);

        // Only create directories for persistent storage
        if (_segmentFactory.IsPersistent)
        {
            Directory.CreateDirectory(dataDirectory);
        }

        // Initialize metadata persistence and load existing topics
        _metadataPersistence = new TopicsMetadataPersistence(dataDirectory, _segmentFactory, persistTopicsToFile);
        _metadataPersistence.Load(_topics, _topicIdToName, _logs);

        // Auto-detect write workers: 2x processor count with minimum of 8 for good parallelism
        var workerCount = writeWorkers > 0 ? writeWorkers : Math.Max(8, Environment.ProcessorCount * 2);
        _writePipeline = new WriteChannelPipeline(workerCount, writeChannelCapacity, writeBatchSize, GetOrCreateLog, _shutdownCts);

        // Start retention cleaner if retention is enabled
        if (_retentionPolicy.RetentionHours > 0 || _retentionPolicy.RetentionBytes > 0)
        {
            _retentionTask = Task.Run(() => RetentionWorkerAsync(_shutdownCts.Token));
        }

        // Start compaction worker (always runs, but only compacts topics with compact policy)
        _compactionTask = Task.Run(() => CompactionWorkerAsync(_shutdownCts.Token));
    }

    /// <summary>
    /// Create a new topic with the specified number of partitions
    /// </summary>
    public async Task<TopicMetadata> CreateTopicAsync(
        string topicName,
        int partitionCount,
        short replicationFactor = 1,
        Dictionary<string, string>? config = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_topics.ContainsKey(topicName))
        {
            throw new InvalidOperationException($"Topic '{topicName}' already exists");
        }

        var topicId = Guid.NewGuid();
        var metadata = new TopicMetadata
        {
            Name = topicName,
            TopicId = topicId,
            PartitionCount = partitionCount,
            ReplicationFactor = replicationFactor,
            Config = config ?? new Dictionary<string, string>(),
            CreatedAt = DateTime.UtcNow
        };

        _topics[topicName] = metadata;
        _topicIdToName[topicId] = topicName;

        // Create partition logs
        for (int i = 0; i < partitionCount; i++)
        {
            var topicPartition = new TopicPartition { Topic = topicName, Partition = i };
            IPartitionLog log;
            if (metadata.CleanupPolicy == CleanupPolicy.Ephemeral)
            {
                var bufferBytes = Configuration.ConfigParser.GetEphemeralBufferBytes(metadata.Config);
                log = new EphemeralPartitionLog(topicPartition, bufferBytes);
            }
            else
            {
                var segmentBytes = GetSegmentBytesFromConfig(metadata.Config);
                log = new PartitionLog(_dataDirectory, topicPartition, _segmentFactory, segmentBytes);
            }
            _logs[topicPartition] = log;
        }

        _metadataPersistence.Save(_topics);

        await FireTopicCreatedAsync(metadata, cancellationToken).ConfigureAwait(false);

        return metadata;
    }

    /// <summary>
    /// Get or create a partition log
    /// </summary>
    public IPartitionLog GetOrCreateLog(TopicPartition topicPartition)
    {
        // Fast path: log already exists
        if (_logs.TryGetValue(topicPartition, out var existingLog))
        {
            return existingLog;
        }

        // Slow path: need to create log with per-partition lock to avoid duplicate creation
        var lockObj = _logCreationLocks.GetOrAdd(topicPartition, _ => new object());
        lock (lockObj)
        {
            // Double-check after acquiring lock
            if (_logs.TryGetValue(topicPartition, out existingLog))
            {
                return existingLog;
            }

            IPartitionLog log;
            if (_topics.TryGetValue(topicPartition.Topic, out var metadata) &&
                metadata.CleanupPolicy == CleanupPolicy.Ephemeral)
            {
                var bufferBytes = Configuration.ConfigParser.GetEphemeralBufferBytes(metadata.Config);
                log = new EphemeralPartitionLog(topicPartition, bufferBytes);
            }
            else
            {
                var segmentBytes = ILogSegment.DefaultMaxSegmentSize;
                if (metadata != null)
                {
                    segmentBytes = GetSegmentBytesFromConfig(metadata.Config);
                }
                log = new PartitionLog(_dataDirectory, topicPartition, _segmentFactory, segmentBytes);
            }
            _logs[topicPartition] = log;
            return log;
        }
    }

    /// <summary>
    /// Get a partition log if it exists
    /// </summary>
    public IPartitionLog? GetLog(TopicPartition topicPartition)
    {
        _logs.TryGetValue(topicPartition, out var log);
        return log;
    }

    /// <summary>
    /// Append raw Kafka RecordBatch bytes to a specific partition (via channel pipeline)
    /// </summary>
    public async ValueTask<long> AppendBatchAsync(
        TopicPartition topicPartition,
        byte[] recordBatch,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var request = WriteRequest.Create(topicPartition, recordBatch, cancellationToken);

        await _writePipeline.Writer.WriteAsync(request, cancellationToken);

        try
        {
            return await request.CompletionSource.ValueTask;
        }
        finally
        {
            request.ReturnToPool();
        }
    }

    /// <summary>
    /// Append raw Kafka RecordBatch using ReadOnlyMemory for zero-copy scenarios.
    /// Optimized to avoid allocation when memory is backed by an array.
    /// </summary>
    public ValueTask<long> AppendBatchAsync(
        TopicPartition topicPartition,
        ReadOnlyMemory<byte> recordBatch,
        CancellationToken cancellationToken = default)
    {
        // Try to get underlying array to avoid allocation
        if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(recordBatch, out var segment))
        {
            if (segment.Offset == 0 && segment.Count == segment.Array!.Length)
            {
                // Memory is backed by exact-fit array - use directly
                return AppendBatchAsync(topicPartition, segment.Array, cancellationToken);
            }
            // Array-backed but sliced - use slice overload
            return AppendBatchSliceAsync(topicPartition, segment.Array!, segment.Offset, segment.Count, cancellationToken);
        }

        // Not array-backed - must copy (rare case for native memory)
        return AppendBatchAsync(topicPartition, recordBatch.ToArray(), cancellationToken);
    }

    /// <summary>
    /// Append a slice of a pooled byte array. Zero-copy - no allocation.
    /// IMPORTANT: The array must remain valid until the returned task completes.
    /// </summary>
    private async ValueTask<long> AppendBatchSliceAsync(
        TopicPartition topicPartition,
        byte[] buffer,
        int offset,
        int length,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Zero-copy: pass slice directly without copying
        var request = WriteRequest.Create(topicPartition, buffer, offset, length, cancellationToken);
        await _writePipeline.Writer.WriteAsync(request, cancellationToken);

        try
        {
            return await request.CompletionSource.ValueTask;
        }
        finally
        {
            request.ReturnToPool();
        }
    }

    /// <summary>
    /// Append directly to partition log, bypassing the channel pipeline.
    /// Use for maximum throughput when caller manages concurrency.
    /// </summary>
    public async ValueTask<long> AppendBatchDirectAsync(
        TopicPartition topicPartition,
        byte[] recordBatch,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var log = GetOrCreateLog(topicPartition);
        return await log.AppendBatchAsync(recordBatch, cancellationToken);
    }

    /// <summary>
    /// Read raw RecordBatch bytes from a partition
    /// </summary>
    public async ValueTask<List<byte[]>> ReadBatchesAsync(
        TopicPartition topicPartition,
        long startOffset,
        int maxBytes = 1024 * 1024,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var log = GetLog(topicPartition);
        if (log == null)
        {
            return [];
        }

        return await log.ReadBatchesAsync(startOffset, maxBytes, cancellationToken);
    }

    /// <summary>
    /// Read raw RecordBatch bytes from a partition as contiguous memory.
    /// More efficient than ReadBatchesAsync for network streaming.
    /// </summary>
    public async ValueTask<(ReadOnlyMemory<byte> Data, List<int> BatchOffsets)> ReadBatchesContiguousAsync(
        TopicPartition topicPartition,
        long startOffset,
        int maxBytes = 1024 * 1024,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var log = GetLog(topicPartition);
        if (log == null)
        {
            return (ReadOnlyMemory<byte>.Empty, []);
        }

        return await log.ReadBatchesContiguousAsync(startOffset, maxBytes, cancellationToken);
    }

    /// <summary>
    /// Get topic metadata by name
    /// </summary>
    public TopicMetadata? GetTopicMetadata(string topicName)
    {
        _topics.TryGetValue(topicName, out var metadata);
        return metadata;
    }

    /// <summary>
    /// Get topic metadata by TopicId (UUID)
    /// </summary>
    public TopicMetadata? GetTopicMetadataById(Guid topicId)
    {
        if (_topicIdToName.TryGetValue(topicId, out var topicName))
        {
            return GetTopicMetadata(topicName);
        }
        return null;
    }

    /// <summary>
    /// Resolve TopicId to topic name. Returns null if not found.
    /// </summary>
    public string? ResolveTopicId(Guid topicId)
    {
        _topicIdToName.TryGetValue(topicId, out var topicName);
        return topicName;
    }

    /// <summary>
    /// Get TopicId for a topic name. Returns Guid.Empty if not found.
    /// </summary>
    public Guid GetTopicId(string topicName)
    {
        if (_topics.TryGetValue(topicName, out var metadata))
        {
            return metadata.TopicId;
        }
        return Guid.Empty;
    }

    /// <summary>
    /// List all topics
    /// </summary>
    public IEnumerable<TopicMetadata> ListTopics()
    {
        return _topics.Values;
    }

    /// <summary>
    /// Returns the partitions of a topic — from the topic metadata when present,
    /// otherwise from the materialized partition logs (topics auto-created via
    /// <see cref="GetOrCreateLog"/> have logs but no metadata entry). Empty when
    /// the topic is unknown.
    /// </summary>
    public IReadOnlyList<int> GetTopicPartitions(string topicName)
    {
        if (_topics.TryGetValue(topicName, out var metadata))
        {
            return [.. Enumerable.Range(0, metadata.PartitionCount)];
        }

        return [.. _logs.Keys
            .Where(tp => tp.Topic == topicName)
            .Select(tp => tp.Partition)
            .OrderBy(p => p)];
    }

    /// <summary>
    /// Update topic configuration.
    /// Supports both Kafka-compatible keys (segment.bytes) and human-readable keys (segment).
    /// </summary>
    /// <param name="topicName">Name of the topic to update</param>
    /// <param name="configUpdates">Configuration key-value pairs to update/add</param>
    /// <param name="deleteKeys">Configuration keys to remove (optional)</param>
    /// <returns>True if topic was found and updated, false if topic doesn't exist</returns>
    public bool UpdateTopicConfig(string topicName, Dictionary<string, string> configUpdates, IEnumerable<string>? deleteKeys = null)
    {
        if (!_topics.TryGetValue(topicName, out var metadata))
        {
            return false;
        }

        // Snapshot the previous config so hooks see the delta.
        var previous = new Dictionary<string, string>(metadata.Config);

        // Normalize the incoming config (converts human-readable to Kafka format)
        var normalizedUpdates = ConfigParser.NormalizeConfig(configUpdates);

        // Apply updates
        foreach (var (key, value) in normalizedUpdates)
        {
            metadata.Config[key] = value;
        }

        // Apply deletions
        if (deleteKeys != null)
        {
            foreach (var key in deleteKeys)
            {
                metadata.Config.Remove(key);
            }
        }

        _metadataPersistence.Save(_topics);

        FireTopicConfigChanged(metadata, previous);

        return true;
    }

    /// <summary>
    /// Get topic configuration.
    /// </summary>
    /// <param name="topicName">Name of the topic</param>
    /// <returns>Configuration dictionary or null if topic doesn't exist</returns>
    public Dictionary<string, string>? GetTopicConfig(string topicName)
    {
        if (!_topics.TryGetValue(topicName, out var metadata))
        {
            return null;
        }

        return new Dictionary<string, string>(metadata.Config);
    }

    /// <summary>
    /// Delete a topic
    /// </summary>
    public async Task DeleteTopicAsync(string topicName, CancellationToken cancellationToken = default)
    {
        if (!_topics.TryRemove(topicName, out var metadata))
        {
            throw new InvalidOperationException($"Topic '{topicName}' does not exist");
        }

        // Remove from TopicId mapping
        _topicIdToName.TryRemove(metadata.TopicId, out _);

        // Remove all partition logs
        var partitionsToRemove = _logs.Keys.Where(tp => tp.Topic == topicName).ToList();
        foreach (var tp in partitionsToRemove)
        {
            if (_logs.TryRemove(tp, out var log))
            {
                log.Dispose();
            }
        }

        // Delete directory
        var topicDirectory = Path.Combine(_dataDirectory, topicName);
        if (Directory.Exists(topicDirectory))
        {
            Directory.Delete(topicDirectory, recursive: true);
        }

        _metadataPersistence.Save(_topics);

        await FireTopicDeletedAsync(metadata, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Delete a single partition's log data.
    /// </summary>
    public Task DeleteLogAsync(TopicPartition tp, CancellationToken cancellationToken = default)
    {
        if (_logs.TryRemove(tp, out var log))
        {
            log.Dispose();
        }

        // Delete partition directory
        var partitionDirectory = Path.Combine(_dataDirectory, tp.Topic, $"partition-{tp.Partition}");
        if (Directory.Exists(partitionDirectory))
        {
            Directory.Delete(partitionDirectory, recursive: true);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Add partitions to an existing topic.
    /// Returns true if partitions were added, false if the topic doesn't exist.
    /// </summary>
    /// <param name="topicName">The topic to add partitions to</param>
    /// <param name="totalPartitions">The new total number of partitions (must be greater than current count)</param>
    /// <returns>True if successful, false if topic doesn't exist or partition count is not increasing</returns>
    public bool AddPartitions(string topicName, int totalPartitions)
    {
        if (!_topics.TryGetValue(topicName, out var metadata))
        {
            return false;
        }

        if (totalPartitions <= metadata.PartitionCount)
        {
            return false; // Can only increase partition count
        }

        // Create new partition logs
        for (int i = metadata.PartitionCount; i < totalPartitions; i++)
        {
            var topicPartition = new TopicPartition { Topic = topicName, Partition = i };
            IPartitionLog log;
            if (metadata.CleanupPolicy == CleanupPolicy.Ephemeral)
            {
                var bufferBytes = Configuration.ConfigParser.GetEphemeralBufferBytes(metadata.Config);
                log = new EphemeralPartitionLog(topicPartition, bufferBytes);
            }
            else
            {
                var segmentBytes = GetSegmentBytesFromConfig(metadata.Config);
                log = new PartitionLog(_dataDirectory, topicPartition, _segmentFactory, segmentBytes);
            }
            _logs[topicPartition] = log;
        }

        // Update metadata
        metadata.PartitionCount = totalPartitions;

        _metadataPersistence.Save(_topics);

        return true;
    }

    /// <summary>
    /// Delete records in a partition up to the specified offset.
    /// Returns the new log start offset after deletion.
    /// </summary>
    /// <param name="topicPartition">The topic-partition to delete records from</param>
    /// <param name="beforeOffset">Delete all records with offset less than this value. Use -1 to delete all.</param>
    /// <returns>The new log start offset, or null if partition doesn't exist</returns>
    public long? DeleteRecords(TopicPartition topicPartition, long beforeOffset)
    {
        var log = GetLog(topicPartition);
        if (log is not PartitionLog persistentLog)
        {
            return null;
        }

        return persistentLog.DeleteRecordsToOffset(beforeOffset);
    }

    /// <summary>
    /// Background worker that periodically applies retention policy to all partitions
    /// </summary>
    private async Task RetentionWorkerAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_retentionCheckInterval, cancellationToken);

                var totalDeleted = ApplyRetentionPolicy();
                // Note: In production, we'd log this if totalDeleted > 0
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch
            {
                // Log error but continue running
            }
        }
    }

    /// <summary>
    /// Apply retention policy to all partition logs
    /// </summary>
    /// <returns>Total number of segments deleted across all partitions</returns>
    public int ApplyRetentionPolicy()
    {
        var totalDeleted = 0;
        foreach (var log in _logs.Values)
        {
            if (log is not PartitionLog partitionLog) continue;
            try
            {
                totalDeleted += partitionLog.ApplyRetentionPolicy(_retentionPolicy);
            }
            catch
            {
                // Log error but continue with other partitions
            }
        }
        return totalDeleted;
    }

    /// <summary>
    /// Background worker that periodically compacts logs for topics with compact cleanup policy
    /// </summary>
    private async Task CompactionWorkerAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_compactionCheckInterval, cancellationToken);

                var result = await ApplyCompactionAsync(cancellationToken);
                // Note: In production, we'd log this if result.RecordsRemoved > 0
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch
            {
                // Log error but continue running
            }
        }
    }

    /// <summary>
    /// Apply log compaction to all partitions that have compact cleanup policy
    /// </summary>
    /// <returns>Aggregated compaction result</returns>
    public async Task<CompactionResult> ApplyCompactionAsync(CancellationToken cancellationToken = default)
    {
        var startTime = DateTimeOffset.UtcNow;
        var stopwatch = Stopwatch.StartNew();
        var totalRecordsRemoved = 0L;
        var totalBytesRemoved = 0L;
        var totalSegmentsCompacted = 0;
        var partitionsSkipped = 0;
        var partitionsCompacted = 0;
        var bytesCompactedThisRun = 0L;
        var wasLimitedByMaxBytes = false;

        // Get topics that have compact cleanup policy
        var compactableTopics = _topics.Values
            .Where(t => t.CleanupPolicy.HasFlag(CleanupPolicy.Compact))
            .Select(t => t.Name)
            .ToHashSet();

        if (compactableTopics.Count == 0)
        {
            return new CompactionResult(0, 0, 0)
            {
                StartTime = startTime,
                Duration = stopwatch.Elapsed
            };
        }

        // Compact logs for those topics
        foreach (var (topicPartition, log) in _logs)
        {
            if (!compactableTopics.Contains(topicPartition.Topic))
            {
                continue;
            }

            // Compaction only applies to persistent partition logs
            if (log is not PartitionLog persistentLog) continue;

            // Check MaxCompactionBytes limit
            if (_compactionConfig.MaxCompactionBytes > 0 && bytesCompactedThisRun >= _compactionConfig.MaxCompactionBytes)
            {
                // Hit byte limit for this compaction run, stop processing
                wasLimitedByMaxBytes = true;
                _logger?.LogDebug("Compaction stopped: reached MaxCompactionBytes limit ({BytesCompacted}/{MaxBytes})",
                    bytesCompactedThisRun, _compactionConfig.MaxCompactionBytes);
                break;
            }

            // Check dirty ratio threshold - skip if partition is not dirty enough
            if (persistentLog.DirtyRatio < _compactionConfig.MinCleanableDirtyRatio)
            {
                partitionsSkipped++;
                continue;
            }

            try
            {
                _logger?.LogDebug("Compacting partition {TopicPartition} (dirty ratio: {DirtyRatio:P1})",
                    topicPartition, persistentLog.DirtyRatio);

                var result = await _compactor.CompactAsync(persistentLog, cancellationToken);
                totalRecordsRemoved += result.RecordsRemoved;
                totalBytesRemoved += result.BytesRemoved;
                totalSegmentsCompacted += result.SegmentsCompacted;
                bytesCompactedThisRun += result.BytesRemoved;

                // Reset dirty tracking after successful compaction
                if (result.RecordsRemoved > 0 || result.BytesRemoved > 0)
                {
                    persistentLog.ResetDirtyTracking(result.BytesRemoved);
                    partitionsCompacted++;

                    _logger?.LogDebug("Compacted partition {TopicPartition}: removed {Records} records, {Bytes} bytes",
                        topicPartition, result.RecordsRemoved, result.BytesRemoved);
                }

                _compactionStats[topicPartition] = new PartitionCompactionStats(
                    DateTimeOffset.UtcNow, result.RecordsRemoved, result.BytesRemoved);
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to compact partition {TopicPartition}", topicPartition);
            }
        }

        stopwatch.Stop();

        if (partitionsCompacted > 0)
        {
            _logger?.LogInformation(
                "Compaction completed: {PartitionsCompacted} partitions, {RecordsRemoved} records, {BytesRemoved} bytes removed in {Duration:F2}s (skipped {PartitionsSkipped} partitions below threshold)",
                partitionsCompacted, totalRecordsRemoved, totalBytesRemoved, stopwatch.Elapsed.TotalSeconds, partitionsSkipped);
        }

        return new CompactionResult(totalRecordsRemoved, totalBytesRemoved, totalSegmentsCompacted)
        {
            PartitionsCompacted = partitionsCompacted,
            PartitionsSkipped = partitionsSkipped,
            WasLimitedByMaxBytes = wasLimitedByMaxBytes,
            StartTime = startTime,
            Duration = stopwatch.Elapsed
        };
    }

    /// <summary>
    /// Last compaction stats for a partition, or null when it has not been
    /// compacted during this broker's lifetime. (Stats are in-memory only.)
    /// </summary>
    public PartitionCompactionStats? GetCompactionStats(TopicPartition topicPartition)
        => _compactionStats.TryGetValue(topicPartition, out var stats) ? stats : null;

    /// <summary>
    /// Current dirty ratio of a partition (uncompacted fraction), or null when
    /// the partition does not exist or is not a persistent log.
    /// </summary>
    public double? GetDirtyRatio(TopicPartition topicPartition)
        => (GetLog(topicPartition) as PartitionLog)?.DirtyRatio;

    // ───────────────────────────────────────────────────────────────────
    // Topic lifecycle hooks (KIP-1010)
    // ───────────────────────────────────────────────────────────────────

    /// <summary>
    /// Registers a topic-lifecycle hook. Hooks are invoked in registration order
    /// after each successful topic create / config-change / delete. A hook that
    /// throws is logged but never aborts the operation, since the underlying
    /// state has already changed.
    /// </summary>
    public void RegisterTopicHook(ITopicLifecycleHook hook)
    {
        ArgumentNullException.ThrowIfNull(hook);
        _topicHooks.Add(hook);
    }

    private async Task FireTopicCreatedAsync(TopicMetadata metadata, CancellationToken cancellationToken)
    {
        if (_topicHooks.IsEmpty) return;
        var ctx = BuildContext(metadata, previousConfig: null);
        foreach (var hook in _topicHooks)
        {
            try { await hook.OnTopicCreatedAsync(ctx, cancellationToken).ConfigureAwait(false); }
            catch (Exception ex) { _logger?.LogError(ex, "Topic-created hook {Hook} threw for {Topic}", hook.GetType().FullName, LogSanitizer.Sanitize(metadata.Name)); }
        }
    }

    private void FireTopicConfigChanged(TopicMetadata metadata, IReadOnlyDictionary<string, string> previousConfig)
    {
        if (_topicHooks.IsEmpty) return;
        var ctx = BuildContext(metadata, previousConfig);
        foreach (var hook in _topicHooks)
        {
            try { hook.OnTopicConfigChangedAsync(ctx, CancellationToken.None).GetAwaiter().GetResult(); }
            catch (Exception ex) { _logger?.LogError(ex, "Topic-config-changed hook {Hook} threw for {Topic}", hook.GetType().FullName, metadata.Name); }
        }
    }

    private async Task FireTopicDeletedAsync(TopicMetadata metadata, CancellationToken cancellationToken)
    {
        if (_topicHooks.IsEmpty) return;
        var ctx = BuildContext(metadata, previousConfig: null);
        foreach (var hook in _topicHooks)
        {
            try { await hook.OnTopicDeletedAsync(ctx, cancellationToken).ConfigureAwait(false); }
            catch (Exception ex) { _logger?.LogError(ex, "Topic-deleted hook {Hook} threw for {Topic}", hook.GetType().FullName, metadata.Name); }
        }
    }

    private static TopicLifecycleContext BuildContext(TopicMetadata metadata, IReadOnlyDictionary<string, string>? previousConfig) =>
        new(metadata.Name,
            metadata.TopicId,
            metadata.PartitionCount,
            metadata.ReplicationFactor,
            new Dictionary<string, string>(metadata.Config),
            previousConfig);

    public void Dispose()
    {
        if (_disposed) return;

        // Signal shutdown
        _shutdownCts.Cancel();

        // Dispose write pipeline (waits for workers)
        _writePipeline.Dispose();

        // Wait for retention task if running
        if (_retentionTask != null)
        {
            try
            {
                _retentionTask.Wait(TimeSpan.FromSeconds(5));
            }
            catch
            {
                // Ignore timeout
            }
        }

        // Wait for compaction task if running
        if (_compactionTask != null)
        {
            try
            {
                _compactionTask.Wait(TimeSpan.FromSeconds(5));
            }
            catch
            {
                // Ignore timeout
            }
        }

        // Dispose partition logs
        foreach (var log in _logs.Values)
        {
            log.Dispose();
        }

        _logs.Clear();
        _topics.Clear();
        _shutdownCts.Dispose();
        _disposed = true;
    }

    /// <summary>
    /// Get segment bytes from topic config, or default if not specified.
    /// </summary>
    private static long GetSegmentBytesFromConfig(Dictionary<string, string> config)
    {
        return ConfigParser.GetSegmentBytes(config, ILogSegment.DefaultMaxSegmentSize);
    }
}
