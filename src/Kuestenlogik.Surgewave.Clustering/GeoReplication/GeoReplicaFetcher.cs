using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Exceptions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.GeoReplication;

/// <summary>
/// Fetches data from remote clusters and writes to local mirror topics with offset preservation.
/// Analogous to ReplicaFetcher but for cross-cluster geo-replication.
/// </summary>
public sealed partial class GeoReplicaFetcher : IAsyncDisposable
{
    private readonly ClusterLink _link;
    private readonly LogManager _logManager;
    private readonly IClusteringMetrics? _metrics;
    private readonly ILogger _logger;

    private readonly ConcurrentDictionary<TopicPartition, long> _fetchPositions = new();
    private readonly ConcurrentDictionary<TopicPartition, long> _replicationLag = new();

    private CancellationTokenSource? _cts;
    private Task[]? _fetchTasks;

    public TimeSpan FetchInterval { get; set; }
    public int MaxFetchBytes { get; set; }
    public int FetcherThreads { get; set; }
    public string LinkId => _link.LinkId;

    public GeoReplicaFetcher(
        ClusterLink link,
        LogManager logManager,
        IClusteringMetrics? metrics,
        ILogger logger)
    {
        _link = link;
        _logManager = logManager;
        _metrics = metrics;
        _logger = logger;

        FetchInterval = TimeSpan.FromMilliseconds(link.Config.FetchIntervalMs);
        MaxFetchBytes = link.Config.FetchMaxBytes;
        FetcherThreads = link.Config.FetcherThreads;
    }

    /// <summary>
    /// Start fetching for the given partitions.
    /// </summary>
    public void AddPartitions(IEnumerable<TopicPartition> partitions)
    {
        foreach (var tp in partitions)
        {
            var log = _logManager.GetLog(tp);
            var startOffset = log?.NextOffset ?? 0;
            _fetchPositions[tp] = startOffset;
            LogAddedPartition(tp.Topic, tp.Partition, startOffset);
        }
    }

    /// <summary>
    /// Remove partitions from fetching (e.g., on promote/failover).
    /// </summary>
    public void RemovePartitions(IEnumerable<TopicPartition> partitions)
    {
        foreach (var tp in partitions)
        {
            _fetchPositions.TryRemove(tp, out _);
            _replicationLag.TryRemove(tp, out _);
        }
    }

    /// <summary>
    /// Remove all partitions for a given topic.
    /// </summary>
    public void RemoveTopic(string topic)
    {
        var toRemove = _fetchPositions.Keys.Where(tp => tp.Topic == topic).ToList();
        RemovePartitions(toRemove);
    }

    /// <summary>
    /// Start the fetcher threads.
    /// </summary>
    public Task StartAsync(CancellationToken ct)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _fetchTasks = new Task[FetcherThreads];

        for (int i = 0; i < FetcherThreads; i++)
        {
            var threadId = i;
            _fetchTasks[i] = Task.Run(() => FetchLoopAsync(threadId, _cts.Token), _cts.Token);
        }

        LogFetcherStarted(_link.LinkId, FetcherThreads);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Get current replication lag for a partition.
    /// </summary>
    public long GetLag(TopicPartition tp) =>
        _replicationLag.TryGetValue(tp, out var lag) ? lag : 0;

    /// <summary>
    /// Get total lag across all partitions.
    /// </summary>
    public long GetTotalLag() => _replicationLag.Values.Sum();

    private async Task FetchLoopAsync(int threadId, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(FetchInterval, ct);

                // Get partitions assigned to this thread (round-robin by thread ID)
                var allPartitions = _fetchPositions.Keys.ToList();
                var myPartitions = allPartitions
                    .Where((_, idx) => idx % FetcherThreads == threadId)
                    .ToList();

                foreach (var tp in myPartitions)
                {
                    if (ct.IsCancellationRequested) break;
                    if (!_fetchPositions.TryGetValue(tp, out var fetchOffset)) continue;

                    try
                    {
                        await FetchPartitionAsync(tp, fetchOffset, ct);
                    }
                    catch (Exception ex) when (!ct.IsCancellationRequested)
                    {
                        LogFetchPartitionError(tp.Topic, tp.Partition, ex);
                    }
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogFetchLoopError(threadId, ex);
                await Task.Delay(TimeSpan.FromSeconds(1), ct);
            }
        }
    }

    private async Task FetchPartitionAsync(TopicPartition tp, long fetchOffset, CancellationToken ct)
    {
        var result = await _link.FetchAsync(tp.Topic, tp.Partition, fetchOffset, MaxFetchBytes, ct);

        if (result.ErrorCode != 0)
        {
            LogRemoteFetchError(tp.Topic, tp.Partition, result.ErrorCode);
            return;
        }

        if (result.RecordBatch.Length < 12)
        {
            // Update lag even when no data
            _replicationLag[tp] = Math.Max(0, result.HighWatermark - fetchOffset);
            return;
        }

        // A remote Kafka fetch returns a records section of N complete batches behind one length
        // prefix (plus possibly a truncated trailing batch at maxBytes). Split it and append each
        // batch offset-preserving at its OWN remote base offset, validating its CRC — so each batch
        // keeps its own checksum (#92) and _fetchPositions advances past every committed batch (#93).
        var data = result.RecordBatch;
        var log = _logManager.GetOrCreateLog(tp);
        var cursor = 0;
        long firstBaseOffset = -1;
        try
        {
            while (RecordBatchValidator.TryReadBatchBoundary(data, cursor, out var total, out var batchBaseOffset, out _))
            {
                if (firstBaseOffset < 0)
                    firstBaseOffset = batchBaseOffset;

                // Skip batches already stored (idempotent re-fetch); pin the rest at the remote's
                // own offset — a leading gap is a valid sparse gap when remote retention advanced.
                if (batchBaseOffset >= log.NextOffset)
                    await log.AppendBatchAtOffsetAsync(data, cursor, total, batchBaseOffset, BatchCrcMode.Validate, ct);

                cursor += total;
            }
        }
        catch (DataCorruptionException ex)
        {
            // Keep the good prefix; refuse the corrupt batch and everything after it. _fetchPositions
            // advances only to the committed LEO below, so the next cycle re-fetches from it.
            LogCorruptRemoteBatch(tp.Topic, tp.Partition, ex);
        }

        // Authoritative LEO past every committed batch (fixes the old baseOffset + first-batch-count).
        var newOffset = log.NextOffset;
        _fetchPositions[tp] = newOffset;

        // Update lag
        var lag = Math.Max(0, result.HighWatermark - newOffset);
        _replicationLag[tp] = lag;

        // Update link timestamp
        _link.LastFetchTimestamp = DateTimeOffset.UtcNow;

        // Record metrics
        _metrics?.RecordReplicationBytes(tp.Topic, tp.Partition, data.Length);
        _metrics?.RecordReplicationLag(tp.Topic, tp.Partition, lag);

        LogFetchedData(tp.Topic, tp.Partition, firstBaseOffset < 0 ? fetchOffset : firstBaseOffset,
            (int)(newOffset - fetchOffset), data.Length);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Rejected corrupt remote batch for {Topic}-{Partition}; kept the good prefix, will re-fetch")]
    private partial void LogCorruptRemoteBatch(string topic, int partition, Exception ex);

    public async ValueTask DisposeAsync()
    {
        _cts?.Cancel();

        if (_fetchTasks != null)
        {
            try
            {
                await Task.WhenAll(_fetchTasks);
            }
            catch (OperationCanceledException) { }
        }

        _cts?.Dispose();
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Geo-replication fetcher started for link {LinkId} with {ThreadCount} threads")]
    private partial void LogFetcherStarted(string linkId, int threadCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Added partition {Topic}-{Partition} to geo-replication fetcher at offset {Offset}")]
    private partial void LogAddedPartition(string topic, int partition, long offset);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Geo-replicated {Topic}-{Partition} offset={BaseOffset} records={RecordCount} bytes={Size}")]
    private partial void LogFetchedData(string topic, int partition, long baseOffset, int recordCount, int size);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Remote fetch error for {Topic}-{Partition}: errorCode={ErrorCode}")]
    private partial void LogRemoteFetchError(string topic, int partition, short errorCode);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error fetching partition {Topic}-{Partition}")]
    private partial void LogFetchPartitionError(string topic, int partition, Exception ex);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in geo-replication fetch loop (thread {ThreadId})")]
    private partial void LogFetchLoopError(int threadId, Exception ex);
}
