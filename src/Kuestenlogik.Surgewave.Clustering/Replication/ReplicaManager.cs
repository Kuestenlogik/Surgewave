using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Manages replicas for this broker, coordinating leader/follower state transitions.
/// </summary>
public sealed partial class ReplicaManager : IAsyncDisposable
{
    private readonly ILogger<ReplicaManager> _logger;
    private readonly ClusterState _clusterState;
    private readonly LogManager _logManager;
    private readonly ClusteringConfig _config;
    private readonly IClusteringMetrics? _metrics;

    private readonly ConcurrentDictionary<TopicPartition, PartitionReplica> _localReplicas = new();
    private readonly ConcurrentDictionary<TopicPartition, PendingProduceAcks> _pendingAcks = new();

    // Track remote replica LEOs for high watermark calculation
    // Key: TopicPartition, Value: Dictionary of BrokerId -> (LEO, LastUpdateTime)
    private readonly ConcurrentDictionary<TopicPartition, ConcurrentDictionary<int, (long Leo, DateTimeOffset LastUpdate)>> _followerLeos = new();

    private ReplicaFetcher? _replicaFetcher;
    private CancellationTokenSource? _cts;
    private Task? _isrCheckTask;

    /// <summary>
    /// Time after which a lagging replica is removed from ISR.
    /// </summary>
    public TimeSpan ReplicaLagTimeMax { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Maximum offset lag before removing from ISR.
    /// </summary>
    public long ReplicaLagMaxMessages { get; set; } = 10_000;

    /// <summary>
    /// Grace period before removing a lagging replica from ISR.
    /// Prevents thrashing on temporary network issues.
    /// </summary>
    public TimeSpan IsrShrinkGracePeriod { get; set; } = TimeSpan.FromSeconds(10);

    public ReplicaManager(
        ILogger<ReplicaManager> logger,
        ClusterState clusterState,
        LogManager logManager,
        ClusteringConfig config,
        Kuestenlogik.Surgewave.Transport.IPeerTransport peerTransport,
        IClusteringMetrics? metrics = null)
    {
        _logger = logger;
        _clusterState = clusterState;
        _logManager = logManager;
        _config = config;
        _peerTransport = peerTransport;
        _metrics = metrics;
    }

    private readonly Kuestenlogik.Surgewave.Transport.IPeerTransport _peerTransport;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // Start replica fetcher for follower partitions
        _replicaFetcher = new ReplicaFetcher(
            _logger,
            _clusterState,
            _logManager,
            this,
            _config,
            _peerTransport);
        await _replicaFetcher.StartAsync(_cts.Token);

        // Start ISR check background task
        _isrCheckTask = Task.Run(() => IsrCheckLoopAsync(_cts.Token), _cts.Token);

        LogReplicaManagerStarted(_config.BrokerId);
    }

    public async ValueTask DisposeAsync()
    {
        _cts?.Cancel();

        if (_replicaFetcher != null)
        {
            await _replicaFetcher.DisposeAsync();
        }

        if (_isrCheckTask != null)
        {
            try { await _isrCheckTask; } catch (OperationCanceledException) { }
        }

        _cts?.Dispose();
    }

    /// <summary>
    /// Called when this broker becomes leader for a partition.
    /// </summary>
    public async Task BecomeLeaderAsync(TopicPartition tp, int leaderEpoch, CancellationToken ct)
    {
        var replica = GetOrCreateLocalReplica(tp);

        lock (replica)
        {
            replica.State = ReplicaState.Leader;
            replica.LeaderEpoch = leaderEpoch;
            replica.IsInSync = true;
        }

        // Update cluster state
        _clusterState.UpdatePartitionState(tp, state =>
        {
            state.LeaderBrokerId = _config.BrokerId;
            state.LeaderEpoch = leaderEpoch;
            if (!state.Isr.Contains(_config.BrokerId))
            {
                state.Isr.Add(_config.BrokerId);
            }
        });

        // Stop fetching for this partition
        _replicaFetcher?.StopFetching(tp);

        // Clear stale follower LEO tracking from previous leadership
        _followerLeos.TryRemove(tp, out _);

        // Recover high watermark from log
        var log = _logManager.GetOrCreateLog(tp);
        replica.LogEndOffset = log.HighWatermark;
        replica.HighWatermark = log.HighWatermark;

        LogBecameLeader(tp.Topic, tp.Partition, leaderEpoch);
    }

    /// <summary>
    /// Called when this broker becomes follower for a partition.
    /// </summary>
    public async Task BecomeFollowerAsync(TopicPartition tp, int leaderId, int leaderEpoch, CancellationToken ct)
    {
        var replica = GetOrCreateLocalReplica(tp);

        lock (replica)
        {
            replica.State = ReplicaState.Follower;
            replica.LeaderEpoch = leaderEpoch;
            replica.IsInSync = false; // Will become in-sync after catching up
        }

        // Update cluster state
        _clusterState.UpdatePartitionState(tp, state =>
        {
            state.LeaderBrokerId = leaderId;
            state.LeaderEpoch = leaderEpoch;
        });

        // Get current log end offset
        var log = _logManager.GetOrCreateLog(tp);
        replica.LogEndOffset = log.HighWatermark;

        // Start fetching from leader
        _replicaFetcher?.StartFetching(tp, leaderId);

        LogBecameFollower(tp.Topic, tp.Partition, leaderId, leaderEpoch);
    }

    /// <summary>
    /// Check if this broker is the leader for the partition.
    /// </summary>
    public bool IsLeader(TopicPartition tp)
    {
        return _localReplicas.TryGetValue(tp, out var replica) && replica.IsLeader;
    }

    /// <summary>
    /// Get the local replica for a partition.
    /// </summary>
    public PartitionReplica? GetReplica(TopicPartition tp)
    {
        return _localReplicas.TryGetValue(tp, out var replica) ? replica : null;
    }

    /// <summary>
    /// Stop replication for a partition. Called when the broker is being removed as a replica.
    /// </summary>
    public void StopReplica(TopicPartition tp)
    {
        // Stop fetching if we were a follower
        _replicaFetcher?.StopFetching(tp);

        // Remove from local replicas
        _localReplicas.TryRemove(tp, out _);

        // Clear follower LEO tracking if we were leader
        _followerLeos.TryRemove(tp, out _);
    }

    /// <summary>
    /// Append data to the local log (for both leader and follower).
    /// Returns the base offset of the appended batch.
    /// </summary>
    public async Task<long> AppendAsync(TopicPartition tp, byte[] recordBatch, CancellationToken ct)
    {
        var offset = await _logManager.AppendBatchAsync(tp, recordBatch, ct);

        // Update local replica state
        if (_localReplicas.TryGetValue(tp, out var replica))
        {
            replica.LogEndOffset = offset + 1; // LEO is next offset to write
        }

        return offset;
    }

    /// <summary>
    /// Called when a follower reports its fetch position.
    /// Updates ISR and potentially advances the high watermark.
    /// </summary>
    public void UpdateFollowerFetchPosition(TopicPartition tp, int followerId, long fetchOffset)
    {
        if (!IsLeader(tp))
            return;

        var partitionState = _clusterState.GetPartitionState(tp);
        if (partitionState == null)
            return;

        var replica = GetReplica(tp);
        if (replica == null)
            return;

        // Track follower LEO for high watermark calculation
        var followerLeos = _followerLeos.GetOrAdd(tp, _ => new ConcurrentDictionary<int, (long, DateTimeOffset)>());
        var now = DateTimeOffset.UtcNow;
        followerLeos[followerId] = (fetchOffset, now);

        // Check if follower is caught up
        var leaderLeo = replica.LogEndOffset;
        var lag = leaderLeo - fetchOffset;

        // Update ISR based on lag with grace period
        if (lag <= ReplicaLagMaxMessages)
        {
            // Follower is caught up, add to ISR immediately
            if (_clusterState.AddToIsr(tp, followerId))
            {
                _metrics?.RecordReplicaJoinedIsr(tp.Topic, tp.Partition);
            }
        }
        else if (lag > ReplicaLagMaxMessages)
        {
            // Check grace period before removing from ISR
            // Only remove if consistently lagging beyond the grace period
            if (followerLeos.TryGetValue(followerId, out var state))
            {
                var timeSinceLastCaughtUp = now - state.LastUpdate;
                if (timeSinceLastCaughtUp > IsrShrinkGracePeriod && partitionState.Isr.Contains(followerId))
                {
                    if (_clusterState.RemoveFromIsr(tp, followerId))
                    {
                        _metrics?.RecordReplicaLeftIsr(tp.Topic, tp.Partition);
                    }
                    LogFollowerRemovedFromIsr(tp.Topic, tp.Partition, followerId, lag);
                }
            }
        }

        // Update high watermark using ISR minimum LEO
        UpdateHighWatermark(tp);
    }

    /// <summary>
    /// Update the high watermark based on ISR acknowledgments.
    /// High watermark = min(LEO of all ISR replicas)
    /// </summary>
    private void UpdateHighWatermark(TopicPartition tp)
    {
        var partitionState = _clusterState.GetPartitionState(tp);
        if (partitionState == null)
            return;

        var replica = GetReplica(tp);
        if (replica == null)
            return;

        // Calculate high watermark as minimum LEO across all ISR replicas
        var leaderLeo = replica.LogEndOffset;
        var minLeo = leaderLeo;

        // Get follower LEOs for this partition
        if (_followerLeos.TryGetValue(tp, out var followerLeos))
        {
            foreach (var isrBrokerId in partitionState.Isr)
            {
                // Skip leader (already included as leaderLeo)
                if (isrBrokerId == _config.BrokerId)
                    continue;

                // Get follower's LEO if tracked
                if (followerLeos.TryGetValue(isrBrokerId, out var followerState))
                {
                    minLeo = Math.Min(minLeo, followerState.Leo);
                }
                else
                {
                    // Follower hasn't reported yet - use 0 to be conservative
                    // This ensures we don't advance HW until all ISR replicas report
                    minLeo = 0;
                }
            }
        }
        else if (partitionState.Isr.Count > 1)
        {
            // No follower LEOs tracked but ISR has multiple members
            // Don't advance HW until followers report
            minLeo = partitionState.HighWatermark;
        }

        var newHw = minLeo;

        if (newHw > partitionState.HighWatermark)
        {
            partitionState.HighWatermark = newHw;
            replica.HighWatermark = newHw;

            // Complete pending produce requests waiting for this HW
            CompletePendingAcks(tp, newHw);
        }
    }

    /// <summary>
    /// Register a pending produce request waiting for acknowledgment.
    /// </summary>
    public void RegisterPendingAck(TopicPartition tp, long offset, TaskCompletionSource<bool> tcs)
    {
        var pending = _pendingAcks.GetOrAdd(tp, _ => new PendingProduceAcks());
        pending.Add(offset, tcs);
    }

    /// <summary>
    /// Complete pending acks up to the given offset.
    /// </summary>
    private void CompletePendingAcks(TopicPartition tp, long upToOffset)
    {
        if (_pendingAcks.TryGetValue(tp, out var pending))
        {
            pending.Complete(upToOffset);
        }
    }

    /// <summary>
    /// Get high watermark for a partition.
    /// </summary>
    public long GetHighWatermark(TopicPartition tp)
    {
        var partitionState = _clusterState.GetPartitionState(tp);
        return partitionState?.HighWatermark ?? 0;
    }

    private PartitionReplica GetOrCreateLocalReplica(TopicPartition tp)
    {
        return _localReplicas.GetOrAdd(tp, _ => new PartitionReplica
        {
            TopicPartition = tp,
            BrokerId = _config.BrokerId
        });
    }

    private async Task IsrCheckLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(10), ct);

                // Check each leader partition for lagging followers
                foreach (var tp in _clusterState.GetLeaderPartitions(_config.BrokerId))
                {
                    CheckIsrForPartition(tp);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogIsrCheckError(ex);
            }
        }
    }

    private void CheckIsrForPartition(TopicPartition tp)
    {
        var partitionState = _clusterState.GetPartitionState(tp);
        if (partitionState == null)
            return;

        // For now, just ensure leader is always in ISR
        if (!partitionState.Isr.Contains(_config.BrokerId))
        {
            _clusterState.AddToIsr(tp, _config.BrokerId);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "ReplicaManager started for broker {BrokerId}")]
    private partial void LogReplicaManagerStarted(int brokerId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Became leader for {Topic}-{Partition} (epoch={LeaderEpoch})")]
    private partial void LogBecameLeader(string topic, int partition, int leaderEpoch);

    [LoggerMessage(Level = LogLevel.Information, Message = "Became follower for {Topic}-{Partition} (leader={LeaderId}, epoch={LeaderEpoch})")]
    private partial void LogBecameFollower(string topic, int partition, int leaderId, int leaderEpoch);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Follower {FollowerId} removed from ISR for {Topic}-{Partition} (lag={Lag})")]
    private partial void LogFollowerRemovedFromIsr(string topic, int partition, int followerId, long lag);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in ISR check loop")]
    private partial void LogIsrCheckError(Exception ex);
}

/// <summary>
/// Tracks pending produce acknowledgments for a partition.
/// </summary>
internal sealed class PendingProduceAcks
{
    private readonly SortedDictionary<long, List<TaskCompletionSource<bool>>> _pending = new();
    private readonly object _lock = new();

    public void Add(long offset, TaskCompletionSource<bool> tcs)
    {
        lock (_lock)
        {
            if (!_pending.TryGetValue(offset, out var list))
            {
                list = [];
                _pending[offset] = list;
            }
            list.Add(tcs);
        }
    }

    public void Complete(long upToOffset)
    {
        List<TaskCompletionSource<bool>> toComplete = [];

        lock (_lock)
        {
            var completed = _pending.Keys.Where(o => o <= upToOffset).ToList();
            foreach (var offset in completed)
            {
                if (_pending.TryGetValue(offset, out var list))
                {
                    toComplete.AddRange(list);
                    _pending.Remove(offset);
                }
            }
        }

        foreach (var tcs in toComplete)
        {
            tcs.TrySetResult(true);
        }
    }
}
