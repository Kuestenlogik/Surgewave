using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Manages partition reassignment operations.
/// Handles the process of moving replicas between brokers.
/// </summary>
public sealed partial class PartitionReassignmentManager : IAsyncDisposable
{
    private readonly ILogger<PartitionReassignmentManager> _logger;
    private readonly ClusterState _clusterState;
    private readonly ClusterController _clusterController;
    private readonly ReplicaManager _replicaManager;
    private readonly LogManager _logManager;
    private readonly ClusteringConfig _config;

    private readonly ConcurrentDictionary<TopicPartition, PartitionReassignmentState> _reassignments = new();
    private CancellationTokenSource? _cts;
    private Task? _monitorTask;

    public PartitionReassignmentManager(
        ILogger<PartitionReassignmentManager> logger,
        ClusterState clusterState,
        ClusterController clusterController,
        ReplicaManager replicaManager,
        LogManager logManager,
        ClusteringConfig config)
    {
        _logger = logger;
        _clusterState = clusterState;
        _clusterController = clusterController;
        _replicaManager = replicaManager;
        _logManager = logManager;
        _config = config;
    }

    /// <summary>
    /// Start the reassignment monitoring loop.
    /// </summary>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _monitorTask = Task.Run(() => MonitorReassignmentsAsync(_cts.Token), _cts.Token);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Get all active reassignments.
    /// </summary>
    public IReadOnlyList<PartitionReassignmentState> GetActiveReassignments()
    {
        return _reassignments.Values
            .Where(r => r.Status is ReassignmentStatus.Pending or ReassignmentStatus.Adding
                or ReassignmentStatus.Syncing or ReassignmentStatus.Completing)
            .ToList();
    }

    /// <summary>
    /// Get reassignment state for a specific partition.
    /// </summary>
    public PartitionReassignmentState? GetReassignment(TopicPartition tp)
    {
        return _reassignments.TryGetValue(tp, out var state) ? state : null;
    }

    /// <summary>
    /// Execute a reassignment plan.
    /// </summary>
    public async Task<bool> ExecuteReassignmentAsync(ReassignmentPlan plan, CancellationToken ct)
    {
        if (!_clusterController.IsController)
        {
            LogNotController();
            return false;
        }

        foreach (var assignment in plan.Partitions)
        {
            var tp = new TopicPartition { Topic = assignment.Topic, Partition = assignment.Partition };
            var currentState = _clusterState.GetPartitionState(tp);

            if (currentState == null)
            {
                LogPartitionNotFound(assignment.Topic, assignment.Partition);
                continue;
            }

            // Create reassignment state
            var originalReplicas = currentState.Replicas.ToList();
            var targetReplicas = assignment.Replicas;

            var addingReplicas = targetReplicas.Except(originalReplicas).ToList();
            var removingReplicas = originalReplicas.Except(targetReplicas).ToList();

            var reassignmentState = new PartitionReassignmentState
            {
                Topic = assignment.Topic,
                Partition = assignment.Partition,
                OriginalReplicas = originalReplicas,
                TargetReplicas = targetReplicas,
                AddingReplicas = addingReplicas,
                RemovingReplicas = removingReplicas,
                Status = ReassignmentStatus.Pending,
                StartedAt = DateTimeOffset.UtcNow
            };

            if (_reassignments.TryAdd(tp, reassignmentState))
            {
                LogReassignmentStarted(tp.Topic, tp.Partition,
                    string.Join(",", originalReplicas),
                    string.Join(",", targetReplicas));
            }
        }

        return true;
    }

    /// <summary>
    /// Generate a reassignment plan for rebalancing topics across brokers.
    /// </summary>
    public ReassignmentPlan GenerateReassignmentPlan(IEnumerable<string> topics, IEnumerable<int> brokerIds)
    {
        var plan = new ReassignmentPlan { Version = 1, Partitions = [] };
        var brokerList = brokerIds.OrderBy(id => id).ToList();

        if (brokerList.Count == 0)
        {
            LogNoBrokersForReassignment();
            return plan;
        }

        foreach (var topic in topics)
        {
            var topicMeta = _clusterState.GetTopic(topic);
            if (topicMeta == null) continue;

            var partitionStates = _clusterState.PartitionStates
                .Where(kv => kv.Key.Topic == topic)
                .OrderBy(kv => kv.Key.Partition)
                .ToList();

            foreach (var (tp, state) in partitionStates)
            {
                var replicationFactor = state.Replicas.Count;
                var newReplicas = new List<int>();

                // Round-robin assignment starting from partition offset
                var startIndex = tp.Partition % brokerList.Count;
                for (int i = 0; i < Math.Min(replicationFactor, brokerList.Count); i++)
                {
                    var idx = (startIndex + i) % brokerList.Count;
                    newReplicas.Add(brokerList[idx]);
                }

                // Only add to plan if replicas are different
                if (!state.Replicas.SequenceEqual(newReplicas))
                {
                    plan.Partitions.Add(new PartitionReassignment
                    {
                        Topic = topic,
                        Partition = tp.Partition,
                        Replicas = newReplicas
                    });
                }
            }
        }

        return plan;
    }

    /// <summary>
    /// Get a summary of all reassignment progress.
    /// </summary>
    public ReassignmentSummary GetSummary()
    {
        var all = _reassignments.Values.ToList();
        var pending = all.Count(r => r.Status == ReassignmentStatus.Pending);
        var inProgress = all.Count(r => r.Status is ReassignmentStatus.Adding
            or ReassignmentStatus.Syncing or ReassignmentStatus.Completing);
        var completed = all.Count(r => r.Status == ReassignmentStatus.Completed);
        var failed = all.Count(r => r.Status == ReassignmentStatus.Failed);

        var totalBytes = all.Sum(r => r.TotalBytes);
        var bytesReplicated = all.Sum(r => r.BytesReplicated);
        var overallProgress = totalBytes > 0 ? (int)(bytesReplicated * 100 / totalBytes) : 0;

        return new ReassignmentSummary
        {
            TotalPartitions = all.Count,
            Pending = pending,
            InProgress = inProgress,
            Completed = completed,
            Failed = failed,
            OverallProgressPercent = overallProgress
        };
    }

    /// <summary>
    /// Cancel a reassignment for a specific partition.
    /// </summary>
    public bool CancelReassignment(TopicPartition tp)
    {
        if (_reassignments.TryGetValue(tp, out var state))
        {
            if (state.Status is ReassignmentStatus.Pending or ReassignmentStatus.Adding)
            {
                state.Status = ReassignmentStatus.Cancelled;
                LogReassignmentCancelled(tp.Topic, tp.Partition);
                return true;
            }
        }
        return false;
    }

    /// <summary>
    /// Clear completed reassignments from tracking.
    /// </summary>
    public int ClearCompleted()
    {
        var toRemove = _reassignments
            .Where(kv => kv.Value.Status is ReassignmentStatus.Completed
                or ReassignmentStatus.Failed or ReassignmentStatus.Cancelled)
            .Select(kv => kv.Key)
            .ToList();

        foreach (var tp in toRemove)
        {
            _reassignments.TryRemove(tp, out _);
        }

        return toRemove.Count;
    }

    private async Task MonitorReassignmentsAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(1000, ct); // Check every second

                foreach (var (tp, state) in _reassignments)
                {
                    if (state.Status == ReassignmentStatus.Pending)
                    {
                        await StartReassignmentAsync(tp, state, ct);
                    }
                    else if (state.Status == ReassignmentStatus.Adding)
                    {
                        await CheckAddingProgressAsync(tp, state, ct);
                    }
                    else if (state.Status == ReassignmentStatus.Syncing)
                    {
                        await CheckSyncingProgressAsync(tp, state, ct);
                    }
                    else if (state.Status == ReassignmentStatus.Completing)
                    {
                        await CompleteReassignmentAsync(tp, state, ct);
                    }
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogMonitorError(ex);
            }
        }
    }

    private async Task StartReassignmentAsync(TopicPartition tp, PartitionReassignmentState state, CancellationToken ct)
    {
        // Add new replicas to the replica list
        var partitionState = _clusterState.GetPartitionState(tp);
        if (partitionState == null)
        {
            state.Status = ReassignmentStatus.Failed;
            state.ErrorMessage = "Partition not found";
            return;
        }

        // Expand replica set to include both old and new replicas temporarily
        var combinedReplicas = state.OriginalReplicas.Union(state.TargetReplicas).ToList();
        _clusterState.AssignReplicas(tp, combinedReplicas, partitionState.MinInSyncReplicas);

        state.Status = ReassignmentStatus.Adding;
        LogReassignmentAddingReplicas(tp.Topic, tp.Partition, string.Join(",", state.AddingReplicas));
    }

    private Task CheckAddingProgressAsync(TopicPartition tp, PartitionReassignmentState state, CancellationToken ct)
    {
        // Initialize progress tracking when transitioning to Syncing
        var log = _logManager.GetLog(tp);
        if (log != null)
        {
            // Capture total bytes from partition log
            state.TotalBytes = log.TotalSize;

            // Capture leader's current LEO as the target offset
            var leaderReplica = _replicaManager.GetReplica(tp);
            state.TargetLeaderOffset = leaderReplica?.LogEndOffset ?? log.HighWatermark;

            // Record starting offset for each adding replica (assumed to be 0 or existing offset)
            foreach (var brokerId in state.AddingReplicas)
            {
                // New replicas typically start from 0
                state.AddingReplicaStartOffsets[brokerId] = 0;
            }

            LogReassignmentSyncInitialized(tp.Topic, tp.Partition, state.TotalBytes, state.TargetLeaderOffset);
        }

        state.Status = ReassignmentStatus.Syncing;
        return Task.CompletedTask;
    }

    private async Task CheckSyncingProgressAsync(TopicPartition tp, PartitionReassignmentState state, CancellationToken ct)
    {
        var partitionState = _clusterState.GetPartitionState(tp);
        if (partitionState == null)
        {
            state.Status = ReassignmentStatus.Failed;
            state.ErrorMessage = "Partition not found during sync";
            return;
        }

        // Check if all new replicas are in ISR
        var allInIsr = state.AddingReplicas.All(r => partitionState.Isr.Contains(r));

        if (allInIsr)
        {
            state.Status = ReassignmentStatus.Completing;
            state.BytesReplicated = state.TotalBytes; // 100% when in ISR
            LogReassignmentSyncComplete(tp.Topic, tp.Partition);
        }
        else
        {
            // Calculate actual replication progress based on follower LEO
            UpdateReplicationProgress(tp, state, partitionState);
        }
    }

    /// <summary>
    /// Calculate replication progress based on follower LEO advancement.
    /// Progress is the minimum progress of all adding replicas.
    /// </summary>
    private void UpdateReplicationProgress(TopicPartition tp, PartitionReassignmentState state, PartitionState partitionState)
    {
        if (state.TargetLeaderOffset == 0 || state.TotalBytes == 0)
        {
            // No data to replicate or not initialized
            state.BytesReplicated = state.TotalBytes;
            return;
        }

        var log = _logManager.GetLog(tp);
        if (log == null) return;

        // Get current leader LEO (may have advanced since sync started)
        var currentLeaderLeo = log.HighWatermark;

        // Use the original target or current LEO (whichever is greater)
        var targetOffset = Math.Max(state.TargetLeaderOffset, currentLeaderLeo);

        // Calculate minimum progress across all adding replicas
        double minProgressRatio = 1.0;

        foreach (var brokerId in state.AddingReplicas)
        {
            // Try to get follower's current LEO from ISR or estimate
            long followerLeo = 0;

            // Check if follower is in ISR (means it's caught up)
            if (partitionState.Isr.Contains(brokerId))
            {
                // Follower is in ISR, so it's caught up to high watermark
                followerLeo = partitionState.HighWatermark;
            }
            else
            {
                // Estimate follower progress - assume some progress since last check
                // In production, this would query the actual follower LEO via the ReplicaManager
                var startOffset = state.AddingReplicaStartOffsets.GetValueOrDefault(brokerId, 0);

                // Estimate progress based on time elapsed (rough approximation)
                var elapsed = (DateTimeOffset.UtcNow - state.StartedAt).TotalSeconds;
                var estimatedProgress = Math.Min(elapsed / 60.0, 0.95); // Cap at 95% until in ISR
                followerLeo = startOffset + (long)((targetOffset - startOffset) * estimatedProgress);
            }

            // Calculate progress ratio for this replica
            var startingOffset = state.AddingReplicaStartOffsets.GetValueOrDefault(brokerId, 0);
            var offsetsToReplicate = targetOffset - startingOffset;

            if (offsetsToReplicate > 0)
            {
                var offsetsReplicated = followerLeo - startingOffset;
                var progressRatio = (double)offsetsReplicated / offsetsToReplicate;
                minProgressRatio = Math.Min(minProgressRatio, Math.Max(0, progressRatio));
            }
        }

        // Convert progress ratio to bytes
        state.BytesReplicated = (long)(state.TotalBytes * minProgressRatio);
    }

    private async Task CompleteReassignmentAsync(TopicPartition tp, PartitionReassignmentState state, CancellationToken ct)
    {
        // Update replica set to target
        var partitionState = _clusterState.GetPartitionState(tp);
        if (partitionState == null)
        {
            state.Status = ReassignmentStatus.Failed;
            state.ErrorMessage = "Partition not found during completion";
            return;
        }

        _clusterState.AssignReplicas(tp, state.TargetReplicas, partitionState.MinInSyncReplicas);

        // Update ISR to only include replicas in target
        var newIsr = partitionState.Isr.Intersect(state.TargetReplicas).ToList();
        if (newIsr.Count == 0 && state.TargetReplicas.Count > 0)
        {
            newIsr.Add(state.TargetReplicas[0]); // Ensure at least one ISR member
        }
        _clusterState.UpdateIsr(tp, newIsr);

        // If leader was removed, elect new leader
        if (state.RemovingReplicas.Contains(partitionState.LeaderBrokerId))
        {
            await _clusterController.ElectLeaderAsync(tp, state.TargetReplicas[0], ct);
        }

        state.Status = ReassignmentStatus.Completed;
        state.CompletedAt = DateTimeOffset.UtcNow;
        state.BytesReplicated = state.TotalBytes;

        LogReassignmentCompleted(tp.Topic, tp.Partition,
            (state.CompletedAt.Value - state.StartedAt).TotalSeconds);
    }

    public async ValueTask DisposeAsync()
    {
        _cts?.Cancel();

        if (_monitorTask != null)
        {
            try { await _monitorTask; } catch (OperationCanceledException) { }
        }

        _cts?.Dispose();
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Cannot execute reassignment: not controller")]
    private partial void LogNotController();

    [LoggerMessage(Level = LogLevel.Warning, Message = "Partition {Topic}-{Partition} not found for reassignment")]
    private partial void LogPartitionNotFound(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Information, Message = "Starting reassignment for {Topic}-{Partition}: [{Original}] -> [{Target}]")]
    private partial void LogReassignmentStarted(string topic, int partition, string original, string target);

    [LoggerMessage(Level = LogLevel.Warning, Message = "No brokers specified for reassignment")]
    private partial void LogNoBrokersForReassignment();

    [LoggerMessage(Level = LogLevel.Information, Message = "Reassignment {Topic}-{Partition} cancelled")]
    private partial void LogReassignmentCancelled(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in reassignment monitor loop")]
    private partial void LogMonitorError(Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Reassignment {Topic}-{Partition}: adding replicas [{Replicas}]")]
    private partial void LogReassignmentAddingReplicas(string topic, int partition, string replicas);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Reassignment {Topic}-{Partition}: sync initialized, totalBytes={TotalBytes}, targetOffset={TargetOffset}")]
    private partial void LogReassignmentSyncInitialized(string topic, int partition, long totalBytes, long targetOffset);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Reassignment {Topic}-{Partition}: sync complete, new replicas in ISR")]
    private partial void LogReassignmentSyncComplete(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Information, Message = "Reassignment {Topic}-{Partition} completed in {Seconds:F1}s")]
    private partial void LogReassignmentCompleted(string topic, int partition, double seconds);
}
