using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Upgrades;

/// <summary>
/// Handles transferring partition leadership from one broker to another.
/// Used during graceful shutdown and rolling upgrades to ensure zero-downtime.
/// </summary>
public sealed partial class LeadershipTransfer
{
    private readonly ILogger<LeadershipTransfer> _logger;
    private readonly ClusterState _clusterState;
    private readonly ClusterController _clusterController;
    private readonly ClusteringConfig _config;
    private readonly RollingUpgradeConfig _upgradeConfig;

    public LeadershipTransfer(
        ILogger<LeadershipTransfer> logger,
        ClusterState clusterState,
        ClusterController clusterController,
        ClusteringConfig config,
        RollingUpgradeConfig upgradeConfig)
    {
        _logger = logger;
        _clusterState = clusterState;
        _clusterController = clusterController;
        _config = config;
        _upgradeConfig = upgradeConfig;
    }

    /// <summary>
    /// Transfer leadership for a single partition to the best candidate.
    /// Picks an ISR member with the lowest lag, or falls back to any replica.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="partition">Partition number.</param>
    /// <param name="preferredBrokerId">Optional preferred target broker. If null, picks best ISR member.</param>
    /// <param name="timeout">Maximum time to wait for the transfer. Defaults to <see cref="RollingUpgradeConfig.LeaderTransferTimeout"/>.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>True if leadership was successfully transferred.</returns>
    public async Task<bool> TransferPartitionLeadershipAsync(
        string topic, int partition, int? preferredBrokerId = null,
        TimeSpan? timeout = null, CancellationToken ct = default)
    {
        var effectiveTimeout = timeout ?? _upgradeConfig.LeaderTransferTimeout;
        var tp = new TopicPartition { Topic = topic, Partition = partition };
        var state = _clusterState.GetPartitionState(tp);

        if (state is null)
        {
            LogPartitionNotFound(topic, partition);
            return false;
        }

        // Already not the leader — nothing to transfer
        if (state.LeaderBrokerId != _config.BrokerId)
        {
            LogAlreadyNotLeader(topic, partition);
            return true;
        }

        // Find best candidate from ISR (excluding ourselves)
        var candidates = state.Isr
            .Where(b => b != _config.BrokerId)
            .ToList();

        if (candidates.Count == 0)
        {
            // Fall back to any replica
            candidates = state.Replicas
                .Where(b => b != _config.BrokerId)
                .ToList();
        }

        if (candidates.Count == 0)
        {
            LogNoCandidates(topic, partition);
            return false;
        }

        // Use preferred broker if specified and eligible
        var targetBroker = preferredBrokerId.HasValue && candidates.Contains(preferredBrokerId.Value)
            ? preferredBrokerId.Value
            : candidates[0]; // Pick first ISR member (lowest lag by convention)

        LogTransferringLeadership(topic, partition, _config.BrokerId, targetBroker);

        // Elect the new leader via the cluster controller
        var elected = await _clusterController.ElectLeaderAsync(tp, targetBroker, ct);
        if (!elected)
        {
            LogTransferFailed(topic, partition, targetBroker);
            return false;
        }

        // Wait for the leadership change to take effect
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(effectiveTimeout);

        while (!cts.IsCancellationRequested)
        {
            var current = _clusterState.GetPartitionState(tp);
            if (current is not null && current.LeaderBrokerId == targetBroker)
            {
                LogTransferCompleted(topic, partition, targetBroker);
                return true;
            }

            await Task.Delay(50, cts.Token);
        }

        LogTransferTimeout(topic, partition, targetBroker, effectiveTimeout);
        return false;
    }

    /// <summary>
    /// Transfer leadership for ALL partitions where this broker is the leader.
    /// </summary>
    /// <param name="timeout">Maximum time for all transfers combined. Defaults to <see cref="RollingUpgradeConfig.GracefulShutdownTimeout"/>.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A <see cref="TransferResult"/> summarizing the outcome.</returns>
    public async Task<TransferResult> TransferAllLeadershipsAsync(
        TimeSpan? timeout = null, CancellationToken ct = default)
    {
        var effectiveTimeout = timeout ?? _upgradeConfig.GracefulShutdownTimeout;
        var deadline = DateTimeOffset.UtcNow + effectiveTimeout;

        var leaderPartitions = _clusterState.GetLeaderPartitions(_config.BrokerId).ToList();
        if (leaderPartitions.Count == 0)
        {
            LogNoPartitionsToTransfer();
            return new TransferResult(0, 0, 0, []);
        }

        LogStartingBulkTransfer(leaderPartitions.Count);

        var transferred = 0;
        var failed = 0;
        var failedPartitions = new List<string>();

        foreach (var tp in leaderPartitions)
        {
            if (ct.IsCancellationRequested || DateTimeOffset.UtcNow >= deadline)
            {
                failedPartitions.Add($"{tp.Topic}-{tp.Partition} (timeout)");
                failed++;
                continue;
            }

            var remaining = deadline - DateTimeOffset.UtcNow;
            var perPartitionTimeout = TimeSpan.FromMilliseconds(
                Math.Min(remaining.TotalMilliseconds, _upgradeConfig.LeaderTransferTimeout.TotalMilliseconds));

            var success = await TransferPartitionLeadershipAsync(
                tp.Topic, tp.Partition, timeout: perPartitionTimeout, ct: ct);

            if (success)
            {
                transferred++;
            }
            else
            {
                failedPartitions.Add($"{tp.Topic}-{tp.Partition}");
                failed++;
            }
        }

        LogBulkTransferCompleted(transferred, failed, leaderPartitions.Count);
        return new TransferResult(leaderPartitions.Count, transferred, failed, failedPartitions);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Partition {Topic}-{Partition} not found in cluster state")]
    private partial void LogPartitionNotFound(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Partition {Topic}-{Partition}: already not the leader, skipping transfer")]
    private partial void LogAlreadyNotLeader(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Partition {Topic}-{Partition}: no eligible candidates for leadership transfer")]
    private partial void LogNoCandidates(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Information, Message = "Transferring leadership for {Topic}-{Partition}: broker {From} -> broker {To}")]
    private partial void LogTransferringLeadership(string topic, int partition, int from, int to);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Leadership transfer failed for {Topic}-{Partition} to broker {TargetBroker}")]
    private partial void LogTransferFailed(string topic, int partition, int targetBroker);

    [LoggerMessage(Level = LogLevel.Information, Message = "Leadership transfer completed for {Topic}-{Partition} to broker {TargetBroker}")]
    private partial void LogTransferCompleted(string topic, int partition, int targetBroker);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Leadership transfer timed out for {Topic}-{Partition} to broker {TargetBroker} after {Timeout}")]
    private partial void LogTransferTimeout(string topic, int partition, int targetBroker, TimeSpan timeout);

    [LoggerMessage(Level = LogLevel.Information, Message = "No partitions to transfer — this broker is not leading any partitions")]
    private partial void LogNoPartitionsToTransfer();

    [LoggerMessage(Level = LogLevel.Information, Message = "Starting bulk leadership transfer for {Count} partitions")]
    private partial void LogStartingBulkTransfer(int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "Bulk leadership transfer completed: {Transferred} transferred, {Failed} failed out of {Total}")]
    private partial void LogBulkTransferCompleted(int transferred, int failed, int total);
}
