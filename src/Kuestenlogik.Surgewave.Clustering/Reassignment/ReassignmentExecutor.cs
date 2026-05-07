using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Reassignment;

/// <summary>
/// Executes and manages online partition reassignment plans.
/// Handles the full lifecycle: validation, data replication, ISR expansion,
/// metadata cutover, and cleanup. Supports throttling, progress tracking,
/// and cancellation.
/// </summary>
public sealed partial class ReassignmentExecutor : IAsyncDisposable
{
    private readonly ILogger<ReassignmentExecutor> _logger;
    private readonly ClusterState _clusterState;
    private readonly PartitionReassignmentManager _partitionReassignmentManager;
    private readonly ReassignmentPlanner _planner;
    private readonly ReassignmentConfig _config;

    private readonly ConcurrentDictionary<string, OnlineReassignmentPlan> _plans = new();
    private CancellationTokenSource? _cts;
    private Task? _monitorTask;

    public ReassignmentExecutor(
        ILogger<ReassignmentExecutor> logger,
        ClusterState clusterState,
        PartitionReassignmentManager partitionReassignmentManager,
        ReassignmentPlanner planner,
        ReassignmentConfig config)
    {
        _logger = logger;
        _clusterState = clusterState;
        _partitionReassignmentManager = partitionReassignmentManager;
        _planner = planner;
        _config = config;
    }

    /// <summary>
    /// Start the background monitoring loop that tracks reassignment progress.
    /// </summary>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _monitorTask = Task.Run(() => MonitorPlansAsync(_cts.Token), _cts.Token);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Execute a reassignment plan. Validates the plan, then submits
    /// individual partition assignments to the underlying reassignment manager.
    /// </summary>
    public async Task<ReassignmentResult> ExecuteAsync(
        OnlineReassignmentPlan plan, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(plan);

        var sw = System.Diagnostics.Stopwatch.StartNew();

        // Validate first
        var availableBrokers = _clusterState.Brokers.Keys.ToList();
        var validation = _planner.ValidatePlan(plan, availableBrokers);

        if (!validation.IsValid)
        {
            plan.Status = ReassignmentPlanStatus.Failed;
            plan.CompletedAt = DateTimeOffset.UtcNow;
            LogPlanValidationFailed(plan.Id, validation.Errors.Count);

            return new ReassignmentResult(
                plan.Id,
                ReassignmentPlanStatus.Failed,
                plan.Assignments.Count,
                Completed: 0,
                Failed: plan.Assignments.Count,
                sw.Elapsed,
                TotalBytesCopied: 0);
        }

        // Register the plan
        plan.Status = ReassignmentPlanStatus.Executing;
        plan.StartedAt = DateTimeOffset.UtcNow;
        _plans[plan.Id] = plan;

        LogPlanStarted(plan.Id, plan.Assignments.Count, plan.ThrottleRateBytesPerSec);

        // Convert to the lower-level ReassignmentPlan and submit
        var lowLevelPlan = new ReassignmentPlan
        {
            Version = 1,
            Partitions = plan.Assignments.Select(a => new PartitionReassignment
            {
                Topic = a.Topic,
                Partition = a.Partition,
                Replicas = a.TargetReplicas.ToList()
            }).ToList()
        };

        var submitted = await _partitionReassignmentManager.ExecuteReassignmentAsync(lowLevelPlan, ct);

        if (!submitted)
        {
            plan.Status = ReassignmentPlanStatus.Failed;
            plan.CompletedAt = DateTimeOffset.UtcNow;
            LogPlanSubmissionFailed(plan.Id);

            return new ReassignmentResult(
                plan.Id,
                ReassignmentPlanStatus.Failed,
                plan.Assignments.Count,
                Completed: 0,
                Failed: plan.Assignments.Count,
                sw.Elapsed,
                TotalBytesCopied: 0);
        }

        // Mark individual assignments as in-progress
        foreach (var assignment in plan.Assignments)
        {
            assignment.Status = ReassignmentStatus.Adding;
            assignment.StartedAt = DateTimeOffset.UtcNow;
        }

        return new ReassignmentResult(
            plan.Id,
            ReassignmentPlanStatus.Executing,
            plan.Assignments.Count,
            Completed: 0,
            Failed: 0,
            sw.Elapsed,
            TotalBytesCopied: 0);
    }

    /// <summary>
    /// Cancel a running reassignment plan.
    /// </summary>
    public Task CancelAsync(string planId, CancellationToken ct = default)
    {
        if (_plans.TryGetValue(planId, out var plan))
        {
            if (plan.Status == ReassignmentPlanStatus.Executing)
            {
                // Cancel individual partition reassignments
                foreach (var assignment in plan.Assignments)
                {
                    if (assignment.Status is ReassignmentStatus.Pending or ReassignmentStatus.Adding
                        or ReassignmentStatus.Syncing)
                    {
                        var tp = new TopicPartition { Topic = assignment.Topic, Partition = assignment.Partition };
                        _partitionReassignmentManager.CancelReassignment(tp);
                        assignment.Status = ReassignmentStatus.Cancelled;
                    }
                }

                plan.Status = ReassignmentPlanStatus.Cancelled;
                plan.CompletedAt = DateTimeOffset.UtcNow;
                LogPlanCancelled(planId);
            }
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Get the current status of a reassignment plan.
    /// </summary>
    public OnlineReassignmentPlan? GetStatus(string planId)
    {
        return _plans.TryGetValue(planId, out var plan) ? plan : null;
    }

    /// <summary>
    /// List all reassignment plans (active and completed).
    /// </summary>
    public IReadOnlyList<OnlineReassignmentPlan> ListReassignments()
    {
        return _plans.Values.OrderByDescending(p => p.CreatedAt).ToList();
    }

    /// <summary>
    /// Get current partition assignments across all brokers.
    /// </summary>
    public IReadOnlyList<TopicPartitionInfo> GetCurrentAssignments()
    {
        return _clusterState.PartitionStates
            .Select(kv => new TopicPartitionInfo(
                kv.Key.Topic,
                kv.Key.Partition,
                kv.Value.LeaderBrokerId,
                kv.Value.Replicas.ToList(),
                kv.Value.Isr.ToList(),
                SizeBytes: 0))
            .OrderBy(a => a.Topic)
            .ThenBy(a => a.Partition)
            .ToList();
    }

    private async Task MonitorPlansAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_config.ProgressCheckInterval, ct);

                foreach (var (planId, plan) in _plans)
                {
                    if (plan.Status != ReassignmentPlanStatus.Executing)
                        continue;

                    UpdatePlanProgress(plan);
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

    private void UpdatePlanProgress(OnlineReassignmentPlan plan)
    {
        bool allDone = true;
        bool anyFailed = false;

        foreach (var assignment in plan.Assignments)
        {
            if (assignment.Status is ReassignmentStatus.Completed or ReassignmentStatus.Failed
                or ReassignmentStatus.Cancelled)
                continue;

            var tp = new TopicPartition { Topic = assignment.Topic, Partition = assignment.Partition };
            var underlyingState = _partitionReassignmentManager.GetReassignment(tp);

            if (underlyingState == null)
            {
                // Reassignment may have completed and been removed
                assignment.Status = ReassignmentStatus.Completed;
                assignment.Progress = 1.0;
                assignment.BytesCopied = assignment.TotalBytes;
                assignment.CompletedAt = DateTimeOffset.UtcNow;
                continue;
            }

            // Sync status from underlying manager
            assignment.Status = underlyingState.Status;
            assignment.BytesCopied = underlyingState.BytesReplicated;
            assignment.TotalBytes = underlyingState.TotalBytes;
            assignment.Progress = underlyingState.TotalBytes > 0
                ? (double)underlyingState.BytesReplicated / underlyingState.TotalBytes
                : 0;

            if (underlyingState.Status == ReassignmentStatus.Completed)
            {
                assignment.CompletedAt = underlyingState.CompletedAt;
            }
            else if (underlyingState.Status == ReassignmentStatus.Failed)
            {
                assignment.Error = underlyingState.ErrorMessage;
                anyFailed = true;
            }
            else
            {
                allDone = false;
            }
        }

        if (allDone)
        {
            plan.Status = anyFailed ? ReassignmentPlanStatus.Failed : ReassignmentPlanStatus.Completed;
            plan.CompletedAt = DateTimeOffset.UtcNow;

            var completed = plan.Assignments.Count(a => a.Status == ReassignmentStatus.Completed);
            var failed = plan.Assignments.Count(a => a.Status == ReassignmentStatus.Failed);
            var duration = plan.CompletedAt.Value - (plan.StartedAt ?? plan.CreatedAt);
            var bytesCopied = plan.Assignments.Sum(a => a.BytesCopied);

            LogPlanCompleted(plan.Id, completed, failed, duration.TotalSeconds, bytesCopied);
        }
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

    [LoggerMessage(Level = LogLevel.Warning, Message = "Plan {PlanId} validation failed with {ErrorCount} error(s)")]
    private partial void LogPlanValidationFailed(string planId, int errorCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "Plan {PlanId} started: {AssignmentCount} partition(s), throttle={ThrottleBytes} bytes/sec")]
    private partial void LogPlanStarted(string planId, int assignmentCount, int throttleBytes);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Plan {PlanId} submission failed (not controller)")]
    private partial void LogPlanSubmissionFailed(string planId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Plan {PlanId} cancelled")]
    private partial void LogPlanCancelled(string planId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Plan {PlanId} completed: {Completed} succeeded, {Failed} failed, {Duration:F1}s, {BytesCopied} bytes copied")]
    private partial void LogPlanCompleted(string planId, int completed, int failed, double duration, long bytesCopied);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in reassignment plan monitor loop")]
    private partial void LogMonitorError(Exception ex);
}
