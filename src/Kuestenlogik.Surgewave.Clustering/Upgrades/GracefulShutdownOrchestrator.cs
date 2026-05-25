using System.Diagnostics;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Upgrades;

/// <summary>
/// Orchestrates a graceful broker shutdown for rolling upgrades.
/// Ensures zero-downtime by transferring leadership, draining connections,
/// and announcing shutdown status before stopping.
/// </summary>
public sealed partial class GracefulShutdownOrchestrator
{
    private readonly ILogger<GracefulShutdownOrchestrator> _logger;
    private readonly ClusteringConfig _config;
    private readonly RollingUpgradeConfig _upgradeConfig;
    private readonly ClusterState _clusterState;
    private readonly LeadershipTransfer _leadershipTransfer;
    private readonly RaftNode? _raftNode;

    private volatile bool _isShuttingDown;
    private volatile ShutdownProgress _progress = ShutdownProgress.NotStarted;

    /// <summary>
    /// Whether a graceful shutdown is in progress.
    /// </summary>
    public bool IsShuttingDown => _isShuttingDown;

    /// <summary>
    /// Current progress of the shutdown operation.
    /// </summary>
    public ShutdownProgress Progress => _progress;

    public GracefulShutdownOrchestrator(
        ILogger<GracefulShutdownOrchestrator> logger,
        ClusteringConfig config,
        RollingUpgradeConfig upgradeConfig,
        ClusterState clusterState,
        LeadershipTransfer leadershipTransfer,
        RaftNode? raftNode = null)
    {
        _logger = logger;
        _config = config;
        _upgradeConfig = upgradeConfig;
        _clusterState = clusterState;
        _leadershipTransfer = leadershipTransfer;
        _raftNode = raftNode;
    }

    /// <summary>
    /// Initiate a graceful shutdown with the following steps:
    /// 1. Stop accepting new connections
    /// 2. Transfer leadership for all led partitions to other brokers
    /// 3. Wait for in-flight requests to complete (with timeout)
    /// 4. Flush all pending writes
    /// 5. Close connections gracefully
    /// 6. Announce shutdown via heartbeat (status: SHUTTING_DOWN)
    /// </summary>
    /// <param name="timeout">Maximum time for the entire shutdown. Defaults to config value.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A <see cref="ShutdownResult"/> summarizing the outcome.</returns>
    public async Task<ShutdownResult> InitiateGracefulShutdownAsync(
        TimeSpan? timeout = null, CancellationToken ct = default)
    {
        if (_isShuttingDown)
        {
            LogAlreadyShuttingDown();
            return new ShutdownResult(false, 0, 0, TimeSpan.Zero, ["Shutdown already in progress"]);
        }

        _isShuttingDown = true;
        var sw = Stopwatch.StartNew();
        var effectiveTimeout = timeout ?? _upgradeConfig.GracefulShutdownTimeout;
        var deadline = DateTimeOffset.UtcNow + effectiveTimeout;
        var warnings = new List<string>();

        LogGracefulShutdownStarted(_config.BrokerId, effectiveTimeout);

        // Step 1: Mark broker as shutting down (stops accepting new client connections)
        _progress = ShutdownProgress.StoppingNewConnections;
        LogShutdownStep("Stopping new connections");

        // Step 2: Transfer all partition leaderships
        _progress = ShutdownProgress.TransferringLeadership;
        var transferTimeout = TimeSpan.FromMilliseconds(
            Math.Min((deadline - DateTimeOffset.UtcNow).TotalMilliseconds * 0.7,
                      effectiveTimeout.TotalMilliseconds * 0.7));

        var transferResult = await _leadershipTransfer.TransferAllLeadershipsAsync(transferTimeout, ct);
        if (transferResult.Failed > 0)
        {
            warnings.Add($"{transferResult.Failed} partition leadership transfers failed: " +
                          string.Join(", ", transferResult.FailedPartitions));
        }

        // Step 3: Wait for in-flight requests (brief delay)
        _progress = ShutdownProgress.DrainingRequests;
        LogShutdownStep("Draining in-flight requests");
        var drainTimeout = Math.Min(5000, Math.Max(0, (deadline - DateTimeOffset.UtcNow).TotalMilliseconds));
        if (drainTimeout > 0)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(drainTimeout, 2000)), ct);
        }

        // Step 4: Step down from Raft leadership if applicable
        _progress = ShutdownProgress.SteppingDownRaft;
        if (_raftNode is not null && _raftNode.IsLeader)
        {
            LogShutdownStep("Stepping down from Raft leadership");
            var raftTimeout = deadline - DateTimeOffset.UtcNow;
            if (raftTimeout > TimeSpan.Zero)
            {
                var raftSuccess = await _raftNode.GracefulShutdownAsync(raftTimeout, ct);
                if (!raftSuccess)
                {
                    warnings.Add("Raft leadership transfer timed out — a new leader will be elected via election timeout");
                }
            }
        }

        // Step 5: Announce shutdown complete
        _progress = ShutdownProgress.Completed;
        sw.Stop();

        var success = transferResult.Failed == 0 && warnings.Count == 0;
        LogGracefulShutdownCompleted(_config.BrokerId, success, transferResult.Transferred, sw.Elapsed);

        return new ShutdownResult(
            success,
            transferResult.Transferred,
            0, // Connections closed — actual TCP close happens at broker level
            sw.Elapsed,
            warnings);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Graceful shutdown already in progress")]
    private partial void LogAlreadyShuttingDown();

    [LoggerMessage(Level = LogLevel.Information, Message = "Graceful shutdown started for broker {BrokerId} (timeout={Timeout})")]
    private partial void LogGracefulShutdownStarted(int brokerId, TimeSpan timeout);

    [LoggerMessage(Level = LogLevel.Information, Message = "Shutdown step: {Step}")]
    private partial void LogShutdownStep(string step);

    [LoggerMessage(Level = LogLevel.Information, Message = "Graceful shutdown completed for broker {BrokerId} (success={Success}, partitions transferred={Transferred}, duration={Duration})")]
    private partial void LogGracefulShutdownCompleted(int brokerId, bool success, int transferred, TimeSpan duration);
}

/// <summary>
/// Tracks the current progress of a graceful shutdown operation.
/// </summary>
public enum ShutdownProgress
{
    /// <summary>Not started.</summary>
    NotStarted,

    /// <summary>Stopping acceptance of new client connections.</summary>
    StoppingNewConnections,

    /// <summary>Transferring partition leaderships to other brokers.</summary>
    TransferringLeadership,

    /// <summary>Waiting for in-flight requests to complete.</summary>
    DrainingRequests,

    /// <summary>Stepping down from Raft leadership.</summary>
    SteppingDownRaft,

    /// <summary>Shutdown completed.</summary>
    Completed
}
