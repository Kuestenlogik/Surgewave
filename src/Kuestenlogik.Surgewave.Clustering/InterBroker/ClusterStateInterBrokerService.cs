using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker;

/// <summary>
/// #60 Inc4/Inc5 — the neutral, in-Clustering implementation of <see cref="INativeInterBrokerService"/>.
/// Applies decoded native inter-broker requests to local broker/cluster state, matching the semantics
/// of the Kafka-wire <c>InterBrokerApiHandler</c> — without any Protocol.Kafka dependency.
/// </summary>
/// <remarks>
/// <b>Controller-epoch fencing (Inc5).</b> Every controller push (UpdateMetadata / LeaderAndIsr /
/// StopReplica) carries the sender's ControllerId/ControllerEpoch and is fenced via the atomic
/// <see cref="ClusterState.TryAdvanceControllerEpoch"/>: an older epoch is rejected with
/// <see cref="ClusterRpcStatus.StaleControllerEpoch"/> before anything is applied, so a delayed push
/// from a demoted controller cannot regress partition metadata during failover. The Kafka-wire
/// <c>InterBrokerApiHandler</c> fences through the SAME method, so pushes interleaved over both
/// wires during a rolling upgrade share one fence and cannot slip past each other.
/// <para>
/// Known limit (parity with the Kafka-wire handler, revisit with Inc6): the fence is atomic but
/// fence-then-apply is not one critical section — two pushes that BOTH pass the fence (equal
/// epochs, or new-then-delayed interleaving) can interleave their per-partition applications.
/// Partition-level staleness is bounded by the leader epoch carried per entry.
/// </para>
/// </remarks>
public sealed partial class ClusterStateInterBrokerService : INativeInterBrokerService
{
    private readonly ILogger<ClusterStateInterBrokerService> _logger;
    private readonly ClusterState _clusterState;
    private readonly ReplicaManager _replicaManager;
    private readonly LogManager _logManager;
    private readonly int _localBrokerId;
    private readonly IIsrUpdateApplier? _isrUpdateApplier;

    public ClusterStateInterBrokerService(
        ILogger<ClusterStateInterBrokerService> logger,
        ClusterState clusterState,
        ReplicaManager replicaManager,
        LogManager logManager,
        int localBrokerId,
        IIsrUpdateApplier? isrUpdateApplier = null)
    {
        _logger = logger;
        _clusterState = clusterState;
        _replicaManager = replicaManager;
        _logManager = logManager;
        _localBrokerId = localBrokerId;
        _isrUpdateApplier = isrUpdateApplier;
    }

    public ValueTask<ClusterRpcStatus> ApplyUpdateMetadataAsync(PartitionStatesPayload payload, CancellationToken ct = default)
    {
        var fence = FenceAndAdvanceControllerEpoch("UpdateMetadata", payload.ControllerId, payload.ControllerEpoch);
        if (fence != ClusterRpcStatus.None)
            return ValueTask.FromResult(fence);

        ApplyLiveBrokers(payload.LiveBrokers);

        foreach (var (tp, state) in payload.Entries)
        {
            // Apply only the topology fields the controller owns (leader/epoch/replicas/ISR); local
            // watermarks/log offsets stay follower-owned, exactly as the Kafka-wire UpdateMetadata does.
            _clusterState.UpdatePartitionState(tp, s =>
            {
                s.LeaderBrokerId = state.LeaderBrokerId;
                s.LeaderEpoch = state.LeaderEpoch;
                s.Replicas.Clear();
                s.Replicas.AddRange(state.Replicas);
                s.Isr.Clear();
                s.Isr.AddRange(state.Isr);
            });
        }

        return ValueTask.FromResult(ClusterRpcStatus.None);
    }

    public async ValueTask<ClusterRpcStatus> ApplyLeaderAndIsrAsync(PartitionStatesPayload payload, CancellationToken ct = default)
    {
        var fence = FenceAndAdvanceControllerEpoch("LeaderAndIsr", payload.ControllerId, payload.ControllerEpoch);
        if (fence != ClusterRpcStatus.None)
            return fence;

        // Learn broker endpoints BEFORE applying partition states: a BecomeFollower transition needs
        // the leader's node (host + real replication port) in cluster state to start fetching (#69).
        ApplyLiveBrokers(payload.LiveBrokers);

        foreach (var (tp, state) in payload.Entries)
        {
            try
            {
                _clusterState.UpdatePartitionState(tp, s =>
                {
                    s.LeaderBrokerId = state.LeaderBrokerId;
                    s.LeaderEpoch = state.LeaderEpoch;
                    s.Replicas.Clear();
                    s.Replicas.AddRange(state.Replicas);
                    s.Isr.Clear();
                    s.Isr.AddRange(state.Isr);
                });

                if (state.LeaderBrokerId == _localBrokerId)
                {
                    await _replicaManager.BecomeLeaderAsync(tp, state.LeaderEpoch, ct).ConfigureAwait(false);
                    LogBecameLeader(tp.Topic, tp.Partition, state.LeaderEpoch);
                }
                else if (state.Replicas.Contains(_localBrokerId))
                {
                    await _replicaManager.BecomeFollowerAsync(tp, state.LeaderBrokerId, state.LeaderEpoch, ct).ConfigureAwait(false);
                    LogBecameFollower(tp.Topic, tp.Partition, state.LeaderBrokerId, state.LeaderEpoch);
                }
                else
                {
                    // The Kafka wire reports this per partition (ReplicaNotAvailable); the native
                    // response is a single status, so a misdirected entry is logged and skipped —
                    // the push as a whole still succeeds, matching the Kafka top-level ErrorCode.
                    LogNotReplica(tp.Topic, tp.Partition);
                }
            }
            catch (Exception ex)
            {
                // Per-partition best effort, like the Kafka-wire handler: one bad partition must not
                // block the leadership transitions of the remaining entries.
                LogApplyError("LeaderAndIsr", tp.Topic, tp.Partition, ex);
            }
        }

        return ClusterRpcStatus.None;
    }

    public async ValueTask<ClusterRpcStatus> ApplyStopReplicaAsync(StopReplicaPayload payload, CancellationToken ct = default)
    {
        var fence = FenceAndAdvanceControllerEpoch("StopReplica", payload.ControllerId, payload.ControllerEpoch);
        if (fence != ClusterRpcStatus.None)
            return fence;

        // A stop can delete partition data, so a frame routed to the wrong broker must be refused,
        // not applied. (The Kafka wire has no such check — the target is implied by the connection.)
        if (payload.BrokerId != _localBrokerId)
        {
            LogMisroutedStopReplica(payload.BrokerId, _localBrokerId);
            return ClusterRpcStatus.ReplicaNotAvailable;
        }

        foreach (var (tp, _, deletePartition) in payload.Partitions)
        {
            try
            {
                _replicaManager.StopReplica(tp);

                if (deletePartition)
                {
                    await _logManager.DeleteLogAsync(tp, ct).ConfigureAwait(false);
                    _clusterState.RemovePartitionState(tp);
                    LogPartitionDeleted(tp.Topic, tp.Partition);
                }
                else
                {
                    LogReplicaStopped(tp.Topic, tp.Partition);
                }
            }
            catch (Exception ex)
            {
                LogApplyError("StopReplica", tp.Topic, tp.Partition, ex);
            }
        }

        return ClusterRpcStatus.None;
    }

    public async ValueTask<ClusterRpcStatus> ApplyIsrChangeAsync(AlterPartitionPayload payload, CancellationToken ct = default)
    {
        // Only the controller may apply ISR updates (mirrors the Kafka-wire AlterPartition handler).
        if (_isrUpdateApplier is null || !_isrUpdateApplier.IsController)
            return ClusterRpcStatus.NotController;

        var updated = await _isrUpdateApplier
            .ApplyIsrUpdateAsync(payload.Tp, payload.LeaderId, payload.LeaderEpoch, payload.NewIsr, ct)
            .ConfigureAwait(false);

        if (updated is null)
            return ClusterRpcStatus.UnknownTopicOrPartition;

        LogIsrChangeApplied(payload.Tp.Topic, payload.Tp.Partition, payload.LeaderId, payload.LeaderEpoch);
        return ClusterRpcStatus.None;
    }

    /// <summary>
    /// Learn broker endpoints from a controller push (#69 / Inc5). Unknown brokers are registered
    /// with their FULL inter-broker identity — including the real replication port and advertised
    /// protocol level, which the Kafka-wire LiveLeaders could never carry. Known brokers keep their
    /// endpoint data (mirroring the Kafka-wire handler's clobber guard: a node discovered from
    /// cluster-node config may hold a better endpoint than a push), but their protocol LEVEL is
    /// converged — it originates from the peer's own registration, and without convergence a
    /// non-controller leader's finalized level would stay pinned forever and its reverse-ISR
    /// reports could never go native. The LOCAL broker's entry is never touched: it is owned by
    /// startup/registration self-advertisement, and a controller snapshot taken before our latest
    /// (re-)registration could regress it.
    /// </summary>
    /// <remarks>
    /// <b>Inc6 gate — level convergence needs ordering.</b> The snapshot carries no version: a
    /// delayed native push (same controller epoch — the fence accepts equals) can re-raise a level
    /// the truth has since dropped, and the Kafka-wire handlers never converge levels DOWN for
    /// already-known brokers, so a downgrade only propagates while the controller stays put. Both
    /// are harmless while nothing can finalize to native without registration (dead until Inc6),
    /// but BEFORE native registration goes live this must carry a metadata version (ignore older
    /// views) and the Kafka-wire path must converge levels too. The GetBroker→AddBroker
    /// read-modify-write here should then also move to a CAS-style ClusterState.UpdateBroker.
    /// </remarks>
    private void ApplyLiveBrokers(IReadOnlyList<LiveBrokerSpec> liveBrokers)
    {
        foreach (var b in liveBrokers)
        {
            if (b.BrokerId == _localBrokerId)
                continue;

            var known = _clusterState.GetBroker(b.BrokerId);
            if (known is null)
            {
                _clusterState.AddBroker(new BrokerNode
                {
                    BrokerId = b.BrokerId,
                    Host = b.Host,
                    Port = b.Port,
                    Rack = b.Rack,
                    ReplicationPort = b.ReplicationPort,
                    InterBrokerProtocol = b.InterBrokerProtocol,
                });
                LogBrokerLearned(b.BrokerId, b.Host, b.Port, b.ReplicationPort);
            }
            else if (known.InterBrokerProtocol != b.InterBrokerProtocol)
            {
                _clusterState.AddBroker(known with { InterBrokerProtocol = b.InterBrokerProtocol });
            }
        }
    }

    private ClusterRpcStatus FenceAndAdvanceControllerEpoch(string opName, int controllerId, int controllerEpoch)
    {
        if (!_clusterState.TryAdvanceControllerEpoch(controllerId, controllerEpoch))
        {
            LogStaleControllerEpoch(opName, controllerEpoch, _clusterState.ControllerEpoch);
            return ClusterRpcStatus.StaleControllerEpoch;
        }

        return ClusterRpcStatus.None;
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Rejecting stale native {OpName} push: controller epoch {RequestEpoch} < current {CurrentEpoch}")]
    private partial void LogStaleControllerEpoch(string opName, int requestEpoch, int currentEpoch);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Refusing native StopReplica addressed to broker {TargetBrokerId} (this is broker {LocalBrokerId})")]
    private partial void LogMisroutedStopReplica(int targetBrokerId, int localBrokerId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Became leader for {Topic}-{Partition} epoch {LeaderEpoch} (native push)")]
    private partial void LogBecameLeader(string topic, int partition, int leaderEpoch);

    [LoggerMessage(Level = LogLevel.Information, Message = "Became follower for {Topic}-{Partition} leader={LeaderId} epoch {LeaderEpoch} (native push)")]
    private partial void LogBecameFollower(string topic, int partition, int leaderId, int leaderEpoch);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Not a replica for {Topic}-{Partition}, ignoring native LeaderAndIsr entry")]
    private partial void LogNotReplica(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Information, Message = "Stopped replica for {Topic}-{Partition} (native push)")]
    private partial void LogReplicaStopped(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Information, Message = "Deleted partition {Topic}-{Partition} (native push)")]
    private partial void LogPartitionDeleted(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Applied native ISR change for {Topic}-{Partition} from leader {LeaderId} epoch {LeaderEpoch}")]
    private partial void LogIsrChangeApplied(string topic, int partition, int leaderId, int leaderEpoch);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error applying native {OpName} for {Topic}-{Partition}")]
    private partial void LogApplyError(string opName, string topic, int partition, Exception ex);

    [LoggerMessage(Level = LogLevel.Information, Message = "Learned broker {BrokerId} at {Host}:{Port} (replication port {ReplicationPort}) from native controller push")]
    private partial void LogBrokerLearned(int brokerId, string host, int port, int replicationPort);
}
