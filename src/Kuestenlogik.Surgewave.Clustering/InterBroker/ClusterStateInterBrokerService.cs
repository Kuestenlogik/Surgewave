using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker;

/// <summary>
/// #60 Inc4/Inc5 — the neutral, in-Clustering implementation of <see cref="INativeInterBrokerService"/>.
/// Applies decoded native inter-broker requests to local broker/cluster state, matching the semantics
/// of the Kafka-wire <c>InterBrokerApiHandler</c> — without any Protocol.Kafka dependency.
/// </summary>
/// <remarks>
/// <b>Controller-push ordering (Inc5/Inc6a).</b> Every controller push (UpdateMetadata / LeaderAndIsr /
/// StopReplica) is applied under the shared <see cref="ClusterState.AcquireControllerPushScopeAsync">
/// push gate</see>, which the Kafka-wire <c>InterBrokerApiHandler</c> also holds, so native and Kafka
/// pushes during a rolling upgrade share one critical section and never interleave their per-partition
/// writes. Ordering is layered: (1) the <b>controller-epoch fence</b>
/// (<see cref="ClusterState.TryAdvanceControllerEpoch(int,int,ControllerPushWire?)"/>) rejects a
/// push from a demoted controller (older epoch); (2) the <b>per-partition leader-epoch guard</b>
/// (<see cref="ClusterState.TryApplyControllerPartitionState"/> / <see cref="ClusterState.ShouldStopReplica"/>)
/// skips a delayed/reordered entry whose leader epoch is older than the stored one, while UNRELATED
/// partitions in the same push still apply — so disjoint partial pushes never fence each other out
/// (a coarse per-push version would wrongly drop them). Both guards run through the same
/// <see cref="ClusterState"/> primitives on the Kafka-wire <c>InterBrokerApiHandler</c> too, so the
/// ordering is symmetric across both wires during a rolling upgrade.
/// </remarks>
public sealed partial class ClusterStateInterBrokerService : INativeInterBrokerService
{
    private readonly ILogger<ClusterStateInterBrokerService> _logger;
    private readonly ClusterState _clusterState;
    private readonly ReplicaManager _replicaManager;
    private readonly LogManager _logManager;
    private readonly int _localBrokerId;
    private readonly IIsrUpdateApplier? _isrUpdateApplier;
    private readonly ClusterMembershipService? _membership;
    private readonly ITransactionMarkerSink? _markerSink;

    public ClusterStateInterBrokerService(
        ILogger<ClusterStateInterBrokerService> logger,
        ClusterState clusterState,
        ReplicaManager replicaManager,
        LogManager logManager,
        int localBrokerId,
        IIsrUpdateApplier? isrUpdateApplier = null,
        ClusterMembershipService? membership = null,
        ITransactionMarkerSink? markerSink = null)
    {
        _logger = logger;
        _clusterState = clusterState;
        _replicaManager = replicaManager;
        _logManager = logManager;
        _localBrokerId = localBrokerId;
        _isrUpdateApplier = isrUpdateApplier;
        _membership = membership;
        _markerSink = markerSink;
    }

    public async ValueTask<ClusterRpcStatus> ApplyUpdateMetadataAsync(PartitionStatesPayload payload, CancellationToken ct = default)
    {
        // Hold the push gate across the whole fence-through-apply span so two pushes that both pass
        // the epoch fence cannot interleave their per-partition writes (#60 Inc6a).
        using var scope = await _clusterState.AcquireControllerPushScopeAsync(ct).ConfigureAwait(false);

        var fence = FenceControllerEpoch("UpdateMetadata", payload.ControllerId, payload.ControllerEpoch);
        if (fence != ClusterRpcStatus.None)
            return fence;

        ApplyLiveBrokers(payload.LiveBrokers);

        foreach (var (tp, state) in payload.Entries)
        {
            // Apply the controller-owned topology fields, but only when this entry's leader epoch is
            // not older than the stored one — a delayed/reordered push for this partition is skipped
            // while unrelated partitions still apply (Inc6a per-partition ordering).
            if (!_clusterState.TryApplyControllerPartitionState(tp, state.LeaderBrokerId, state.LeaderEpoch, state.Replicas, state.Isr))
                LogStalePartition("UpdateMetadata", tp.Topic, tp.Partition, state.LeaderEpoch);
        }

        return ClusterRpcStatus.None;
    }

    public async ValueTask<ClusterRpcStatus> ApplyLeaderAndIsrAsync(PartitionStatesPayload payload, CancellationToken ct = default)
    {
        using var scope = await _clusterState.AcquireControllerPushScopeAsync(ct).ConfigureAwait(false);

        var fence = FenceControllerEpoch("LeaderAndIsr", payload.ControllerId, payload.ControllerEpoch);
        if (fence != ClusterRpcStatus.None)
            return fence;

        // Learn broker endpoints BEFORE applying partition states: a BecomeFollower transition needs
        // the leader's node (host + real replication port) in cluster state to start fetching (#69).
        ApplyLiveBrokers(payload.LiveBrokers);

        foreach (var (tp, state) in payload.Entries)
        {
            try
            {
                // Per-partition ordering: skip a delayed/reordered entry whose leader epoch is older
                // than the stored one, but keep applying the rest (Inc6a) — the state write and the
                // BecomeLeader/Follower transition below must agree on the same epoch, so both are
                // gated together.
                if (!_clusterState.TryApplyControllerPartitionState(tp, state.LeaderBrokerId, state.LeaderEpoch, state.Replicas, state.Isr))
                {
                    LogStalePartition("LeaderAndIsr", tp.Topic, tp.Partition, state.LeaderEpoch);
                    continue;
                }

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
        using var scope = await _clusterState.AcquireControllerPushScopeAsync(ct).ConfigureAwait(false);

        var fence = FenceControllerEpoch("StopReplica", payload.ControllerId, payload.ControllerEpoch);
        if (fence != ClusterRpcStatus.None)
            return fence;

        // A stop can delete partition data, so a frame routed to the wrong broker must be refused,
        // not applied. (The Kafka wire has no such check — the target is implied by the connection.)
        if (payload.BrokerId != _localBrokerId)
        {
            LogMisroutedStopReplica(payload.BrokerId, _localBrokerId);
            return ClusterRpcStatus.ReplicaNotAvailable;
        }

        foreach (var (tp, leaderEpoch, deletePartition) in payload.Partitions)
        {
            try
            {
                // Per-partition ordering: a delayed stop for an epoch older than a newer re-assignment
                // must not delete the re-created partition (Inc6a).
                if (!_clusterState.ShouldStopReplica(tp, leaderEpoch))
                {
                    LogStalePartition("StopReplica", tp.Topic, tp.Partition, leaderEpoch);
                    continue;
                }

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

    public ValueTask<BrokerRegistrationOutcome> RegisterBrokerAsync(BrokerRegistrationInput input, CancellationToken ct = default)
    {
        // Only the controller owns the membership epoch/registration store; a non-controller returns
        // NotController so the joining broker retries against the real controller (#60 Inc6b).
        if (_membership is null || !IsController)
            return ValueTask.FromResult(new BrokerRegistrationOutcome(ClusterRpcStatus.NotController, -1));

        var finalizedBefore = _clusterState.FinalizedInterBrokerProtocol;
        var outcome = _membership.Register(input);

        // #72 Inc1 — deterministic upgrade re-convergence: when this registration raises the
        // controller's finalized level to Native (the last downgraded/old peer just re-registered
        // native), bump the controller epoch. Nothing else in a rolling upgrade produces a new epoch
        // at the gate flip, and receivers capped by this reign's earlier Kafka-wire pushes only clear
        // on a native push with a STRICTLY newer epoch (the cap's tie rule) — without the bump they
        // would stay on the fallback wire until some unrelated election. The bump is monotone and
        // benign: both push clients stamp payloads with the live ClusterState.ControllerEpoch.
        if (outcome.Status == ClusterRpcStatus.None
            && finalizedBefore < InterBrokerProtocolFeature.Native
            && _clusterState.FinalizedInterBrokerProtocol >= InterBrokerProtocolFeature.Native)
        {
            var epoch = _clusterState.BecomeController(_localBrokerId);
            LogFinalizedRoseEpochBumped(epoch);
        }

        return ValueTask.FromResult(outcome);
    }

    public ValueTask<BrokerHeartbeatOutcome> HeartbeatAsync(BrokerHeartbeatInput input, CancellationToken ct = default)
    {
        if (_membership is null || !IsController)
            return ValueTask.FromResult(new BrokerHeartbeatOutcome(ClusterRpcStatus.NotController, IsFenced: true, IsCaughtUp: false, ShouldShutDown: false));

        return ValueTask.FromResult(_membership.Heartbeat(input));
    }

    public async ValueTask<ClusterRpcStatus> ApplyWriteTxnMarkersAsync(WriteTxnMarkersRequestPayload payload, CancellationToken ct = default)
    {
        // The sender groups by leader, so every partition here should be led by this broker. Verify
        // leadership for ALL of them BEFORE writing any, so a stale-routed frame (topology moved
        // between grouping and apply) is rejected atomically — no partition gets a marker while
        // another is rejected, which would leave a partial write the sender might re-send.
        foreach (var tp in payload.Partitions)
        {
            var state = _clusterState.GetPartitionState(tp);
            if (state is null || state.LeaderBrokerId != _localBrokerId)
            {
                LogTxnMarkerNotLeader(tp.Topic, tp.Partition);
                return ClusterRpcStatus.NotLeaderForPartition;
            }
        }

        var controlType = payload.Commit
            ? KafkaConstants.ControlRecordType.Commit
            : KafkaConstants.ControlRecordType.Abort;

        // Best-effort per-partition write: an I/O error on partition k leaves 0..k-1 applied and
        // returns Unknown (multi-log append can't be made atomic). The native replicator is
        // single-shot (no retry), so a partial apply is never re-sent and no marker is double-written;
        // the marker sink's LSO recalculation is idempotent per producer/partition.
        foreach (var tp in payload.Partitions)
        {
            try
            {
                var markerBatch = ControlBatchBuilder.BuildTransactionMarker(payload.ProducerId, payload.ProducerEpoch, controlType);
                var offset = await _logManager.AppendBatchAsync(tp, markerBatch, ct).ConfigureAwait(false);

                if (payload.Commit)
                    _markerSink?.CommitTransaction(payload.ProducerId, [tp], offset);
                else
                    _markerSink?.AbortTransaction(payload.ProducerId, [tp], offset);

                LogTxnMarkerWritten(tp.Topic, tp.Partition, payload.ProducerId, payload.Commit ? "COMMIT" : "ABORT", offset);
            }
            catch (Exception ex)
            {
                LogTxnMarkerError(tp.Topic, tp.Partition, ex);
                return ClusterRpcStatus.Unknown;
            }
        }

        return ClusterRpcStatus.None;
    }

    private bool IsController => _isrUpdateApplier?.IsController ?? (_clusterState.ControllerId == _localBrokerId);

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
    /// <b>Level convergence (Inc6a).</b> The whole apply runs under the push gate, and the
    /// endpoint/level write goes through the atomic
    /// <see cref="ClusterState.UpdateBroker(int,BrokerNode,System.Func{BrokerNode,BrokerNode},out bool)"/>
    /// so a concurrent registration writing the same broker's real port is never clobbered by a stale
    /// read (the #69 wrong-port failure).
    /// <para>Downgrade convergence (#72 Inc1): the Kafka UpdateMetadata/LeaderAndIsr DTOs carry no
    /// per-broker level, so a rolling DOWNGRADE cannot converge through the broker map on
    /// non-controllers. It converges on the first fence-passing Kafka-wire controller push instead,
    /// which caps <see cref="ClusterState.FinalizedInterBrokerProtocol"/> via
    /// <see cref="ClusterState.TryAdvanceControllerEpoch(int,int,ControllerPushWire?)"/> (native appliers clear a strictly older
    /// cap). Still open (#72 follow-up): pushes are event-driven only (topic create / election / ISR
    /// apply — there is no periodic UpdateMetadata), so a broker that receives NO push after the
    /// downgrade keeps its stale-high level and its native sends to downgraded peers fail until one
    /// arrives; a per-call transport fallback on native send failure would close that window.</para>
    /// </remarks>
    private void ApplyLiveBrokers(IReadOnlyList<LiveBrokerSpec> liveBrokers)
    {
        foreach (var b in liveBrokers)
        {
            if (b.BrokerId == _localBrokerId)
                continue;

            var ifAbsent = new BrokerNode
            {
                BrokerId = b.BrokerId,
                Host = b.Host,
                Port = b.Port,
                Rack = b.Rack,
                ReplicationPort = b.ReplicationPort,
                InterBrokerProtocol = b.InterBrokerProtocol,
            };

            // Atomic add-or-converge: register an unknown broker with its full identity, or converge
            // ONLY the protocol level of a known broker (keeping its possibly-better discovered
            // endpoint), without a lost-update window against a concurrent registration. The
            // inserted flag is decided inside the lock, so the log is accurate under contention.
            _clusterState.UpdateBroker(
                b.BrokerId,
                ifAbsent,
                known => known.InterBrokerProtocol == b.InterBrokerProtocol
                    ? known
                    : known with { InterBrokerProtocol = b.InterBrokerProtocol },
                out var inserted);

            if (inserted)
                LogBrokerLearned(b.BrokerId, b.Host, b.Port, b.ReplicationPort);
        }
    }

    private ClusterRpcStatus FenceControllerEpoch(string opName, int controllerId, int controllerEpoch)
    {
        // Controller-level fence, evaluated inside the held push gate: a push from a demoted controller
        // (older epoch) is rejected outright. Per-partition ordering within an accepted epoch is
        // handled by the leader-epoch guard in the apply loop (#60 Inc5/Inc6a). The fence atomically
        // records the NATIVE wire (#72 Inc1): a fence-passing remote native push clears a strictly
        // older Kafka-wire cap on the finalized level. A self-delivered push records nothing.
        if (!_clusterState.TryAdvanceControllerEpoch(controllerId, controllerEpoch,
                controllerId != _localBrokerId ? ControllerPushWire.Native : null))
        {
            LogStaleControllerEpoch(opName, controllerEpoch, _clusterState.ControllerEpoch);
            return ClusterRpcStatus.StaleControllerEpoch;
        }

        return ClusterRpcStatus.None;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Finalized inter-broker protocol rose to Native — bumped controller epoch to {Epoch} so capped receivers re-converge on the next push (#72 Inc1)")]
    private partial void LogFinalizedRoseEpochBumped(int epoch);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Rejecting stale native {OpName} push: controller epoch {RequestEpoch} < current {CurrentEpoch}")]
    private partial void LogStaleControllerEpoch(string opName, int requestEpoch, int currentEpoch);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Skipping stale native {OpName} entry for {Topic}-{Partition}: leader epoch {LeaderEpoch} older than stored")]
    private partial void LogStalePartition(string opName, string topic, int partition, int leaderEpoch);

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

    [LoggerMessage(Level = LogLevel.Warning, Message = "Not leader for {Topic}-{Partition}, refusing native WriteTxnMarkers")]
    private partial void LogTxnMarkerNotLeader(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Wrote native {MarkerType} marker for {Topic}-{Partition}, ProducerId={ProducerId}, Offset={Offset}")]
    private partial void LogTxnMarkerWritten(string topic, int partition, long producerId, string markerType, long offset);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error writing native transaction marker for {Topic}-{Partition}")]
    private partial void LogTxnMarkerError(string topic, int partition, Exception ex);
}
