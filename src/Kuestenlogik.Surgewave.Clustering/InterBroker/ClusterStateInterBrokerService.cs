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
    private readonly BrokerRegistrationCoordinator? _registrationCoordinator;
    private readonly IControlledShutdownCoordinator? _shutdownCoordinator;

    public ClusterStateInterBrokerService(
        ILogger<ClusterStateInterBrokerService> logger,
        ClusterState clusterState,
        ReplicaManager replicaManager,
        LogManager logManager,
        int localBrokerId,
        IIsrUpdateApplier? isrUpdateApplier = null,
        ClusterMembershipService? membership = null,
        ITransactionMarkerSink? markerSink = null,
        BrokerRegistrationCoordinator? registrationCoordinator = null,
        IControlledShutdownCoordinator? shutdownCoordinator = null)
    {
        _logger = logger;
        _clusterState = clusterState;
        _replicaManager = replicaManager;
        _logManager = logManager;
        _localBrokerId = localBrokerId;
        _isrUpdateApplier = isrUpdateApplier;
        _membership = membership;
        _markerSink = markerSink;
        _registrationCoordinator = registrationCoordinator;
        _shutdownCoordinator = shutdownCoordinator;
    }

    public async ValueTask<ControlledShutdownResponsePayload> ApplyControlledShutdownAsync(
        ControlledShutdownPayload payload, CancellationToken ct = default)
    {
        // Only the controller may elect, so only the controller can answer this. A broker that is
        // not it says so and the caller finds the real one — the same shape as every other
        // controller-only op here (#180).
        if (_shutdownCoordinator is null || !_shutdownCoordinator.IsController)
        {
            return new ControlledShutdownResponsePayload(ClusterRpcStatus.NotController, []);
        }

        var stillLed = await _shutdownCoordinator
            .MoveLeadershipAwayAsync(payload.BrokerId, ct)
            .ConfigureAwait(false);

        LogControlledShutdownServed(payload.BrokerId, stillLed.Count);
        return new ControlledShutdownResponsePayload(ClusterRpcStatus.None, stillLed);
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
    /// Commits a joining broker's registration to the metadata log (#171).
    /// </summary>
    /// <remarks>
    /// Delegated so this wire and the Kafka wire cannot drift: both used to write the membership
    /// store directly, which minted an epoch on whichever broker answered and never reached the log.
    /// </remarks>
    public async ValueTask<BrokerRegistrationOutcome> RegisterBrokerAsync(BrokerRegistrationInput input, CancellationToken ct = default)
    {
        if (_registrationCoordinator is null)
            return new BrokerRegistrationOutcome(ClusterRpcStatus.NotController, -1);

        return await _registrationCoordinator.RegisterAsync(input, ct).ConfigureAwait(false);
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

    [LoggerMessage(Level = LogLevel.Information, Message = "Served controlled shutdown for broker {BrokerId}: {StillLed} partition(s) still with it")]
    private partial void LogControlledShutdownServed(int brokerId, int stillLed);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Applied native ISR change for {Topic}-{Partition} from leader {LeaderId} epoch {LeaderEpoch}")]
    private partial void LogIsrChangeApplied(string topic, int partition, int leaderId, int leaderEpoch);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Not leader for {Topic}-{Partition}, refusing native WriteTxnMarkers")]
    private partial void LogTxnMarkerNotLeader(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Wrote native {MarkerType} marker for {Topic}-{Partition}, ProducerId={ProducerId}, Offset={Offset}")]
    private partial void LogTxnMarkerWritten(string topic, int partition, long producerId, string markerType, long offset);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error writing native transaction marker for {Topic}-{Partition}")]
    private partial void LogTxnMarkerError(string topic, int partition, Exception ex);
}
