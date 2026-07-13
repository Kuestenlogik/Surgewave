using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker;

/// <summary>
/// #60 Inc5 — the native SRWV controller→replica client: the protocol-neutral counterpart of the
/// Kafka plugin's <c>ControllerClient</c>. Sends LeaderAndIsr / UpdateMetadata / StopReplica pushes
/// and — as the leader-side <see cref="IIsrChangeNotifier"/> — reverse AlterPartition reports (#69)
/// as <see cref="InterBrokerFrameCodec"/> frames to the peer's <b>ReplicationPort</b>, where the
/// <see cref="NativeInterBrokerServer"/> receives them (Inc4 multiplex). Lives in <c>Clustering</c>
/// with no Protocol.Kafka edge, so a broker without the Kafka plugin can drive the control plane.
/// <para>
/// Every send stamps the payload with this broker's id and the current
/// <see cref="ClusterState.ControllerEpoch"/> so receivers can fence stale pushes from a demoted
/// controller. All sends are best-effort fire-and-forget (matching the neutral
/// <see cref="IControllerReplicaRpc"/> contract): failures are logged, never thrown — state
/// reconciles on the next push or fetch cycle.
/// </para>
/// </summary>
public sealed partial class NativeControllerClient : IControllerReplicaRpc
{
    /// <summary>
    /// Upper bound on a single controller-to-broker round-trip, mirroring the Kafka-wire
    /// ControllerClient: without it an unreachable or wedged peer would block callers (topic create,
    /// reelection) that await the send on their critical path.
    /// </summary>
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(10);

    private readonly ConnectionPool _connectionPool;
    private readonly ClusterState _clusterState;
    private readonly ClusteringConfig _config;
    private readonly ILogger<NativeControllerClient> _logger;

    public NativeControllerClient(
        ConnectionPool connectionPool,
        ClusterState clusterState,
        ClusteringConfig config,
        ILogger<NativeControllerClient> logger)
    {
        _connectionPool = connectionPool;
        _clusterState = clusterState;
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Send LeaderAndIsr to every affected broker (leader and all replicas), one frame per broker.
    /// </summary>
    public async Task SendLeaderAndIsrAsync(
        IEnumerable<(TopicPartition Tp, PartitionState State)> partitionChanges,
        CancellationToken ct = default)
    {
        if (!TrySnapshot(partitionChanges, out var changes))
            return;

        var brokerPartitions = GroupByAffectedBroker(changes);

        var controllerEpoch = _clusterState.ControllerEpoch;
        var liveBrokers = SnapshotLiveBrokers();
        var tasks = brokerPartitions
            .Where(kvp => kvp.Key != _config.BrokerId) // don't send to self
            .Select(kvp => SendFrameAsync(
                kvp.Key,
                SurgewaveOpCode.InterBrokerLeaderAndIsr,
                new PartitionStatesPayload(_config.BrokerId, controllerEpoch, liveBrokers, kvp.Value),
                ct));

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    /// <summary>
    /// Send UpdateMetadata to all brokers (all partitions if <paramref name="partitionStates"/> is null).
    /// </summary>
    public async Task SendUpdateMetadataAsync(
        IEnumerable<(TopicPartition Tp, PartitionState State)>? partitionStates = null,
        CancellationToken ct = default)
    {
        if (!TrySnapshot(partitionStates ?? _clusterState.GetAllPartitionStates(), out var partitions))
            return;

        var payload = new PartitionStatesPayload(
            _config.BrokerId, _clusterState.ControllerEpoch, SnapshotLiveBrokers(), partitions);

        var tasks = _clusterState.Brokers.Values
            .Where(b => b.BrokerId != _config.BrokerId)
            .Select(b => SendFrameAsync(b.BrokerId, SurgewaveOpCode.InterBrokerUpdateMetadata, payload, ct));

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    /// <summary>
    /// Send StopReplica to a specific broker.
    /// </summary>
    public async Task SendStopReplicaAsync(
        int brokerId,
        IEnumerable<(TopicPartition Tp, int LeaderEpoch, bool DeletePartition)> partitions,
        CancellationToken ct = default)
    {
        var payload = new StopReplicaPayload(
            _config.BrokerId, _clusterState.ControllerEpoch, brokerId, partitions.ToList());

        await SendFrameAsync(brokerId, SurgewaveOpCode.InterBrokerStopReplica, payload, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Reverse ISR propagation (#69): a partition leader reports its new ISR to the controller. If
    /// this broker IS the controller, the ISR is already in the shared ClusterState (the leader
    /// mutated it directly), so this only re-broadcasts LeaderAndIsr to the other replicas — no
    /// self-RPC. Otherwise it sends a native AlterPartition frame to the controller's ReplicationPort.
    /// </summary>
    public async Task NotifyIsrChangedAsync(
        TopicPartition tp,
        int leaderId,
        int leaderEpoch,
        IReadOnlyList<int> isr,
        CancellationToken ct = default)
    {
        var controllerId = _clusterState.ControllerId;
        if (controllerId < 0)
            return;

        if (controllerId == _config.BrokerId)
        {
            var state = _clusterState.GetPartitionState(tp);
            if (state != null)
            {
                await SendLeaderAndIsrAsync([(tp, state)], ct).ConfigureAwait(false);
            }
            return;
        }

        await SendFrameAsync(
            controllerId,
            SurgewaveOpCode.InterBrokerAlterPartition,
            new AlterPartitionPayload(leaderId, leaderEpoch, tp, isr),
            ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Snapshot every known broker as a <see cref="LiveBrokerSpec"/> — the native counterpart of
    /// LiveLeaders/LiveBrokers on the Kafka wire (#69): the push that makes a peer a follower must
    /// also teach it the leader's endpoint. Carries the REAL replication port and the advertised
    /// protocol level. Iterates the concurrent map directly (no .Values copy).
    /// </summary>
    private List<LiveBrokerSpec> SnapshotLiveBrokers()
    {
        var brokers = new List<LiveBrokerSpec>();
        foreach (var kvp in _clusterState.Brokers)
        {
            var b = kvp.Value;
            brokers.Add(new LiveBrokerSpec(
                b.BrokerId, b.Host, b.Port, b.ReplicationPort, b.InterBrokerProtocol, b.Rack));
        }
        return brokers;
    }

    /// <summary>
    /// Deep-copy the partition states ONCE per send. The inputs are live <see cref="ClusterState"/>
    /// references whose Replicas/Isr lists mutate concurrently; the frame codec passes over the
    /// payload twice (EstimateSize + Write) and once per target broker, so serializing the live
    /// objects could tear or throw mid-encode. Each entry is cloned under the cluster-state lock
    /// (<see cref="ClusterState.CopyPartitionStateLocked"/>) so the copy itself cannot tear; the
    /// try/catch is a backstop. Best-effort: a failure is logged and the push skipped — state
    /// reconciles on the next push.
    /// </summary>
    private bool TrySnapshot(
        IEnumerable<(TopicPartition Tp, PartitionState State)> source,
        out List<(TopicPartition Tp, PartitionState State)> snapshot)
    {
        try
        {
            snapshot = source
                .Select(e => (e.Tp, _clusterState.CopyPartitionStateLocked(e.State)))
                .ToList();
            return true;
        }
        catch (Exception ex)
        {
            LogSnapshotFailed(ex);
            snapshot = [];
            return false;
        }
    }

    private static Dictionary<int, List<(TopicPartition Tp, PartitionState State)>> GroupByAffectedBroker(
        IEnumerable<(TopicPartition Tp, PartitionState State)> partitionChanges)
    {
        var brokerPartitions = new Dictionary<int, List<(TopicPartition Tp, PartitionState State)>>();
        foreach (var (tp, state) in partitionChanges)
        {
            foreach (var brokerId in state.Replicas)
            {
                if (!brokerPartitions.TryGetValue(brokerId, out var list))
                {
                    list = [];
                    brokerPartitions[brokerId] = list;
                }
                list.Add((tp, state));
            }
        }
        return brokerPartitions;
    }

    /// <summary>
    /// Frame, send and await the status ack of one native inter-broker request to one broker.
    /// Best-effort: resolution/transport failures are logged and swallowed, never thrown.
    /// </summary>
    private async Task SendFrameAsync<TPayload>(
        int brokerId, SurgewaveOpCode opcode, TPayload payload, CancellationToken ct)
        where TPayload : ISerializablePayload<TPayload>
    {
        var broker = _clusterState.GetBroker(brokerId);
        if (broker is null)
        {
            LogBrokerNotFound(brokerId);
            return;
        }

        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(RequestTimeout);
            var token = timeoutCts.Token;

            var frame = InterBrokerFrameCodec.EncodeFrame(opcode, payload);
            var connection = await _connectionPool.GetConnectionAsync(broker.Host, broker.ReplicationPort, token).ConfigureAwait(false);

            // The native frames carry no correlation id — pairing relies on strict one-request/
            // one-response per pooled connection. Only a COMPLETE, matching exchange may return the
            // connection to the pool; any exception (timeout mid-read, EOF) or a mismatched opcode
            // echo means a late/foreign response may sit in the socket buffer, and reusing the
            // connection would pair it with the NEXT request. Such connections are discarded.
            var exchangeComplete = false;
            try
            {
                await connection.Stream.WriteAsync(frame, token).ConfigureAwait(false);
                await connection.Stream.FlushAsync(token).ConfigureAwait(false);

                var response = await InterBrokerFrameCodec.ReadFrameAsync(connection.Stream, token).ConfigureAwait(false)
                    ?? throw new EndOfStreamException("Connection closed while reading native inter-broker response");

                if (response.Opcode != opcode && response.Opcode != SurgewaveOpCode.Error)
                {
                    LogResponseMismatch(opcode, response.Opcode, brokerId);
                    return;
                }

                exchangeComplete = true;
                var reader = new SurgewavePayloadReader(response.Payload.Span);
                var status = InterBrokerStatusPayload.Read(ref reader).Status;

                if (response.Opcode == SurgewaveOpCode.Error || status != ClusterRpcStatus.None)
                {
                    LogRejected(opcode, brokerId, status);
                }
                else
                {
                    LogSent(opcode, brokerId);
                }
            }
            finally
            {
                if (exchangeComplete)
                {
                    connection.Return();
                }
                else
                {
                    connection.Discard();
                }
            }
        }
        catch (Exception ex)
        {
            LogSendFailed(opcode, brokerId, ex);
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Broker {BrokerId} not found in cluster state")]
    private partial void LogBrokerNotFound(int brokerId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Native {Opcode} sent to broker {BrokerId}")]
    private partial void LogSent(SurgewaveOpCode opcode, int brokerId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Native {Opcode} rejected by broker {BrokerId}: {Status}")]
    private partial void LogRejected(SurgewaveOpCode opcode, int brokerId, ClusterRpcStatus status);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to send native {Opcode} to broker {BrokerId}")]
    private partial void LogSendFailed(SurgewaveOpCode opcode, int brokerId, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Native {Opcode} to broker {BrokerId} answered with mismatched opcode {ResponseOpcode} — discarding poisoned connection")]
    private partial void LogResponseMismatch(SurgewaveOpCode opcode, SurgewaveOpCode responseOpcode, int brokerId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Skipping controller push: partition-state snapshot raced a concurrent mutation")]
    private partial void LogSnapshotFailed(Exception ex);
}
