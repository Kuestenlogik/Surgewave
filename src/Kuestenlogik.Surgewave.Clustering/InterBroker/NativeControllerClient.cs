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
/// <see cref="Replication.IIsrChangeNotifier"/> contract): failures are logged, never thrown — state
/// reconciles on the next push or fetch cycle.
/// </para>
/// </summary>
public sealed partial class NativeControllerClient : Replication.IIsrChangeNotifier, Replication.IControlledShutdownRequester, Replication.ITopicCreationRequester, Replication.IGroupConfigRequester, Replication.ITopicConfigRequester
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
    private Replication.IIsrUpdateApplier? _isrApplier;

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
    /// Supplies the applier used when THIS broker is the controller reporting its own ISR (#176).
    /// </summary>
    /// <remarks>
    /// Set the same way as the membership authority, and for the same reason: the client is built
    /// before the controller is.
    /// </remarks>
    public void SetIsrUpdateApplier(Replication.IIsrUpdateApplier applier)
        => _isrApplier = applier;

    /// <summary>
    /// Reverse ISR propagation (#69): a partition leader reports its new ISR to the controller. If
    /// this broker IS the controller, the ISR is already in the shared ClusterState (the leader
    /// mutated it directly), so this only re-broadcasts LeaderAndIsr to the other replicas — no
    /// self-RPC. Otherwise it sends a native AlterPartition frame to the controller's ReplicationPort.
    /// </summary>
    public async Task<bool> NotifyIsrChangedAsync(
        TopicPartition tp,
        int leaderId,
        int leaderEpoch,
        IReadOnlyList<int> isr,
        CancellationToken ct = default)
    {
        var controllerId = _clusterState.ControllerId;
        if (controllerId < 0)
            return false; // no controller yet: the leader reports again

        if (controllerId == _config.BrokerId)
        {
            // We ARE the controller, so the report does not go on the wire — it goes into the
            // metadata log, exactly as one arriving from a remote leader does.
            //
            // This used to re-broadcast LeaderAndIsr instead, which was the push model's answer.
            // With the pushes gone (#163 step 3) that made a controller which is ALSO a partition
            // leader the one node whose ISR changes reached nothing at all (#176).
            return _isrApplier is not null
                && await _isrApplier.ApplyIsrUpdateAsync(tp, leaderId, leaderEpoch, isr, ct).ConfigureAwait(false) is not null;
        }

        return await SendFrameAsync(
            controllerId,
            SurgewaveOpCode.InterBrokerAlterPartition,
            new AlterPartitionPayload(leaderId, leaderEpoch, tp, isr),
            ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Ask the controller to take this broker's partition leaderships away before it stops serving
    /// (#180). Returns the partitions it still leads, or <see langword="null"/> when the controller
    /// could not be reached or refused — the caller then has to fall back on the heartbeat timeout.
    /// </summary>
    /// <remarks>
    /// Unlike the ISR report this is NOT fire-and-forget: the whole point is the answer. A broker
    /// that leaves before hearing it has no idea whether its partitions found successors.
    /// </remarks>
    public async Task<IReadOnlyList<TopicPartition>?> RequestControlledShutdownAsync(
        int brokerId, long brokerEpoch, CancellationToken ct = default)
    {
        var controllerId = _clusterState.ControllerId;
        if (controllerId < 0)
            return null;

        if (controllerId == _config.BrokerId)
        {
            // We are the controller ourselves — the shutdown path handles its own partitions
            // directly and never asks anyone.
            return null;
        }

        var broker = _clusterState.GetBroker(controllerId);
        if (broker is null)
        {
            LogBrokerNotFound(controllerId);
            return null;
        }

        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(RequestTimeout);
            var token = timeoutCts.Token;

            var frame = InterBrokerFrameCodec.EncodeFrame(
                SurgewaveOpCode.InterBrokerControlledShutdown,
                new ControlledShutdownPayload(brokerId, brokerEpoch));

            var connection = await _connectionPool
                .GetConnectionAsync(broker.Host, broker.ReplicationPort, token).ConfigureAwait(false);

            var exchangeComplete = false;
            try
            {
                await connection.Stream.WriteAsync(frame, token).ConfigureAwait(false);
                await connection.Stream.FlushAsync(token).ConfigureAwait(false);

                var response = await InterBrokerFrameCodec.ReadFrameAsync(connection.Stream, token).ConfigureAwait(false)
                    ?? throw new EndOfStreamException("Connection closed while reading controlled-shutdown response");

                if (response.Opcode != SurgewaveOpCode.InterBrokerControlledShutdown)
                {
                    LogResponseMismatch(SurgewaveOpCode.InterBrokerControlledShutdown, response.Opcode, controllerId);
                    return null;
                }

                exchangeComplete = true;
                var reader = new SurgewavePayloadReader(response.Payload.Span);
                var decoded = ControlledShutdownResponsePayload.Read(ref reader);

                if (decoded.Status != ClusterRpcStatus.None)
                {
                    LogRejected(SurgewaveOpCode.InterBrokerControlledShutdown, controllerId, decoded.Status);
                    return null;
                }

                return decoded.RemainingPartitions;
            }
            finally
            {
                if (exchangeComplete) connection.Return(); else connection.Discard();
            }
        }
        catch (Exception ex)
        {
            LogSendFailed(SurgewaveOpCode.InterBrokerControlledShutdown, controllerId, ex);
            return null;
        }
    }

    /// <summary>
    /// Ask the controller to create a topic (#207), and say what it answered: that it did, that it is not the
    /// controller any more, or that the creation failed or it could not be reached. Only the controller writes topics
    /// to the metadata log, so a broker asked for a topic hands it on rather than creating it for itself alone.
    /// </summary>
    public async Task<TopicCreationAnswer> RequestCreateTopicAsync(
        string topic, int partitions, short replicationFactor, IReadOnlyDictionary<string, string>? config,
        IReadOnlyList<IReadOnlyList<int>>? replicaAssignment, CancellationToken ct = default)
    {
        var controllerId = _clusterState.ControllerId;
        if (controllerId < 0 || controllerId == _config.BrokerId)
            return TopicCreationAnswer.NotController; // none known yet, or this broker is named and has lost the office

        var broker = _clusterState.GetBroker(controllerId);
        if (broker is null)
        {
            LogBrokerNotFound(controllerId);
            return TopicCreationAnswer.NotCreated;
        }

        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(RequestTimeout);
            var token = timeoutCts.Token;

            var frame = InterBrokerFrameCodec.EncodeFrame(
                SurgewaveOpCode.InterBrokerCreateTopic,
                new CreateTopicPayload(topic, partitions, replicationFactor, config ?? new Dictionary<string, string>(), replicaAssignment ?? []));

            var connection = await _connectionPool
                .GetConnectionAsync(broker.Host, broker.ReplicationPort, token).ConfigureAwait(false);

            var exchangeComplete = false;
            try
            {
                await connection.Stream.WriteAsync(frame, token).ConfigureAwait(false);
                await connection.Stream.FlushAsync(token).ConfigureAwait(false);

                var response = await InterBrokerFrameCodec.ReadFrameAsync(connection.Stream, token).ConfigureAwait(false)
                    ?? throw new EndOfStreamException("Connection closed while reading create-topic response");
                exchangeComplete = true;

                var reader = new SurgewavePayloadReader(response.Payload.Span);
                var status = InterBrokerStatusPayload.Read(ref reader).Status;
                if (response.Opcode != SurgewaveOpCode.InterBrokerCreateTopic || status != ClusterRpcStatus.None)
                {
                    LogRejected(SurgewaveOpCode.InterBrokerCreateTopic, controllerId, status);
                    return response.Opcode == SurgewaveOpCode.InterBrokerCreateTopic && status == ClusterRpcStatus.NotController
                        ? TopicCreationAnswer.NotController
                        : TopicCreationAnswer.NotCreated;
                }
                return TopicCreationAnswer.Created;
            }
            finally
            {
                if (exchangeComplete) connection.Return(); else connection.Discard();
            }
        }
        catch (Exception ex)
        {
            LogSendFailed(SurgewaveOpCode.InterBrokerCreateTopic, controllerId, ex);
            return TopicCreationAnswer.NotCreated;
        }
    }

    /// <inheritdoc />
    public async Task<bool> RequestSetGroupConfigAsync(string groupId, string name, string? value, CancellationToken ct = default)
    {
        var controllerId = _clusterState.ControllerId;
        if (controllerId < 0 || controllerId == _config.BrokerId)
            return false; // no controller known yet, or this broker thinks it is one and is not: the client retries

        var broker = _clusterState.GetBroker(controllerId);
        if (broker is null)
        {
            LogBrokerNotFound(controllerId);
            return false;
        }

        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(RequestTimeout);
            var token = timeoutCts.Token;

            var frame = InterBrokerFrameCodec.EncodeFrame(SurgewaveOpCode.InterBrokerGroupConfig, new GroupConfigPayload(groupId, name, value));
            var connection = await _connectionPool
                .GetConnectionAsync(broker.Host, broker.ReplicationPort, token).ConfigureAwait(false);

            var exchangeComplete = false;
            try
            {
                await connection.Stream.WriteAsync(frame, token).ConfigureAwait(false);
                await connection.Stream.FlushAsync(token).ConfigureAwait(false);

                var response = await InterBrokerFrameCodec.ReadFrameAsync(connection.Stream, token).ConfigureAwait(false)
                    ?? throw new EndOfStreamException("Connection closed while reading a group-config response");
                exchangeComplete = true;

                var reader = new SurgewavePayloadReader(response.Payload.Span);
                var status = InterBrokerStatusPayload.Read(ref reader).Status;
                if (response.Opcode != SurgewaveOpCode.InterBrokerGroupConfig || status != ClusterRpcStatus.None)
                {
                    LogRejected(SurgewaveOpCode.InterBrokerGroupConfig, controllerId, status);
                    return false;
                }
                return true;
            }
            finally
            {
                if (exchangeComplete) connection.Return(); else connection.Discard();
            }
        }
        catch (Exception ex)
        {
            LogSendFailed(SurgewaveOpCode.InterBrokerGroupConfig, controllerId, ex);
            return false;
        }
    }

    /// <inheritdoc />
    public async Task<bool> RequestChangeTopicConfigAsync(
        string topic, IReadOnlyDictionary<string, string> set, IReadOnlyList<string> removed, CancellationToken ct = default)
    {
        var controllerId = _clusterState.ControllerId;
        if (controllerId < 0 || controllerId == _config.BrokerId)
            return false; // no controller known yet, or this broker thinks it is one and is not: the client retries

        var broker = _clusterState.GetBroker(controllerId);
        if (broker is null)
        {
            LogBrokerNotFound(controllerId);
            return false;
        }

        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(RequestTimeout);
            var token = timeoutCts.Token;

            var frame = InterBrokerFrameCodec.EncodeFrame(SurgewaveOpCode.InterBrokerTopicConfig, new TopicConfigChangePayload(topic, set, removed));
            var connection = await _connectionPool
                .GetConnectionAsync(broker.Host, broker.ReplicationPort, token).ConfigureAwait(false);

            var exchangeComplete = false;
            try
            {
                await connection.Stream.WriteAsync(frame, token).ConfigureAwait(false);
                await connection.Stream.FlushAsync(token).ConfigureAwait(false);

                var response = await InterBrokerFrameCodec.ReadFrameAsync(connection.Stream, token).ConfigureAwait(false)
                    ?? throw new EndOfStreamException("Connection closed while reading a topic-config response");
                exchangeComplete = true;

                var reader = new SurgewavePayloadReader(response.Payload.Span);
                var status = InterBrokerStatusPayload.Read(ref reader).Status;
                if (response.Opcode != SurgewaveOpCode.InterBrokerTopicConfig || status != ClusterRpcStatus.None)
                {
                    LogRejected(SurgewaveOpCode.InterBrokerTopicConfig, controllerId, status);
                    return false;
                }
                return true;
            }
            finally
            {
                if (exchangeComplete) connection.Return(); else connection.Discard();
            }
        }
        catch (Exception ex)
        {
            LogSendFailed(SurgewaveOpCode.InterBrokerTopicConfig, controllerId, ex);
            return false;
        }
    }

    /// <summary>
    /// Frame, send and await the status ack of one native inter-broker request to one broker.
    /// Best-effort: resolution/transport failures are logged and swallowed, never thrown. True when
    /// the broker acknowledged the request.
    /// </summary>
    private async Task<bool> SendFrameAsync<TPayload>(
        int brokerId, SurgewaveOpCode opcode, TPayload payload, CancellationToken ct)
        where TPayload : ISerializablePayload<TPayload>
    {
        var broker = _clusterState.GetBroker(brokerId);
        if (broker is null)
        {
            LogBrokerNotFound(brokerId);
            return false;
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
                    return false;
                }

                exchangeComplete = true;
                var reader = new SurgewavePayloadReader(response.Payload.Span);
                var status = InterBrokerStatusPayload.Read(ref reader).Status;

                if (response.Opcode == SurgewaveOpCode.Error || status != ClusterRpcStatus.None)
                {
                    LogRejected(opcode, brokerId, status);
                    return false;
                }
                LogSent(opcode, brokerId);
                return true;
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
            return false;
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
}
