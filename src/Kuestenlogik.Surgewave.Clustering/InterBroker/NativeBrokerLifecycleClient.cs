using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker;

/// <summary>
/// #60 Inc6b — the native SRWV broker-lifecycle client: the protocol-neutral <see cref="IBrokerLifecycleRpc"/>
/// counterpart of the (dead) Kafka-wire <c>BrokerLifecycleManager</c>. It sends BrokerRegistration and
/// BrokerHeartbeat frames to the CONTROLLER's <b>ReplicationPort</b>, where the
/// <see cref="NativeInterBrokerServer"/> routes them to the membership authority — so a broker without
/// the Kafka plugin can join the cluster. Lives in <c>Clustering</c> with no Protocol.Kafka edge.
/// </summary>
public sealed partial class NativeBrokerLifecycleClient : IBrokerLifecycleRpc
{
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(10);

    private readonly ConnectionPool _connectionPool;
    private readonly ClusterState _clusterState;
    private readonly ClusteringConfig _config;
    private readonly ILogger<NativeBrokerLifecycleClient> _logger;

    public NativeBrokerLifecycleClient(
        ConnectionPool connectionPool,
        ClusterState clusterState,
        ClusteringConfig config,
        ILogger<NativeBrokerLifecycleClient> logger)
    {
        _connectionPool = connectionPool;
        _clusterState = clusterState;
        _config = config;
        _logger = logger;
    }

    public async Task<BrokerRegistrationOutcome> RegisterAsync(BrokerRegistrationInput input, CancellationToken ct = default)
    {
        var outcome = await ExchangeAsync(
            SurgewaveOpCode.InterBrokerRegistration,
            new BrokerRegistrationRequestPayload(input),
            static (ref SurgewavePayloadReader r) => BrokerRegistrationResponsePayload.Read(ref r).Outcome,
            ct).ConfigureAwait(false);

        return outcome ?? new BrokerRegistrationOutcome(ClusterRpcStatus.BrokerNotAvailable, -1);
    }

    public async Task<BrokerHeartbeatOutcome> HeartbeatAsync(BrokerHeartbeatInput input, CancellationToken ct = default)
    {
        var outcome = await ExchangeAsync(
            SurgewaveOpCode.InterBrokerHeartbeat,
            new BrokerHeartbeatRequestPayload(input),
            static (ref SurgewavePayloadReader r) => BrokerHeartbeatResponsePayload.Read(ref r).Outcome,
            ct).ConfigureAwait(false);

        return outcome ?? new BrokerHeartbeatOutcome(ClusterRpcStatus.BrokerNotAvailable, IsFenced: true, IsCaughtUp: false, ShouldShutDown: false);
    }

    private delegate T DecodeResponse<T>(ref SurgewavePayloadReader reader);

    /// <summary>
    /// Send one lifecycle frame to the controller and decode its response, or return <c>default</c>
    /// (null outcome) on any resolution/transport failure so the caller can map it to a retry outcome.
    /// A failed/incomplete exchange discards the pooled connection so a late response cannot poison the
    /// next request (native frames carry no correlation id) — same discipline as NativeControllerClient.
    /// </summary>
    private async Task<T?> ExchangeAsync<TPayload, T>(
        SurgewaveOpCode opcode, TPayload payload, DecodeResponse<T> decode, CancellationToken ct)
        where TPayload : ISerializablePayload<TPayload>
        where T : class
    {
        var controller = ResolveController();
        if (controller is null)
        {
            LogNoController();
            return null;
        }

        var (host, replicationPort) = controller.Value;
        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(RequestTimeout);
            var token = timeoutCts.Token;

            var frame = InterBrokerFrameCodec.EncodeFrame(opcode, payload);
            var connection = await _connectionPool.GetConnectionAsync(host, replicationPort, token).ConfigureAwait(false);

            var exchangeComplete = false;
            try
            {
                await connection.Stream.WriteAsync(frame, token).ConfigureAwait(false);
                await connection.Stream.FlushAsync(token).ConfigureAwait(false);

                var response = await InterBrokerFrameCodec.ReadFrameAsync(connection.Stream, token).ConfigureAwait(false)
                    ?? throw new EndOfStreamException("Connection closed while reading native lifecycle response");

                if (response.Opcode != opcode)
                {
                    LogResponseMismatch(opcode, response.Opcode);
                    return null;
                }

                exchangeComplete = true;
                var reader = new SurgewavePayloadReader(response.Payload.Span);
                return decode(ref reader);
            }
            finally
            {
                if (exchangeComplete)
                    connection.Return();
                else
                    connection.Discard();
            }
        }
        catch (Exception ex)
        {
            LogExchangeFailed(opcode, host, replicationPort, ex);
            return null;
        }
    }

    /// <summary>
    /// Resolve the controller's (host, ReplicationPort): the known controller from cluster state, else
    /// the lowest-id peer (the controller by the lowest-id convention). Both use the endpoint already
    /// discovered into <see cref="ClusterState"/> by the controller's cluster-node parse (with the real
    /// replication port from a 4-part cluster-node entry), NOT a re-parse of the raw config — which
    /// prepends this broker itself and would resolve to self. Returns null when no peer is known yet
    /// (this broker is the seed/controller, or standalone).
    /// </summary>
    private (string Host, int ReplicationPort)? ResolveController()
    {
        var controllerId = _clusterState.ControllerId;

        // We ARE the controller — there is nobody to register with; idle (no seed fallback, or the
        // controller would try to register against a follower and loop on NotController).
        if (controllerId == _config.BrokerId)
            return null;

        if (controllerId >= 0 && _clusterState.GetBroker(controllerId) is { } controller)
            return (controller.Host, controller.ReplicationPort);

        // Controller not yet known — target the lowest-id peer excluding self (the conventional
        // controller/seed) so a joining broker can bootstrap before the first UpdateMetadata push.
        BrokerNode? seed = null;
        foreach (var kvp in _clusterState.Brokers)
        {
            var b = kvp.Value;
            if (b.BrokerId == _config.BrokerId)
                continue;
            if (seed is null || b.BrokerId < seed.BrokerId)
                seed = b;
        }

        return seed is null ? null : (seed.Host, seed.ReplicationPort);
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "No controller resolvable for native lifecycle RPC yet")]
    private partial void LogNoController();

    [LoggerMessage(Level = LogLevel.Warning, Message = "Native lifecycle {Opcode} to {Host}:{ReplicationPort} failed")]
    private partial void LogExchangeFailed(SurgewaveOpCode opcode, string host, int replicationPort, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Native lifecycle {Opcode} answered with mismatched opcode {ResponseOpcode} — discarding poisoned connection")]
    private partial void LogResponseMismatch(SurgewaveOpCode opcode, SurgewaveOpCode responseOpcode);
}
