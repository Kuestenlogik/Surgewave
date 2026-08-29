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
    /// <summary>
    /// How long one lifecycle exchange may take. Public because the native session
    /// timeout has to clear it: the loop is serial — RPC, then delay — so a slow
    /// controller stretches the effective heartbeat interval by up to this much, and a
    /// session timeout below interval + this would expire a broker whose heartbeat was
    /// merely late on our own transport (#123).
    /// </summary>
    public static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(10);

    private readonly ConnectionPool _connectionPool;
    private readonly ClusteringConfig _config;
    private readonly ILogger<NativeBrokerLifecycleClient> _logger;
    private readonly ControllerEndpointResolver _resolver;

    private bool _reportedUnreachableQuorum;

    public NativeBrokerLifecycleClient(
        ConnectionPool connectionPool,
        ClusterState clusterState,
        ClusteringConfig config,
        ILogger<NativeBrokerLifecycleClient> logger)
    {
        _connectionPool = connectionPool;
        _config = config;
        _logger = logger;

        // Who to ask for the controller is its own question (#169) — an observer cannot derive
        // it from having been in the quorum, because it never was.
        _resolver = new ControllerEndpointResolver(config, clusterState);
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
        var controller = _resolver.Resolve();
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
                _resolver.ReportSuccess();
                _reportedUnreachableQuorum = false;
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
            OnExchangeFailed();
            return null;
        }
    }

    /// <summary>
    /// Moves the resolver on, and says once when the configured quorum has been walked through
    /// without a single answer (#169).
    /// </summary>
    /// <remarks>
    /// On a first start there is no metadata log to learn the quorum from, so the configured
    /// list is the whole truth — and a mistyped entry there is indistinguishable from an
    /// unreachable network unless something says which one it is. Reported once per outage
    /// rather than per attempt, because the loop retries about once a second.
    /// </remarks>
    private void OnExchangeFailed()
    {
        _resolver.ReportFailure();

        if (!_reportedUnreachableQuorum && _resolver.ExhaustedQuorumWithoutContact)
        {
            _reportedUnreachableQuorum = true;
            LogQuorumUnreachable(_resolver.DescribeConfiguredQuorum());
        }
    }

    [LoggerMessage(Level = LogLevel.Error,
        Message = "No configured controller answered; every voter in the quorum ({Quorum}) was tried and "
            + "none of them responded. On a first start there is no metadata log to learn the quorum from, "
            + "so this is either the endpoints being wrong or the controllers not being up")]
    private partial void LogQuorumUnreachable(string quorum);

    [LoggerMessage(Level = LogLevel.Debug, Message = "No controller resolvable for native lifecycle RPC yet")]
    private partial void LogNoController();

    [LoggerMessage(Level = LogLevel.Warning, Message = "Native lifecycle {Opcode} to {Host}:{ReplicationPort} failed")]
    private partial void LogExchangeFailed(SurgewaveOpCode opcode, string host, int replicationPort, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Native lifecycle {Opcode} answered with mismatched opcode {ResponseOpcode} — discarding poisoned connection")]
    private partial void LogResponseMismatch(SurgewaveOpCode opcode, SurgewaveOpCode responseOpcode);
}
