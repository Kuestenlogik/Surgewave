using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;

/// <summary>
/// Kafka-wire codec for the cluster-membership APIs (BrokerRegistration 62, BrokerHeartbeat 63).
/// <para>
/// #72 Inc2 — a pure wire adapter over the neutral <see cref="ClusterMembershipService"/>, the SAME
/// registration authority the native SRWV join path uses: ONE epoch counter and ONE store, so a
/// broker registered over either wire can heartbeat over the other without epoch flapping or
/// divergent fencing state (previously this handler kept its own duplicate store and counter).
/// What stays here: DTO decode/encode, correlation/version echo, the
/// <see cref="ClusterRpcStatus"/>→<see cref="ErrorCode"/> cast (the enum values are pinned to
/// Kafka's error codes, so the cast IS the translation), and the Kafka-only OfflineLogDirs warning
/// (log-only; not part of the neutral membership contract).
/// </para>
/// <para>
/// Registration goes through <see cref="BrokerRegistrationCoordinator"/>, the same flow the native
/// SRWV path uses, so a broker gets the same epoch over either wire — the committed index of its
/// registration entry (#171). That also settles the gate this handler used to leave open: it was
/// deliberately un-gated, so any broker answering ApiKey 62 wrote the store and could re-mint a
/// natively registered broker's epoch. Only the controller can commit an entry, so the gate is now
/// structural rather than a check somebody has to remember.
/// </para>
/// </summary>
public sealed class ClusterMembershipHandler : IKafkaRequestHandler
{
    private readonly ClusterMembershipService _membership;
    private readonly BrokerRegistrationCoordinator? _registrationCoordinator;
    private readonly ILogger<ClusterMembershipHandler> _logger;

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.BrokerRegistration,
        ApiKey.BrokerHeartbeat
    ];

    public ClusterMembershipHandler(
        ClusterMembershipService membership,
        ILogger<ClusterMembershipHandler> logger,
        BrokerRegistrationCoordinator? registrationCoordinator = null)
    {
        _membership = membership;
        _logger = logger;
        _registrationCoordinator = registrationCoordinator;
    }

    public Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        return request switch
        {
            BrokerRegistrationRequest registrationRequest => HandleBrokerRegistrationAsync(registrationRequest, cancellationToken),
            BrokerHeartbeatRequest heartbeatRequest => Task.FromResult<KafkaResponse>(HandleBrokerHeartbeat(heartbeatRequest)),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by ClusterMembershipHandler")
        };
    }

    private async Task<KafkaResponse> HandleBrokerRegistrationAsync(
        BrokerRegistrationRequest request, CancellationToken ct)
    {
        // No coordinator means this broker cannot commit — which is what a non-controller answers
        // anyway, so the joiner retries against the controller.
        var outcome = _registrationCoordinator is null
            ? new BrokerRegistrationOutcome(ClusterRpcStatus.NotController, -1)
            : await _registrationCoordinator.RegisterAsync(new BrokerRegistrationInput(
                BrokerId: request.BrokerId,
                ClusterId: request.ClusterId,
                IncarnationId: request.IncarnationId,
                Listeners: [.. request.Listeners.Select(l => new ListenerSpec(l.Name, l.Host, l.Port, l.SecurityProtocol))],
                Features: [.. request.Features.Select(f => new FeatureSpec(f.Name, f.MinSupportedVersion, f.MaxSupportedVersion))],
                Rack: request.Rack,
                PreviousBrokerEpoch: request.PreviousBrokerEpoch), ct).ConfigureAwait(false);

        return new BrokerRegistrationResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = (ErrorCode)(short)outcome.Status,
            BrokerEpoch = outcome.BrokerEpoch
        };
    }

    private BrokerHeartbeatResponse HandleBrokerHeartbeat(BrokerHeartbeatRequest request)
    {
        var outcome = _membership.Heartbeat(new BrokerHeartbeatInput(
            BrokerId: request.BrokerId,
            BrokerEpoch: request.BrokerEpoch,
            CurrentMetadataOffset: request.CurrentMetadataOffset,
            WantFence: request.WantFence,
            WantShutDown: request.WantShutDown));

        // Kafka-only field: log-dir health is not part of the neutral membership contract and is
        // log-only today — it stays a handler-side concern rather than widening the neutral record.
        // Logged only for an ACCEPTED heartbeat, matching the pre-unification handler (a rejected
        // unknown/stale-epoch heartbeat never reached this warning).
        if (outcome.Status == ClusterRpcStatus.None && request.OfflineLogDirs.Count > 0)
        {
            _logger.LogWarning(
                "Broker {BrokerId} reported {Count} offline log directories",
                request.BrokerId, request.OfflineLogDirs.Count);
        }

        return new BrokerHeartbeatResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = (ErrorCode)(short)outcome.Status,
            IsCaughtUp = outcome.IsCaughtUp,
            IsFenced = outcome.IsFenced,
            ShouldShutDown = outcome.ShouldShutDown
        };
    }
}
