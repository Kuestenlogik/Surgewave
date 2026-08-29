using Kuestenlogik.Surgewave.Clustering.Replication;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Commits a broker's registration to the metadata log and reports the epoch it was given (#171).
/// </summary>
/// <remarks>
/// <para>
/// One flow for both wires. The native SRWV path and the Kafka-wire BrokerRegistration API used to
/// call <c>ClusterMembershipService.Register</c> directly, which minted a composed epoch on
/// whichever broker happened to answer. Only the controller's own registration ever reached the
/// log, so the rule the heartbeat depends on — a broker epoch IS the committed index of its
/// registration entry — held for exactly one broker in the cluster, and "is this broker caught up"
/// had no answer for anyone else.
/// </para>
/// <para>
/// This also closes the asymmetry the unification left behind: the Kafka path was deliberately
/// un-gated, so any broker answering ApiKey 62 wrote the store and could re-mint a natively
/// registered broker's epoch. That was tolerable only while epochs gated nothing but the
/// self-healing heartbeat loop; fencing now decides what a broker is told (#123, #164). Only the
/// controller can commit an entry, so both wires are gated by construction rather than by a check
/// somebody has to remember to add.
/// </para>
/// </remarks>
public sealed partial class BrokerRegistrationCoordinator
{
    private readonly ClusterMembershipService _membership;
    private readonly IBrokerRegistrar _registrar;
    private readonly ClusterState _clusterState;
    private readonly int _localBrokerId;
    private readonly ILogger<BrokerRegistrationCoordinator> _logger;

    public BrokerRegistrationCoordinator(
        ClusterMembershipService membership,
        IBrokerRegistrar registrar,
        ClusterState clusterState,
        int localBrokerId,
        ILogger<BrokerRegistrationCoordinator> logger)
    {
        _membership = membership;
        _registrar = registrar;
        _clusterState = clusterState;
        _localBrokerId = localBrokerId;
        _logger = logger;
    }

    public async Task<BrokerRegistrationOutcome> RegisterAsync(BrokerRegistrationInput input, CancellationToken ct)
    {
        LogRegistration(input.BrokerId, input.ClusterId, input.IncarnationId);

        // Only the controller can commit, so a non-controller says so and the joiner retries
        // against the real one rather than being registered somewhere that does not count.
        if (!_registrar.IsController)
            return new BrokerRegistrationOutcome(ClusterRpcStatus.NotController, -1);

        // Checked before proposing: a wrong cluster id must not commit an entry.
        if (!_membership.ValidateClusterId(input.ClusterId))
        {
            LogClusterIdMismatch(input.BrokerId, input.ClusterId);
            return new BrokerRegistrationOutcome(ClusterRpcStatus.ClusterAuthorizationFailed, -1);
        }

        var finalizedBefore = _clusterState.FinalizedInterBrokerProtocol;
        var endpoints = ClusterMembershipService.ResolveEndpoints(input);

        var committed = await _registrar.RegisterBrokerViaRaftAsync(
            input.BrokerId, endpoints.Host, endpoints.Port, input.Rack, input.IncarnationId,
            endpoints.InterBrokerProtocol, endpoints.ReplicationPort ?? 0, ct).ConfigureAwait(false);

        if (!committed)
        {
            // Not committed is not "rejected": the joiner retries, and a second attempt carrying
            // the same incarnation is idempotent on apply.
            LogNotCommitted(input.BrokerId);
            return new BrokerRegistrationOutcome(ClusterRpcStatus.BrokerNotAvailable, -1);
        }

        if (!_membership.TryGetBrokerEpoch(input.BrokerId, out var brokerEpoch))
        {
            // Committed, but not applied here yet. Retrying is correct and cheap; answering with an
            // epoch we cannot name would not be.
            LogNotApplied(input.BrokerId);
            return new BrokerRegistrationOutcome(ClusterRpcStatus.BrokerNotAvailable, -1);
        }

        // #72 Inc1 — deterministic upgrade re-convergence: when this registration raises the
        // controller's finalized level to Native (the last downgraded/old peer just re-registered
        // native), bump the controller epoch. Nothing else in a rolling upgrade produces a new epoch
        // at the gate flip.
        if (finalizedBefore < InterBrokerProtocolFeature.Native
            && _clusterState.FinalizedInterBrokerProtocol >= InterBrokerProtocolFeature.Native)
        {
            var epoch = _clusterState.BecomeController(_localBrokerId);
            LogFinalizedRoseEpochBumped(epoch);
        }

        LogRegistered(input.BrokerId, endpoints.Host, endpoints.Port, brokerEpoch, endpoints.InterBrokerProtocol);
        return new BrokerRegistrationOutcome(ClusterRpcStatus.None, brokerEpoch);
    }

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Broker registration: BrokerId={BrokerId} ClusterId={ClusterId} IncarnationId={IncarnationId}")]
    private partial void LogRegistration(int brokerId, string clusterId, Guid incarnationId);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Rejecting registration of broker {BrokerId}: cluster id {ClusterId} is not this cluster's")]
    private partial void LogClusterIdMismatch(int brokerId, string clusterId);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Registration of broker {BrokerId} did not commit to the metadata log; the joiner will retry")]
    private partial void LogNotCommitted(int brokerId);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Registration of broker {BrokerId} committed but is not applied here yet; the joiner will retry")]
    private partial void LogNotApplied(int brokerId);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Broker {BrokerId} registered at {Host}:{Port} (epoch={Epoch}, interBrokerProtocol={InterBrokerProtocol})")]
    private partial void LogRegistered(int brokerId, string host, int port, long epoch, short interBrokerProtocol);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Finalized inter-broker protocol rose to Native; bumped controller epoch to {Epoch}")]
    private partial void LogFinalizedRoseEpochBumped(int epoch);
}
