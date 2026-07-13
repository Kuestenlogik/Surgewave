using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// #60 Inc6b — the protocol-neutral cluster-membership authority: the owner of native broker
/// registration state (incarnation-keyed epochs, fence-until-caught-up). It is the authority for the
/// NATIVE inter-broker join path — the only registration mechanism driven from inside the cluster
/// today (the Kafka-wire <c>BrokerLifecycleManager</c> sender is dead code). It lets the finalized
/// inter-broker protocol level rise as native brokers join.
/// <para>
/// Lives in <c>Clustering</c> (no Protocol.Kafka dependency) so a broker without the Kafka plugin can
/// run it. Inputs/outcomes are the neutral <see cref="BrokerRegistrationInput"/> /
/// <see cref="BrokerHeartbeatInput"/> records; the wire codecs stay in their respective handlers.
/// </para>
/// <para>
/// The legacy Kafka-wire <c>ClusterMembershipHandler</c> keeps its OWN separate store for external
/// Kafka admin clients (ApiKey 62/63); it is NOT unified with this service yet. Since a broker
/// registers over exactly one wire per build (native for plugin-free, and no internal Kafka sender
/// exists), the two stores never both own the same broker id in practice — unification is deferred.
/// </para>
/// <para>
/// <b>Known limit (Inc7+):</b> broker epochs are NOT failover-durable — the counter and store live in
/// the current controller's memory, so a new controller after failover restarts the counter and a
/// re-registering broker can receive a LOWER epoch than before. This is safe today because the broker
/// epoch is consumed only by the heartbeat-validation loop (which self-heals via re-registration),
/// NOT by partition/leader-epoch fencing; it must be sourced from a replicated/durable monotonic
/// value (e.g. the metadata-log offset, as KRaft does) before broker epochs gate any real traffic.
/// </para>
/// </summary>
public sealed partial class ClusterMembershipService
{
    private const string ReplicationListenerName = "REPLICATION";

    private readonly ClusterIdManager _clusterIdManager;
    private readonly ClusterState _clusterState;
    private readonly ILogger<ClusterMembershipService> _logger;

    private readonly ConcurrentDictionary<int, BrokerRegistrationRecord> _registrations = new();
    private long _nextBrokerEpoch = 1;
    private readonly Lock _epochLock = new();

    public ClusterMembershipService(
        ClusterIdManager clusterIdManager,
        ClusterState clusterState,
        ILogger<ClusterMembershipService> logger)
    {
        _clusterIdManager = clusterIdManager;
        _clusterState = clusterState;
        _logger = logger;
    }

    /// <summary>Register (or re-register) a broker, assigning/keeping its epoch and updating cluster state.</summary>
    public BrokerRegistrationOutcome Register(BrokerRegistrationInput input)
    {
        LogRegistration(input.BrokerId, input.ClusterId, input.IncarnationId);

        if (!_clusterIdManager.ValidateClusterId(input.ClusterId))
        {
            LogClusterIdMismatch(_clusterIdManager.GetClusterId(), input.ClusterId);
            return new BrokerRegistrationOutcome(ClusterRpcStatus.ClusterAuthorizationFailed, -1);
        }

        // Decide the epoch and write the registration record as ONE atomic step under _epochLock, so
        // two concurrent registrations for the same incarnation can't each mint a different epoch (the
        // "same incarnation keeps its epoch" invariant must hold under concurrency, not just the
        // monotonic counter). Reads elsewhere use the ConcurrentDictionary directly.
        long brokerEpoch;
        lock (_epochLock)
        {
            if (_registrations.TryGetValue(input.BrokerId, out var existing) && existing.IncarnationId == input.IncarnationId)
            {
                brokerEpoch = existing.BrokerEpoch; // same incarnation reconnecting — keep the epoch
            }
            else
            {
                brokerEpoch = _nextBrokerEpoch++; // new broker or a restart (new incarnation)
                LogNewEpoch(input.BrokerId, brokerEpoch);
            }

            _registrations[input.BrokerId] = new BrokerRegistrationRecord
            {
                BrokerId = input.BrokerId,
                IncarnationId = input.IncarnationId,
                BrokerEpoch = brokerEpoch,
                IsFenced = true, // start fenced until caught up
            };
        }

        // Resolve the client listener (explicitly skipping REPLICATION, which is the inter-broker
        // endpoint) and the replication listener separately (#60 Inc5).
        ListenerSpec? clientListener = null;
        ListenerSpec? replicationListener = null;
        foreach (var l in input.Listeners)
        {
            if (IsReplication(l))
                replicationListener ??= l;
            else
                clientListener ??= l;
        }
        clientListener ??= input.Listeners.Count > 0 ? input.Listeners[0] : null;
        var host = clientListener?.Host ?? "localhost";
        var port = clientListener?.Port ?? 9092;
        var interBrokerProtocol = InterBrokerProtocolFeature.LevelFrom(input.Features);

        // Merge into cluster state atomically (#60 Inc6a UpdateBroker): register the full identity for
        // an unknown broker, or update host/port/level for a known one while keeping an explicitly
        // discovered replication port when this registration didn't advertise one (#60 Inc5).
        var advertisedReplicationPort = replicationListener?.Port;
        _clusterState.UpdateBroker(
            input.BrokerId,
            NewBrokerNode(input.BrokerId, host, port, input.Rack, interBrokerProtocol, advertisedReplicationPort, existingReplicationPort: null),
            known => NewBrokerNode(
                input.BrokerId, host, port, input.Rack, interBrokerProtocol, advertisedReplicationPort,
                existingReplicationPort: known.HasExplicitReplicationPort ? known.ReplicationPort : null));

        LogRegistered(input.BrokerId, host, port, brokerEpoch, interBrokerProtocol, _clusterState.FinalizedInterBrokerProtocol);
        return new BrokerRegistrationOutcome(ClusterRpcStatus.None, brokerEpoch);
    }

    /// <summary>Process a broker heartbeat, returning the fence/caught-up/shutdown state.</summary>
    public BrokerHeartbeatOutcome Heartbeat(BrokerHeartbeatInput input)
    {
        if (!_registrations.TryGetValue(input.BrokerId, out var registration))
        {
            LogUnregisteredHeartbeat(input.BrokerId);
            return new BrokerHeartbeatOutcome(ClusterRpcStatus.BrokerNotAvailable, IsFenced: true, IsCaughtUp: false, ShouldShutDown: false);
        }

        if (input.BrokerEpoch != registration.BrokerEpoch)
        {
            LogStaleBrokerEpoch(input.BrokerId, input.BrokerEpoch, registration.BrokerEpoch);
            return new BrokerHeartbeatOutcome(ClusterRpcStatus.StaleBrokerEpoch, IsFenced: true, IsCaughtUp: false, ShouldShutDown: false);
        }

        registration.CurrentMetadataOffset = input.CurrentMetadataOffset;

        // Caught-up once the broker has reached a non-negative metadata offset (mirrors the Kafka-wire
        // handler's placeholder — a full impl compares against the controller's metadata-log offset).
        var isCaughtUp = registration.CurrentMetadataOffset >= 0;
        var shouldShutDown = false;

        if (input.WantFence && !registration.IsFenced)
        {
            registration.IsFenced = true;
        }
        else if (!input.WantFence && registration.IsFenced && isCaughtUp)
        {
            LogUnfenced(input.BrokerId, input.CurrentMetadataOffset);
            registration.IsFenced = false;
        }

        if (input.WantShutDown)
        {
            // Immediate approval (partition migration before shutdown is a later increment).
            shouldShutDown = true;
        }

        return new BrokerHeartbeatOutcome(ClusterRpcStatus.None, registration.IsFenced, isCaughtUp, shouldShutDown);
    }

    /// <summary>Whether a broker is currently fenced (unknown brokers read as fenced).</summary>
    public bool IsBrokerFenced(int brokerId)
        => !_registrations.TryGetValue(brokerId, out var reg) || reg.IsFenced;

    private static bool IsReplication(ListenerSpec l)
        => string.Equals(l.Name, ReplicationListenerName, StringComparison.OrdinalIgnoreCase);

    private static BrokerNode NewBrokerNode(
        int brokerId, string host, int port, string? rack, short interBrokerProtocol,
        int? advertisedReplicationPort, int? existingReplicationPort)
    {
        var replicationPort = advertisedReplicationPort ?? existingReplicationPort;
        return replicationPort is { } rp
            ? new BrokerNode { BrokerId = brokerId, Host = host, Port = port, Rack = rack, InterBrokerProtocol = interBrokerProtocol, ReplicationPort = rp }
            : new BrokerNode { BrokerId = brokerId, Host = host, Port = port, Rack = rack, InterBrokerProtocol = interBrokerProtocol };
    }

    private sealed class BrokerRegistrationRecord
    {
        public required int BrokerId { get; init; }
        public required Guid IncarnationId { get; init; }
        public required long BrokerEpoch { get; init; }
        public bool IsFenced { get; set; } = true;
        public long CurrentMetadataOffset { get; set; } = -1;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Broker registration: BrokerId={BrokerId} ClusterId={ClusterId} IncarnationId={IncarnationId}")]
    private partial void LogRegistration(int brokerId, string clusterId, Guid incarnationId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Rejecting broker registration: cluster ID mismatch. Expected={Expected}, Got={Got}")]
    private partial void LogClusterIdMismatch(string expected, string got);

    [LoggerMessage(Level = LogLevel.Information, Message = "Assigning broker {BrokerId} epoch {Epoch}")]
    private partial void LogNewEpoch(int brokerId, long epoch);

    [LoggerMessage(Level = LogLevel.Information, Message = "Broker {BrokerId} registered at {Host}:{Port} (epoch={Epoch}, interBrokerProtocol={InterBrokerProtocol}, finalized={Finalized})")]
    private partial void LogRegistered(int brokerId, string host, int port, long epoch, short interBrokerProtocol, short finalized);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Heartbeat from unregistered broker {BrokerId}")]
    private partial void LogUnregisteredHeartbeat(int brokerId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Heartbeat from broker {BrokerId} with stale epoch {RequestEpoch} (expected {ExpectedEpoch})")]
    private partial void LogStaleBrokerEpoch(int brokerId, long requestEpoch, long expectedEpoch);

    [LoggerMessage(Level = LogLevel.Information, Message = "Unfencing broker {BrokerId} (caught up at metadata offset {Offset})")]
    private partial void LogUnfenced(int brokerId, long offset);
}
