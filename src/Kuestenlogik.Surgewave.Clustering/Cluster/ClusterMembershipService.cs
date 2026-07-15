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
/// #72 Inc2 — this service is the SINGLE registration authority for BOTH wires: the Kafka-wire
/// <c>ClusterMembershipHandler</c> (ApiKey 62/63) is a pure codec delegating here, so a broker
/// registered over either wire heartbeats coherently over the other (one epoch counter, one store).
/// Note the asymmetry the unification creates: the native RPC path is controller-gated (and owns
/// the finalized-level gate-flip epoch bump, see <c>ClusterStateInterBrokerService</c>), while the
/// Kafka path is deliberately un-gated for rolling-upgrade parity — so ANY broker answering
/// ApiKey 62 writes this store, and an external client reaching the controller's Kafka port can
/// re-mint a natively-registered broker's epoch (fence/re-register churn; bounded today because
/// epochs gate only the self-healing heartbeat loop). Gate/authz for the Kafka path is a tracked
/// #72 follow-up and MUST land before fencing/epochs gate real traffic.
/// </para>
/// <para>
/// <b>Epoch scheme (#72 Inc4):</b> epochs are composed — <c>(controller epoch &lt;&lt; 32) | per-reign
/// counter</c>, where the reign epoch folds strictly upward at the mint site (immune to downward
/// wobbles of the shared state) and the hosts persist a node-local high-water
/// (<see cref="ControllerEpochStore"/>, primed at boot) — so a controller mints strictly above every
/// reign its process has ever observed, including across its own restarts; the old restart-at-1
/// counter handed re-registering brokers LOWER epochs. Honest scope: node-local and best-effort — a
/// crash can lose the last persisted advance, and a quiet-reign failover onto a broker that never
/// observed a push of the reign epoch can still elect at (not above) that epoch; both are bounded by
/// exact-equality fencing plus fresh incarnations, and closed for real in Raft mode by #72 Inc5
/// (epoch = committed registration log index, KRaft parity). Broker epochs are still consumed only
/// by the self-healing heartbeat loop, NOT by partition/leader-epoch fencing.
/// </para>
/// </summary>
public sealed partial class ClusterMembershipService
{
    private const string ReplicationListenerName = "REPLICATION";

    private readonly ClusterIdManager _clusterIdManager;
    private readonly ClusterState _clusterState;
    private readonly ILogger<ClusterMembershipService> _logger;

    private readonly ConcurrentDictionary<int, BrokerRegistrationRecord> _registrations = new();

    // #72 Inc4 — composed-mint state (see Register): per-reign counter, reset when the controller
    // epoch observed at mint time moves. Both guarded by _epochLock.
    private int _lastMintControllerEpoch = -1;
    private long _perReignCounter = 1;
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
                // #72 Inc4 — composed mint: (controller epoch << 32) | per-reign counter. Composed
                // epochs are monotone over every reign THIS PROCESS has observed: the reign epoch
                // used for composition only ever rises (strictly-greater fold below, immune to a
                // downward wobble of the shared state such as a snapshot restore), and the hosts
                // persist a node-local high-water (ControllerEpochStore) primed at boot, so a
                // RESTARTED controller elects and mints strictly above its previous reigns too. The
                // epoch is an opaque int64 on both wires and fencing stays exact-equality — no wire
                // change, rolling-upgrade safe. Residual (documented in the class remarks): a
                // quiet-reign failover onto a broker that never observed a push of the reign epoch;
                // Raft mode closes that in #72 Inc5. (The counter cannot realistically overflow
                // 32 bits within one reign — that would take 4 billion registrations.)
                var controllerEpoch = _clusterState.ControllerEpoch;
                if (controllerEpoch > _lastMintControllerEpoch)
                {
                    _lastMintControllerEpoch = controllerEpoch;
                    _perReignCounter = 1;
                }

                brokerEpoch = ((long)_lastMintControllerEpoch << 32) | (uint)_perReignCounter++;
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
