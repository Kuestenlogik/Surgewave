using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc6b — the protocol-neutral membership authority: incarnation-keyed epoch assignment,
/// fence-until-caught-up heartbeats, and the REPLICATION-listener → BrokerNode.ReplicationPort
/// resolution the native controller client depends on.
/// </summary>
public class ClusterMembershipServiceTests
{
    private static (ClusterMembershipService Service, ClusterState State) NewService()
    {
        var config = new ClusteringConfig { BrokerId = 0 };
        var state = new ClusterState();
        var idManager = new ClusterIdManager(config, NullLogger<ClusterIdManager>.Instance);
        var service = new ClusterMembershipService(idManager, state, NullLogger<ClusterMembershipService>.Instance);
        return (service, state);
    }

    // ClusterId "" short-circuits ValidateClusterId to true, keeping tests hermetic (no cluster.id file).
    private static BrokerRegistrationInput Registration(int brokerId, Guid incarnation, short level = InterBrokerProtocolFeature.Native, int? replicationPort = null)
    {
        var listeners = new List<ListenerSpec> { new("PLAINTEXT", "h", 9092 + brokerId, 0) };
        if (replicationPort is { } rp)
            listeners.Add(new ListenerSpec("REPLICATION", "h", rp, 0));

        return new BrokerRegistrationInput(
            BrokerId: brokerId,
            ClusterId: "",
            IncarnationId: incarnation,
            Listeners: listeners,
            Features: [new FeatureSpec(InterBrokerProtocolFeature.FeatureName, InterBrokerProtocolFeature.KafkaWire, level)],
            Rack: null,
            PreviousBrokerEpoch: -1);
    }

    [Fact]
    public void ASessionTimeoutBelowTheFloorIsRejected()
    {
        // The trap #123 named: the lifecycle loop is serial — RPC, then delay — over a
        // client with a 10 s request timeout, so at a 3 s interval a slow controller
        // produces an effective interval of 13 s. HeartbeatTimeoutMs is 10 s and belongs
        // to the TCP detector; borrowing it here would expire brokers for being slow.
        var config = new ClusteringConfig
        {
            BrokerId = 0,
            Port = 9092,
            ReplicationPort = 10092,
            HeartbeatIntervalMs = 3000,
            NativeSessionTimeoutMs = 10000,
        };

        var errors = config.Validate();

        Assert.Contains(errors, e => e.Contains(nameof(ClusteringConfig.NativeSessionTimeoutMs), StringComparison.Ordinal));
    }

    [Fact]
    public void TheDefaultSessionTimeoutClearsTheFloor()
    {
        // A default that fails its own validation would be a trap of its own.
        var config = new ClusteringConfig { BrokerId = 0, Port = 9092, ReplicationPort = 10092 };

        Assert.DoesNotContain(
            config.Validate(),
            e => e.Contains(nameof(ClusteringConfig.NativeSessionTimeoutMs), StringComparison.Ordinal));
    }

    /// <summary>Registers broker 1 and heartbeats it into the unfenced, caught-up state.</summary>
    private static void RegisterAndUnfence(ClusterMembershipService service, int brokerId, Guid incarnation)
    {
        var outcome = service.Register(Registration(brokerId, incarnation));
        var heartbeat = service.Heartbeat(new BrokerHeartbeatInput(
            BrokerId: brokerId, BrokerEpoch: outcome.BrokerEpoch,
            CurrentMetadataOffset: 0, WantFence: false, WantShutDown: false));
        Assert.False(heartbeat.IsFenced);
    }

    [Fact]
    public void ExpireStaleSessions_FencesABrokerThatWentSilent()
    {
        // #123: the native path was purely request-driven. It learned that a broker exists
        // and never that one stopped — a silent broker stayed registered, unfenced and
        // holding a valid epoch, and went into every metadata push as live.
        var (service, _) = NewService();
        RegisterAndUnfence(service, 1, Guid.NewGuid());

        // The clock is a parameter precisely so this is an assertion rather than a wait.
        var expired = service.ExpireStaleSessions(DateTime.UtcNow.AddMinutes(5), TimeSpan.FromSeconds(20));

        Assert.Equal([1], expired);
        Assert.True(service.IsKnownFenced(1));
    }

    [Fact]
    public void ExpireStaleSessions_LeavesABrokerHeardFromRecently()
    {
        var (service, _) = NewService();
        RegisterAndUnfence(service, 1, Guid.NewGuid());

        var expired = service.ExpireStaleSessions(DateTime.UtcNow.AddSeconds(5), TimeSpan.FromSeconds(20));

        Assert.Empty(expired);
        Assert.False(service.IsKnownFenced(1));
    }

    [Fact]
    public void AHeartbeatRefreshesTheSession()
    {
        // The record had no timestamp at all, so a received heartbeat was indistinguishable
        // from its absence the moment the call returned. This is the difference.
        var (service, _) = NewService();
        var incarnation = Guid.NewGuid();
        RegisterAndUnfence(service, 1, incarnation);

        var epoch = service.Register(Registration(1, incarnation)).BrokerEpoch;
        service.Heartbeat(new BrokerHeartbeatInput(
            BrokerId: 1, BrokerEpoch: epoch, CurrentMetadataOffset: 0, WantFence: false, WantShutDown: false));

        Assert.Empty(service.ExpireStaleSessions(DateTime.UtcNow.AddSeconds(5), TimeSpan.FromSeconds(20)));
    }

    [Fact]
    public void ReRegisteringTheSameIncarnationDoesNotReFenceACaughtUpBroker()
    {
        // The latent defect #123 warned about, which session expiry would have made sharp:
        // Register wrote IsFenced = true outside the branch that keeps the epoch, and
        // BrokerLifecycleLoop re-registers with a STABLE incarnation id after any transient
        // heartbeat failure. So every network hiccup fenced a fully caught-up broker.
        var (service, _) = NewService();
        var incarnation = Guid.NewGuid();
        RegisterAndUnfence(service, 1, incarnation);

        service.Register(Registration(1, incarnation));

        Assert.False(service.IsKnownFenced(1));
    }

    [Fact]
    public void ARestartedBrokerStartsFenced()
    {
        // The other half of the same branch: a NEW incarnation is a restarted broker and
        // must start fenced until it catches up. Keeping the fence state would be the
        // opposite bug.
        var (service, _) = NewService();
        RegisterAndUnfence(service, 1, Guid.NewGuid());

        service.Register(Registration(1, Guid.NewGuid()));

        Assert.True(service.IsKnownFenced(1));
    }

    [Fact]
    public void AnUnregisteredBrokerIsNotKnownFenced()
    {
        // IsBrokerFenced answers "fenced" for a broker it has never heard of, which is
        // right for "may this broker act" and wrong for "leave it out of the metadata
        // push": the native path registers followers only, on the controller only, so
        // filtering on it would have emptied the push instead of trimming it.
        var (service, _) = NewService();

        Assert.True(service.IsBrokerFenced(42));
        Assert.False(service.IsKnownFenced(42));
    }

    [Fact]
    public void ApplyReplicatedRemoval_ForgetsBothTheRegistrationAndTheNode()
    {
        // #129: registration had no opposite. _registrations was written and never removed
        // from, so a decommissioned broker stayed in the cluster metadata — and, through the
        // replicated log, stayed there for the cluster, not just for one process.
        var (service, state) = NewService();
        var incarnation = Guid.NewGuid();
        service.Register(Registration(1, incarnation));

        Assert.NotNull(state.GetBroker(1));

        service.ApplyReplicatedRemoval(1);

        // Both halves, because leaving either is what "still in the metadata" was made of.
        Assert.Null(state.GetBroker(1));

        // The epoch bookkeeping is gone too: a heartbeat from the removed broker is answered
        // as unregistered rather than from a record no longer visible in cluster state.
        var outcome = service.Heartbeat(new BrokerHeartbeatInput(
            BrokerId: 1, BrokerEpoch: 1, CurrentMetadataOffset: 0, WantFence: false, WantShutDown: false));
        Assert.Equal(ClusterRpcStatus.BrokerNotAvailable, outcome.Status);
    }

    [Fact]
    public void ApplyReplicatedRemoval_IsIdempotent()
    {
        // A committed entry is applied more than once: on replay after a restart, and by a
        // follower catching up. Removing what is already gone must not throw or report
        // differently — refusing an unknown broker is the proposer's job, not the apply's.
        var (service, state) = NewService();
        service.Register(Registration(1, Guid.NewGuid()));

        service.ApplyReplicatedRemoval(1);
        service.ApplyReplicatedRemoval(1);
        service.ApplyReplicatedRemoval(99);

        Assert.Null(state.GetBroker(1));
        Assert.Null(state.GetBroker(99));
    }

    [Fact]
    public void ARemovedBrokerCanRegisterAgain()
    {
        // Decommission then re-commission with the same id. The re-registration mints a fresh
        // epoch rather than colliding with the forgotten one, which is the property that makes
        // removal safe to use on a broker whose id will be reused.
        var (service, state) = NewService();
        service.Register(Registration(1, Guid.NewGuid()));
        service.ApplyReplicatedRemoval(1);

        var outcome = service.Register(Registration(1, Guid.NewGuid()));

        Assert.Equal(ClusterRpcStatus.None, outcome.Status);
        Assert.NotNull(state.GetBroker(1));
    }

    [Fact]
    public void Register_AssignsMonotonicEpochsAndStoresBroker()
    {
        var (service, state) = NewService();

        var r1 = service.Register(Registration(1, Guid.NewGuid(), replicationPort: 10999));
        var r2 = service.Register(Registration(2, Guid.NewGuid()));

        Assert.Equal(ClusterRpcStatus.None, r1.Status);
        Assert.Equal(ClusterRpcStatus.None, r2.Status);
        Assert.True(r2.BrokerEpoch > r1.BrokerEpoch);

        var node1 = state.GetBroker(1)!;
        Assert.Equal(10999, node1.ReplicationPort);                     // from the REPLICATION listener
        Assert.Equal(InterBrokerProtocolFeature.Native, node1.InterBrokerProtocol);
        Assert.Equal(9093, node1.Port);                                 // client listener 9092+1
    }

    [Fact]
    public void Reregister_SameIncarnation_KeepsEpoch()
    {
        var (service, _) = NewService();
        var incarnation = Guid.NewGuid();

        var first = service.Register(Registration(1, incarnation));
        var again = service.Register(Registration(1, incarnation));

        Assert.Equal(first.BrokerEpoch, again.BrokerEpoch);
    }

    [Fact]
    public void Reregister_NewIncarnation_AssignsFreshEpoch()
    {
        var (service, _) = NewService();

        var first = service.Register(Registration(1, Guid.NewGuid()));
        var restarted = service.Register(Registration(1, Guid.NewGuid())); // restart → new incarnation

        Assert.True(restarted.BrokerEpoch > first.BrokerEpoch);
    }

    // ── #72 Inc4: composed epochs are monotone across controller failover ───────────────────────

    [Fact]
    public void FailoverSuccessor_ThatObservedTheReignEpoch_MintsStrictlyAboveThePredecessor()
    {
        // Old controller's reign: several epochs minted at controller epoch 0.
        var (oldController, _) = NewService();
        var e1 = oldController.Register(Registration(1, Guid.NewGuid())).BrokerEpoch;
        var e2 = oldController.Register(Registration(2, Guid.NewGuid())).BrokerEpoch;
        var e3 = oldController.Register(Registration(3, Guid.NewGuid())).BrokerEpoch;

        // Failover onto a broker that OBSERVED the reign epoch (fence-passing push) — the
        // precondition the composed scheme needs; a successor that never saw a push is the
        // documented quiet-reign residual (Raft mode closes it, #72 Inc5). The old restart-at-1
        // counter would re-mint e1 for the first re-registering broker even WITH this knowledge.
        var (newController, newState) = NewService();
        newState.TryAdvanceControllerEpoch(controllerId: 0, controllerEpoch: 0); // learned via push
        newState.BecomeController(1);

        var afterFailover = newController.Register(Registration(1, Guid.NewGuid())).BrokerEpoch;

        Assert.True(afterFailover > e1);
        Assert.True(afterFailover > e2);
        Assert.True(afterFailover > e3);
    }

    [Fact]
    public void RestartedController_WithHighWaterStore_MintsStrictlyAboveItsPreviousReign()
    {
        var dataDir = Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}");
        try
        {
            // First incarnation of the controller process: elects (epoch persisted via the
            // high-water hook) and mints broker epochs.
            var (first, state1) = NewService();
            var store1 = new ControllerEpochStore(dataDir, NullLogger<ControllerEpochStore>.Instance);
            state1.OnControllerEpochAdvanced = store1.Save;
            state1.BecomeController(0);
            var beforeRestart = first.Register(Registration(1, Guid.NewGuid())).BrokerEpoch;

            // The controller process RESTARTS: fresh ClusterState (in-memory epoch back to 0),
            // fresh membership store. Without the persisted floor it would re-elect at epoch 1 and
            // re-mint duplicate broker epochs (the review MAJOR); the primed floor forces the new
            // reign strictly above the previous one.
            var (restarted, state2) = NewService();
            var store2 = new ControllerEpochStore(dataDir, NullLogger<ControllerEpochStore>.Instance);
            state2.PrimeControllerEpochFloor(store2.Load());
            state2.OnControllerEpochAdvanced = store2.Save;
            state2.BecomeController(0);

            var afterRestart = restarted.Register(Registration(1, Guid.NewGuid())).BrokerEpoch;

            Assert.True(afterRestart > beforeRestart);
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch (IOException) { }
        }
    }

    [Fact]
    public void Mint_IsImmuneToADownwardWobbleOfTheSharedControllerEpoch()
    {
        var (service, state) = NewService();
        state.TryAdvanceControllerEpoch(controllerId: 1, controllerEpoch: 5);

        var atReign5 = service.Register(Registration(1, Guid.NewGuid())).BrokerEpoch;

        // The shared epoch wobbles DOWN (snapshot-restore reset) — the mint folds the reign epoch
        // strictly upward, so composed epochs must not regress below the reign-5 series.
        state.Clear();
        var afterWobble = service.Register(Registration(2, Guid.NewGuid())).BrokerEpoch;

        Assert.True(afterWobble > atReign5);
    }

    [Fact]
    public void ControllerEpochStore_RoundTrips_AndTreatsMissingOrGarbageAsZero()
    {
        var dataDir = Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}");
        try
        {
            var store = new ControllerEpochStore(dataDir, NullLogger<ControllerEpochStore>.Instance);
            Assert.Equal(0, store.Load()); // missing file

            store.Save(42);
            Assert.Equal(42, store.Load());

            Directory.CreateDirectory(dataDir);
            File.WriteAllText(Path.Combine(dataDir, "controller.epoch"), "not-a-number");
            Assert.Equal(0, store.Load()); // garbage reads as 0, never throws
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch (IOException) { }
        }
    }

    [Fact]
    public void ControllerEpochStore_Save_NeverRegressesTheHighWater()
    {
        var dataDir = Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}");
        try
        {
            var store = new ControllerEpochStore(dataDir, NullLogger<ControllerEpochStore>.Instance);
            store.Save(5);
            Assert.Equal(5, store.Load());

            // A LOWER Save must NOT clobber the floor. This is the snapshot-restore path (Clear()
            // resets the in-memory controller epoch to 0, then BecomeController mints epoch 1 and
            // fires Save(1)) and the out-of-order election-vs-push race — a blind overwrite here would
            // prime a restart below the true maximum and re-mint duplicate broker epochs.
            store.Save(3);
            Assert.Equal(5, store.Load());

            // The floor ON DISK (not just this instance's memory) stayed at 5 — a fresh store, as a
            // process restart creates, still reads 5.
            Assert.Equal(5, new ControllerEpochStore(dataDir, NullLogger<ControllerEpochStore>.Instance).Load());

            // A strictly higher Save still advances.
            store.Save(9);
            Assert.Equal(9, store.Load());
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch (IOException) { }
        }
    }

    [Fact]
    public void ControllerEpochBumpMidReign_MintsAboveEarlierEpochs_AndKeepsSameIncarnationEpoch()
    {
        var (service, state) = NewService();
        var keptIncarnation = Guid.NewGuid();

        var before = service.Register(Registration(1, keptIncarnation)).BrokerEpoch;

        // Mid-reign controller-epoch bump (election or the #72 Inc1 finalized-level gate flip).
        state.BecomeController(0);

        // A NEW incarnation mints strictly above the pre-bump epoch …
        var fresh = service.Register(Registration(2, Guid.NewGuid())).BrokerEpoch;
        Assert.True(fresh > before);

        // … while the SAME incarnation re-registering still keeps its original epoch.
        var kept = service.Register(Registration(1, keptIncarnation)).BrokerEpoch;
        Assert.Equal(before, kept);
    }

    [Fact]
    public void Heartbeat_WithPreFailoverEpoch_IsFencedUntilReRegistration()
    {
        var (service, state) = NewService();

        // Pre-failover epoch, held by a broker that survived the controller change.
        var stale = service.Register(Registration(1, Guid.NewGuid())).BrokerEpoch;

        // The broker restarts and re-registers under the new reign (new incarnation, higher epoch).
        state.BecomeController(0);
        var fresh = service.Register(Registration(1, Guid.NewGuid())).BrokerEpoch;
        Assert.True(fresh > stale);

        // A heartbeat still carrying the pre-failover epoch is fenced — the lifecycle loop's
        // re-register self-heal path — while the fresh epoch heartbeats fine.
        Assert.Equal(ClusterRpcStatus.StaleBrokerEpoch, service.Heartbeat(new BrokerHeartbeatInput(1, stale, 0, false, false)).Status);
        Assert.Equal(ClusterRpcStatus.None, service.Heartbeat(new BrokerHeartbeatInput(1, fresh, 0, false, false)).Status);
    }

    [Fact]
    public void Register_WithoutReplicationListener_DerivesReplicationPortFromClientPort()
    {
        var (service, state) = NewService();

        service.Register(Registration(3, Guid.NewGuid())); // no REPLICATION listener

        // Client port 9092+3 = 9095, replication defaults to +1000.
        Assert.Equal(9095 + 1000, state.GetBroker(3)!.ReplicationPort);
    }

    [Fact]
    public void Heartbeat_UnknownBroker_IsBrokerNotAvailable()
    {
        var (service, _) = NewService();

        var outcome = service.Heartbeat(new BrokerHeartbeatInput(99, 1, 0, false, false));

        Assert.Equal(ClusterRpcStatus.BrokerNotAvailable, outcome.Status);
        Assert.True(outcome.IsFenced);
    }

    [Fact]
    public void Heartbeat_StaleEpoch_IsStaleBrokerEpoch()
    {
        var (service, _) = NewService();
        var reg = service.Register(Registration(1, Guid.NewGuid()));

        var outcome = service.Heartbeat(new BrokerHeartbeatInput(1, reg.BrokerEpoch + 99, 0, false, false));

        Assert.Equal(ClusterRpcStatus.StaleBrokerEpoch, outcome.Status);
    }

    [Fact]
    public void Heartbeat_CaughtUp_Unfences()
    {
        var (service, _) = NewService();
        var reg = service.Register(Registration(1, Guid.NewGuid()));
        Assert.True(service.IsBrokerFenced(1)); // starts fenced

        // A heartbeat at a non-negative metadata offset with WantFence=false unfences.
        var outcome = service.Heartbeat(new BrokerHeartbeatInput(1, reg.BrokerEpoch, CurrentMetadataOffset: 0, WantFence: false, WantShutDown: false));

        Assert.Equal(ClusterRpcStatus.None, outcome.Status);
        Assert.True(outcome.IsCaughtUp);
        Assert.False(outcome.IsFenced);
        Assert.False(service.IsBrokerFenced(1));
    }

    [Fact]
    public void Register_NativePeer_RaisesFinalizedLevel()
    {
        var (service, state) = NewService();

        service.Register(Registration(1, Guid.NewGuid(), level: InterBrokerProtocolFeature.Native));
        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);

        // A KafkaWire peer joining drops the finalized MIN back to KafkaWire.
        service.Register(Registration(2, Guid.NewGuid(), level: InterBrokerProtocolFeature.KafkaWire));
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.FinalizedInterBrokerProtocol);
    }
}
