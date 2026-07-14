using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #72 Inc1 — controller-wire convergence. Two coupled fixes exercised together, on the SAME
/// <see cref="ClusterState"/>, over both wires:
/// (1) the Kafka-wire UpdateMetadata handler must MERGE known broker nodes instead of rebuilding
/// them (the old unconditional RegisterBroker reset every listed broker's inter.broker.protocol
/// level to KafkaWire — including the receiver's OWN node, permanently — and clobbered explicitly
/// discovered ReplicationPorts, #69-class);
/// (2) because that clobber was accidentally load-bearing for rolling DOWNGRADES (the Kafka wire
/// carries no per-broker level), downgrade convergence is now structural: a fence-passing REMOTE
/// Kafka-wire controller push caps <see cref="ClusterState.FinalizedInterBrokerProtocol"/>
/// (<see cref="ClusterState.TryAdvanceControllerEpoch(int,int,ControllerPushWire?)"/>), and a
/// native push under a strictly newer controller epoch clears the cap.
/// </summary>
[Trait("Category", TestCategories.Integration)]
public sealed class ControllerWireConvergenceTests
{
    private const int LocalBrokerId = 0;

    private static (InterBrokerApiHandler Kafka, ClusterStateInterBrokerService Native, ClusterState State) NewNode(
        int brokerId = LocalBrokerId)
    {
        var state = new ClusterState();
        var logs = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        var replicas = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, state, logs, new ClusteringConfig { BrokerId = brokerId },
            new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());
        var kafka = new InterBrokerApiHandler(
            new BrokerConfig { BrokerId = brokerId }, state, replicas, logs, NullLogger<InterBrokerApiHandler>.Instance);
        var native = new ClusterStateInterBrokerService(
            NullLogger<ClusterStateInterBrokerService>.Instance, state, replicas, logs, brokerId);
        return (kafka, native, state);
    }

    private static readonly RequestContext Ctx =
        new() { ConnectionState = new ConnectionState("wire-convergence-test"), ClientId = "controller" };

    private static UpdateMetadataRequest KafkaUpdateMetadata(
        int controllerId, int controllerEpoch, params UpdateMetadataRequest.UpdateMetadataBroker[] liveBrokers) => new()
    {
        ApiKey = ApiKey.UpdateMetadata,
        ApiVersion = 6,
        CorrelationId = 1,
        ClientId = "controller",
        ControllerId = controllerId,
        ControllerEpoch = controllerEpoch,
        BrokerEpoch = -1,
        TopicStates = [],
        LiveBrokers = [.. liveBrokers],
    };

    private static UpdateMetadataRequest.UpdateMetadataBroker KafkaBroker(int id, string host, int port, string? rack = null) => new()
    {
        Id = id,
        Endpoints = [new UpdateMetadataRequest.UpdateMetadataEndpoint { Host = host, Port = port, SecurityProtocol = 0 }],
        Rack = rack,
    };

    private static PartitionStatesPayload NativeUpdateMetadata(
        int controllerId, int controllerEpoch, params LiveBrokerSpec[] liveBrokers)
        => new(controllerId, controllerEpoch, [.. liveBrokers], Entries: []);

    // ── The clobber fix: Kafka-wire UpdateMetadata merges, never rebuilds ───────────────────────

    [Fact]
    public async Task KafkaUpdateMetadata_KnownBroker_KeepsLevelAndExplicitReplicationPort_ConvergesEndpoint()
    {
        var (kafka, _, state) = NewNode();
        // Broker 2 registered natively: level Native, explicitly discovered replication port.
        state.AddBroker(new BrokerNode
        {
            BrokerId = 2, Host = "old-host", Port = 9092, ReplicationPort = 10999,
            InterBrokerProtocol = InterBrokerProtocolFeature.Native,
        });

        var response = (UpdateMetadataResponse)await kafka.HandleAsync(
            KafkaUpdateMetadata(controllerId: 1, controllerEpoch: 1, KafkaBroker(2, "new-host", 9095, rack: "r1")),
            Ctx, CancellationToken.None);

        Assert.Equal(ErrorCode.None, response.ErrorCode);
        var node = state.GetBroker(2)!;
        // Endpoint fields converged from the push …
        Assert.Equal("new-host", node.Host);
        Assert.Equal(9095, node.Port);
        Assert.Equal("r1", node.Rack);
        // … but the advertised protocol level and the explicit replication port survived
        // (the old RegisterBroker rebuild reset them to KafkaWire / Port + 1000).
        Assert.Equal(InterBrokerProtocolFeature.Native, node.InterBrokerProtocol);
        Assert.Equal(10999, node.ReplicationPort);
    }

    [Fact]
    public async Task KafkaUpdateMetadata_LocalNode_IsNeverTouched()
    {
        var (kafka, _, state) = NewNode();
        // Startup self-advertisement: the local node carries its own level and real ports.
        state.AddBroker(new BrokerNode
        {
            BrokerId = LocalBrokerId, Host = "self", Port = 9092, ReplicationPort = 10092,
            InterBrokerProtocol = InterBrokerProtocolFeature.Native,
        });

        await kafka.HandleAsync(
            KafkaUpdateMetadata(controllerId: 1, controllerEpoch: 1, KafkaBroker(LocalBrokerId, "not-me", 1)),
            Ctx, CancellationToken.None);

        var node = state.GetBroker(LocalBrokerId)!;
        // Self-advertisement is startup-only, so a clobber here would be PERMANENT — the push
        // must not rewrite the receiver's own node at all.
        Assert.Equal("self", node.Host);
        Assert.Equal(9092, node.Port);
        Assert.Equal(10092, node.ReplicationPort);
        Assert.Equal(InterBrokerProtocolFeature.Native, node.InterBrokerProtocol);
    }

    [Fact]
    public async Task KafkaUpdateMetadata_UnknownBroker_IsLearnedAtKafkaWireDefault()
    {
        var (kafka, _, state) = NewNode();

        await kafka.HandleAsync(
            KafkaUpdateMetadata(controllerId: 1, controllerEpoch: 1, KafkaBroker(7, "b7", 9097)),
            Ctx, CancellationToken.None);

        var node = state.GetBroker(7)!;
        Assert.Equal("b7", node.Host);
        Assert.Equal(9097, node.Port);
        // The Kafka wire carries no level — an unknown broker reads as KafkaWire (safety anchor).
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, node.InterBrokerProtocol);
    }

    // ── The downgrade convergence: observed controller wire caps the finalized level ────────────

    [Fact]
    public async Task FencePassingKafkaPush_CapsFinalized_NewerNativePushRestoresIt()
    {
        var (kafka, native, state) = NewNode();
        // Both brokers advertise Native — map MIN alone would finalize Native forever.
        state.AddBroker(new BrokerNode { BrokerId = LocalBrokerId, Host = "self", Port = 9092, InterBrokerProtocol = InterBrokerProtocolFeature.Native });
        state.AddBroker(new BrokerNode { BrokerId = 1, Host = "ctl", Port = 9093, InterBrokerProtocol = InterBrokerProtocolFeature.Native });
        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);

        // A fence-passing KAFKA-wire push from the remote controller proves the controller
        // finalized to the Kafka wire (rolling downgrade) — the local view must follow.
        await kafka.HandleAsync(KafkaUpdateMetadata(controllerId: 1, controllerEpoch: 1), Ctx, CancellationToken.None);
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.FinalizedInterBrokerProtocol);

        // A NATIVE push under a STRICTLY newer controller epoch (re-election after the peer
        // re-upgraded) proves the controller is back on the native wire — the cap clears.
        var status = await native.ApplyUpdateMetadataAsync(NativeUpdateMetadata(controllerId: 1, controllerEpoch: 2));
        Assert.Equal(ClusterRpcStatus.None, status);
        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);
    }

    [Fact]
    public async Task StaleKafkaPush_DoesNotCapFinalized()
    {
        var (kafka, native, state) = NewNode();
        state.AddBroker(new BrokerNode { BrokerId = LocalBrokerId, Host = "self", Port = 9092, InterBrokerProtocol = InterBrokerProtocolFeature.Native });
        state.AddBroker(new BrokerNode { BrokerId = 1, Host = "ctl", Port = 9093, InterBrokerProtocol = InterBrokerProtocolFeature.Native });

        // The native wire already advanced to controller 1 @ epoch 5.
        var status = await native.ApplyUpdateMetadataAsync(NativeUpdateMetadata(controllerId: 1, controllerEpoch: 5));
        Assert.Equal(ClusterRpcStatus.None, status);

        // A STALE Kafka-wire push (demoted controller, older epoch) is fenced — it proves nothing
        // about the current controller's wire and must not cap the finalized level.
        var response = (UpdateMetadataResponse)await kafka.HandleAsync(
            KafkaUpdateMetadata(controllerId: 9, controllerEpoch: 4), Ctx, CancellationToken.None);
        Assert.Equal(ErrorCode.StaleControllerEpoch, response.ErrorCode);
        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);
    }

    [Fact]
    public async Task DelayedSameEpochNativePush_DoesNotClearTheCap()
    {
        var (kafka, native, state) = NewNode();
        state.AddBroker(new BrokerNode { BrokerId = LocalBrokerId, Host = "self", Port = 9092, InterBrokerProtocol = InterBrokerProtocolFeature.Native });
        state.AddBroker(new BrokerNode { BrokerId = 1, Host = "ctl", Port = 9093, InterBrokerProtocol = InterBrokerProtocolFeature.Native });

        // The controller downgraded mid-reign: its Kafka-wire push arrives first and caps the level.
        await kafka.HandleAsync(KafkaUpdateMetadata(controllerId: 1, controllerEpoch: 3), Ctx, CancellationToken.None);
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.FinalizedInterBrokerProtocol);

        // A DELAYED native frame from the SAME controller epoch (dispatched before the downgrade,
        // still in flight) must NOT clear the cap: same-epoch frames carry no cross-wire ordering,
        // and clearing here would flip the level back UP and send native frames at a peer that can
        // no longer decode them — the unsafe error direction. The cap wins ties by construction.
        var status = await native.ApplyUpdateMetadataAsync(NativeUpdateMetadata(controllerId: 1, controllerEpoch: 3));
        Assert.Equal(ClusterRpcStatus.None, status);
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.FinalizedInterBrokerProtocol);

        // A native push under the NEXT controller epoch is unambiguous — the cap clears.
        await native.ApplyUpdateMetadataAsync(NativeUpdateMetadata(controllerId: 1, controllerEpoch: 4));
        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);
    }

    [Fact]
    public async Task BecomeController_ClearsTheCap()
    {
        var (kafka, _, state) = NewNode();
        state.AddBroker(new BrokerNode { BrokerId = LocalBrokerId, Host = "self", Port = 9092, InterBrokerProtocol = InterBrokerProtocolFeature.Native });
        state.AddBroker(new BrokerNode { BrokerId = 1, Host = "ctl", Port = 9093, InterBrokerProtocol = InterBrokerProtocolFeature.Native });

        // Capped as a follower by the (then Kafka-wire) controller …
        await kafka.HandleAsync(KafkaUpdateMetadata(controllerId: 1, controllerEpoch: 1), Ctx, CancellationToken.None);
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.FinalizedInterBrokerProtocol);

        // … then promoted to controller. The cap models a REMOTE controller's wire and must clear,
        // or the promoted broker would pin its own transport gate, re-cap every receiver with each
        // Kafka-wire push, and never receive the native push that could heal it — an absorbing state
        // in which a fully native-capable cluster stays on the Kafka wire forever (review BLOCKER).
        state.BecomeController(LocalBrokerId);
        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);
    }

    [Fact]
    public void AtomicFence_CapWinsTies_ClearRequiresNewerEpoch_NeverRaisesAboveMapMin()
    {
        var state = new ClusterState();
        state.AddBroker(new BrokerNode { BrokerId = 1, Host = "h", Port = 9092, InterBrokerProtocol = InterBrokerProtocolFeature.Native });

        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);

        Assert.True(state.TryAdvanceControllerEpoch(1, 1, ControllerPushWire.KafkaWire));
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.FinalizedInterBrokerProtocol);

        // Same-epoch native does NOT clear (ties are downward-safe) …
        Assert.True(state.TryAdvanceControllerEpoch(1, 1, ControllerPushWire.Native));
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.FinalizedInterBrokerProtocol);

        // … a strictly newer epoch does.
        Assert.True(state.TryAdvanceControllerEpoch(1, 2, ControllerPushWire.Native));
        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);

        // A fence-REJECTED Kafka push never caps (stale epoch).
        Assert.False(state.TryAdvanceControllerEpoch(9, 1, ControllerPushWire.KafkaWire));
        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);

        // The cap never RAISES the level above the map MIN: with a KafkaWire peer in the map,
        // an uncapped state still finalizes KafkaWire.
        state.AddBroker(new BrokerNode { BrokerId = 2, Host = "h2", Port = 9093 });
        Assert.True(state.TryAdvanceControllerEpoch(1, 3, ControllerPushWire.Native));
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.FinalizedInterBrokerProtocol);
    }

    // ── Upgrade re-convergence: the controller bumps its epoch when finalized rises to Native ───

    [Fact]
    public async Task RegistrationThatRaisesFinalizedToNative_BumpsControllerEpoch()
    {
        var state = new ClusterState();
        var logs = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        var replicas = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, state, logs, new ClusteringConfig { BrokerId = LocalBrokerId },
            new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());
        var membership = new ClusterMembershipService(
            new ClusterIdManager(new ClusteringConfig { BrokerId = LocalBrokerId }, NullLogger<ClusterIdManager>.Instance),
            state, NullLogger<ClusterMembershipService>.Instance);
        var native = new ClusterStateInterBrokerService(
            NullLogger<ClusterStateInterBrokerService>.Instance, state, replicas, logs, LocalBrokerId,
            membership: membership);

        // This broker is the controller (reign epoch 1); it advertises Native, but the old-build
        // peer 1 pins the finalized level to KafkaWire — the mixed window of a rolling upgrade.
        state.BecomeController(LocalBrokerId);
        var reignEpoch = state.ControllerEpoch;
        state.AddBroker(new BrokerNode { BrokerId = LocalBrokerId, Host = "self", Port = 9092, InterBrokerProtocol = InterBrokerProtocolFeature.Native });
        state.AddBroker(new BrokerNode { BrokerId = 1, Host = "peer", Port = 9093, InterBrokerProtocol = InterBrokerProtocolFeature.KafkaWire });
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, state.FinalizedInterBrokerProtocol);

        // Peer 1 finishes its upgrade and re-registers advertising Native: the finalized level rises
        // — WITHIN the same reign. Receivers capped by this reign's earlier Kafka-wire pushes only
        // clear on a strictly newer epoch, so the flip must mint one.
        var outcome = await native.RegisterBrokerAsync(new BrokerRegistrationInput(
            BrokerId: 1,
            ClusterId: "",
            IncarnationId: Guid.NewGuid(),
            Listeners: [new ListenerSpec("PLAINTEXT", "peer", 9093, 0)],
            Features: [new FeatureSpec(InterBrokerProtocolFeature.FeatureName, InterBrokerProtocolFeature.KafkaWire, InterBrokerProtocolFeature.Native)],
            Rack: null,
            PreviousBrokerEpoch: -1));

        Assert.Equal(ClusterRpcStatus.None, outcome.Status);
        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);
        Assert.True(state.ControllerEpoch > reignEpoch);
        Assert.Equal(LocalBrokerId, state.ControllerId);
    }

    // ── Down-convergence twin of the native ApplyLiveBrokers up-convergence test ────────────────

    [Fact]
    public async Task NativeLiveBrokers_ConvergeKnownLevelDown()
    {
        var (_, native, state) = NewNode(brokerId: 3);
        // Broker 5 is known at Native; the controller's registration-authoritative view now says it
        // re-registered as KafkaWire (rolling downgrade) — the level must converge DOWN while the
        // discovered endpoint survives.
        state.AddBroker(new BrokerNode
        {
            BrokerId = 5, Host = "cfg-host", Port = 9097, ReplicationPort = 12345,
            InterBrokerProtocol = InterBrokerProtocolFeature.Native,
        });

        var status = await native.ApplyUpdateMetadataAsync(NativeUpdateMetadata(
            controllerId: 1, controllerEpoch: 1,
            new LiveBrokerSpec(BrokerId: 5, Host: "push-host", Port: 1, ReplicationPort: 2,
                InterBrokerProtocol: InterBrokerProtocolFeature.KafkaWire, Rack: null)));

        Assert.Equal(ClusterRpcStatus.None, status);
        var node = state.GetBroker(5)!;
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, node.InterBrokerProtocol);
        Assert.Equal("cfg-host", node.Host);
        Assert.Equal(12345, node.ReplicationPort);
    }
}
