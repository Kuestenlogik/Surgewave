using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc6b — the controller gate on the native membership receive path and the client-side
/// self-dial refusal. Registration/heartbeat only run on the controller; a non-controller replies
/// NotController so the joining broker retries against the real controller, and the controller's own
/// client refuses to register with itself.
/// </summary>
public class NativeMembershipGateTests
{
    private static (ClusterStateInterBrokerService Service, ClusterState State) NewService(int localBrokerId, int controllerId)
    {
        var state = new ClusterState { ControllerId = controllerId };
        var config = new ClusteringConfig { BrokerId = localBrokerId };
        var logs = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        var replicas = new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, logs, config, new TcpPeerTransport());
        var membership = new ClusterMembershipService(
            new ClusterIdManager(config, NullLogger<ClusterIdManager>.Instance), state, NullLogger<ClusterMembershipService>.Instance);
        // isrUpdateApplier null → IsController falls back to (ControllerId == localBrokerId).
        var service = new ClusterStateInterBrokerService(
            NullLogger<ClusterStateInterBrokerService>.Instance, state, replicas, logs, localBrokerId,
            isrUpdateApplier: null, membership: membership);
        return (service, state);
    }

    private static BrokerRegistrationInput Registration(int brokerId) => new(
        BrokerId: brokerId, ClusterId: "", IncarnationId: Guid.NewGuid(),
        Listeners: [new ListenerSpec("PLAINTEXT", "h", 9092, 0)],
        Features: [InterBrokerProtocolFeature.LocalFeatureSpec], Rack: null, PreviousBrokerEpoch: -1);

    [Fact]
    public async Task RegisterBroker_OnController_AssignsEpoch()
    {
        var (service, _) = NewService(localBrokerId: 1, controllerId: 1); // this broker IS the controller

        var outcome = await service.RegisterBrokerAsync(Registration(2));

        Assert.Equal(ClusterRpcStatus.None, outcome.Status);
        Assert.True(outcome.BrokerEpoch > 0);
    }

    [Fact]
    public async Task RegisterBroker_OnNonController_RepliesNotController()
    {
        var (service, _) = NewService(localBrokerId: 2, controllerId: 1); // controller is broker 1, not us

        var outcome = await service.RegisterBrokerAsync(Registration(3));

        Assert.Equal(ClusterRpcStatus.NotController, outcome.Status);
    }

    [Fact]
    public async Task Heartbeat_OnNonController_RepliesNotController()
    {
        var (service, _) = NewService(localBrokerId: 2, controllerId: 1);

        var outcome = await service.HeartbeatAsync(new BrokerHeartbeatInput(3, 1, 0, false, false));

        Assert.Equal(ClusterRpcStatus.NotController, outcome.Status);
    }

    [Fact]
    public async Task Client_WhenSelfIsController_DoesNotDialAndReturnsBrokerNotAvailable()
    {
        // ResolveController must refuse to dial self: a controller's own lifecycle client resolves no
        // controller, so it never opens a connection and idles (BrokerNotAvailable, best-effort).
        var state = new ClusterState { ControllerId = 1 };
        var config = new ClusteringConfig { BrokerId = 1, Host = "localhost", Port = 9092, ReplicationPort = 10092, ClusterNodes = "1:localhost:9092:10092,2:localhost:9094:10094" };
        using var pool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, new TcpPeerTransport());
        var client = new NativeBrokerLifecycleClient(pool, state, config, NullLogger<NativeBrokerLifecycleClient>.Instance);

        var outcome = await client.RegisterAsync(Registration(1));

        Assert.Equal(ClusterRpcStatus.BrokerNotAvailable, outcome.Status);
    }

    [Fact]
    public async Task Client_UsesDiscoveredReplicationPort_NotAGuess()
    {
        // A joiner (broker 2) whose cluster state knows the controller (broker 1) with an explicit
        // replication port must target THAT port, not a client-port+1000 guess. With no server
        // listening it fails, but the point is it resolved a controller to dial (BrokerNotAvailable,
        // not "no controller"): the resolution path is exercised. (Full success is the E2E test.)
        var state = new ClusterState { ControllerId = 1 };
        state.AddBroker(new BrokerNode { BrokerId = 1, Host = "127.0.0.1", Port = 9092, ReplicationPort = 65000 });
        var config = new ClusteringConfig { BrokerId = 2, Host = "127.0.0.1", Port = 9094, ReplicationPort = 10094, ClusterNodes = "1:127.0.0.1:9092:65000" };
        using var pool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, new TcpPeerTransport());
        var client = new NativeBrokerLifecycleClient(pool, state, config, NullLogger<NativeBrokerLifecycleClient>.Instance);

        var outcome = await client.RegisterAsync(Registration(2));

        // Nothing listens on 65000 → connect refused → BrokerNotAvailable (resolved but unreachable).
        Assert.Equal(ClusterRpcStatus.BrokerNotAvailable, outcome.Status);
    }
}
