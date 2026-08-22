using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// How a broker takes and gives up the controller role outside Raft.
///
/// <para>Taking it was covered by the integration failover tests. Giving it up was not covered
/// anywhere, because nothing could ever elect a second controller: <c>_isController</c> is cleared
/// only on the two Raft paths, so a legacy-mode broker that won the lowest-id election believed it
/// held the role forever. Wiring a failure detector into the shipped broker (#121) makes a second
/// election reachable — and with it a broker that keeps serving topic creation and leader elections
/// against its own view while another broker does the same.</para>
/// </summary>
public class ControllerRoleTransitionTests
{
    [Fact]
    public async Task AnAcceptedPushNamingAnotherController_MakesUsStepDown()
    {
        var (controller, state) = NewController(NewConfig(brokerId: 1));
        state.AddBroker(NewBroker(1));
        state.AddBroker(NewBroker(2));

        await controller.StartAsync(CancellationToken.None);
        Assert.True(controller.IsController, "broker 1 should win the lowest-id election");

        // What a real push does after a peer took over: it passes the epoch fence carrying the new
        // controller's id. That is the only signal a legacy-mode broker ever gets.
        Assert.True(state.TryAdvanceControllerEpoch(controllerId: 2, controllerEpoch: state.ControllerEpoch + 1));

        Assert.False(controller.IsController, "we were replaced and kept acting as controller");
        Assert.Equal(2, state.ControllerId);
    }

    [Fact]
    public async Task AnAcceptedPushNamingUs_DoesNotMakeUsStepDown()
    {
        // Self-delivered pushes exist, and a controller must not demote itself on its own epoch.
        var (controller, state) = NewController(NewConfig(brokerId: 1));
        state.AddBroker(NewBroker(1));
        state.AddBroker(NewBroker(2));

        await controller.StartAsync(CancellationToken.None);
        Assert.True(controller.IsController);

        Assert.True(state.TryAdvanceControllerEpoch(controllerId: 1, controllerEpoch: state.ControllerEpoch + 1));

        Assert.True(controller.IsController);
    }

    [Fact]
    public async Task InRaftMode_APushDoesNotDemoteUs()
    {
        // Raft owns the role there: the leader watch sets and clears _isController, and a push-driven
        // step-down would fight it. Pinned so the legacy mechanism stays legacy-only.
        var config = NewConfig(brokerId: 1);
        config.UseRaftConsensus = true;
        var (controller, state) = NewController(config);
        state.AddBroker(NewBroker(1));
        state.AddBroker(NewBroker(2));

        await controller.StartAsync(CancellationToken.None);
        Assert.True(controller.IsController);

        Assert.True(state.TryAdvanceControllerEpoch(controllerId: 2, controllerEpoch: state.ControllerEpoch + 1));

        Assert.True(controller.IsController);
    }

    [Fact(Timeout = 60_000)]
    public async Task WhenTheControllerIsDetectedDead_AFollowerTakesTheRole()
    {
        // The unit-level counterpart of the integration failover test: the whole chain from the
        // health monitor marking a peer dead through to the re-election, without two real brokers.
        var config = NewConfig(brokerId: 2);
        var (controller, state) = NewController(config);
        state.AddBroker(NewBroker(1));
        state.AddBroker(NewBroker(2));

        await controller.StartAsync(CancellationToken.None);
        Assert.False(controller.IsController, "broker 1 has the lower id and no failure is known yet");
        Assert.Equal(1, state.ControllerId);

        await using var heartbeats = new HeartbeatManager(
            NullLogger<HeartbeatManager>.Instance, state, config);
        controller.SetHeartbeatManager(heartbeats);
        await heartbeats.StartAsync(CancellationToken.None);

        Assert.True(await WaitFor(() => controller.IsController, TestContext.Current.CancellationToken),
            "the survivor never took the role after broker 1 was declared dead");
    }

    [Fact(Timeout = 60_000)]
    public async Task InRaftMode_ADetectedFailureDoesNotRunTheLegacyElection()
    {
        // Running it would contradict Raft and start a second controller loop next to the one the
        // leader watch already owns.
        var config = NewConfig(brokerId: 2);
        config.UseRaftConsensus = true;
        var (controller, state) = NewController(config);
        state.AddBroker(NewBroker(1));
        state.AddBroker(NewBroker(2));

        await controller.StartAsync(CancellationToken.None);
        Assert.False(controller.IsController);

        await using var heartbeats = new HeartbeatManager(
            NullLogger<HeartbeatManager>.Instance, state, config);
        controller.SetHeartbeatManager(heartbeats);
        await heartbeats.StartAsync(CancellationToken.None);

        // Assert the failure was actually detected first — otherwise "still not controller" would
        // pass for the wrong reason.
        Assert.True(
            await WaitFor(() => heartbeats.GetBrokerHealth(1) is { IsAlive: false }, TestContext.Current.CancellationToken),
            "broker 1 was never declared dead, so this proves nothing");
        Assert.False(controller.IsController, "the legacy election ran in Raft mode");
    }

    private static async Task<bool> WaitFor(Func<bool> condition, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (DateTime.UtcNow < deadline && !cancellationToken.IsCancellationRequested)
        {
            if (condition()) return true;
            await Task.Delay(100, cancellationToken);
        }
        return condition();
    }

    private static ClusteringConfig NewConfig(int brokerId) => new()
    {
        BrokerId = brokerId,
        Host = "localhost",
        Port = 9092 + brokerId,
        ReplicationPort = 10092 + brokerId,
        RebalanceCheckIntervalSeconds = 5,
        // Short enough that the monitor loop reaches its verdict inside the test. The peer is not
        // listening at all, so the send loop fails immediately rather than waiting out a timeout.
        HeartbeatIntervalMs = 200,
        HeartbeatTimeoutMs = 500,
    };

    private static BrokerNode NewBroker(int brokerId) => new()
    {
        BrokerId = brokerId,
        Host = "localhost",
        Port = 9092 + brokerId,
        ReplicationPort = 10092 + brokerId,
    };

    private static (ClusterController Controller, ClusterState State) NewController(ClusteringConfig config)
    {
        var state = new ClusterState();
        var logs = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        var replicaManager = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, state, logs, config,
            new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());
        var controller = new ClusterController(
            NullLogger<ClusterController>.Instance, state, replicaManager, config);
        return (controller, state);
    }
}
