using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// A broker asking its own failure detector whether it is alive used to be told "no".
///
/// <para>The health map holds peers only — deliberately, because a record for the local broker
/// would be timed out by the monitor loop and reported as the broker's own failure — and
/// <see cref="HeartbeatManager.IsBrokerAlive"/> was a plain lookup into it. So the one broker whose
/// liveness is certain was the one broker the detector called dead, and every caller asking "is this
/// replica alive" about a replica that happened to be itself got the wrong answer.</para>
///
/// <para>These tests pin the two places where that answer decides something: the unclean election
/// that keeps a partition alive when its ISR has drained, and the preferred-leader rebalance.</para>
/// </summary>
public class LocalBrokerLivenessTests
{
    private const int LocalBroker = 1;
    private const int Peer = 2;

    [Fact]
    public void IsBrokerAlive_AskedAboutItself_SaysYes()
    {
        var state = new ClusterState();
        var config = NewConfig();
        var heartbeats = new HeartbeatManager(NullLogger<HeartbeatManager>.Instance, state, config);

        Assert.True(heartbeats.IsBrokerAlive(LocalBroker), "the broker running this code is alive");
    }

    [Fact]
    public void IsBrokerAlive_AskedAboutAnUnobservedPeer_StillSaysNo()
    {
        // The local answer is certain; a peer's is not. "Never heard from" must not be upgraded to
        // "alive", or the change would silently make every unknown broker look healthy.
        var state = new ClusterState();
        var heartbeats = new HeartbeatManager(
            NullLogger<HeartbeatManager>.Instance, state, NewConfig());

        Assert.False(heartbeats.IsBrokerAlive(Peer));
    }

    [Fact]
    public async Task UncleanElection_LastSurvivingReplicaIsTheLocalBroker_ElectsIt()
    {
        // The partition that this actually saves: ISR drained to nothing, the local broker still
        // holds a replica, no peer is reachable. The unclean branch scans the replicas for a live
        // one — and used to skip the only live broker there is, hard-fail the election, and leave
        // the partition without a leader with nothing scheduled to try again.
        var config = NewConfig();
        var (controller, state) = NewController(config);
        var tp = new TopicPartition { Topic = "drained", Partition = 0 };

        state.AddBroker(NewBroker(LocalBroker));
        state.AddBroker(NewBroker(Peer));
        state.AssignReplicas(tp, [Peer, LocalBroker]);
        state.ElectLeader(tp, Peer);
        state.UpdateIsr(tp, []);

        var heartbeats = new HeartbeatManager(NullLogger<HeartbeatManager>.Instance, state, config);
        controller.SetHeartbeatManager(heartbeats);
        await controller.StartAsync(CancellationToken.None);
        Assert.True(controller.IsController, "the lowest live broker id should hold the role");

        Assert.True(await controller.ElectLeaderAsync(tp),
            "the election failed although a live replica exists — the local broker was skipped");
        Assert.Equal(LocalBroker, state.GetPartitionState(tp)!.LeaderBrokerId);
    }

    [Fact]
    public async Task UncleanElection_ReplicaOrderPrefersTheLocalBroker_ElectsItOverALivePeer()
    {
        // The other half of the same change, and the one that alters an existing outcome rather
        // than rescuing a stuck one: replicas are preference-ordered, so when the local broker comes
        // first it should win the unclean election instead of being passed over for a live peer.
        var config = NewConfig();
        var (controller, state) = NewController(config);
        var tp = new TopicPartition { Topic = "preference", Partition = 0 };

        state.AddBroker(NewBroker(LocalBroker));
        state.AddBroker(NewBroker(Peer));
        state.AssignReplicas(tp, [LocalBroker, Peer]);
        state.ElectLeader(tp, Peer);
        state.UpdateIsr(tp, []);

        var heartbeats = new HeartbeatManager(NullLogger<HeartbeatManager>.Instance, state, config);
        heartbeats.ProcessHeartbeat(new HeartbeatRequest(Peer, 0, 0, LocalBroker, 0));
        Assert.True(heartbeats.IsBrokerAlive(Peer), "the peer must be live for this to prove anything");

        controller.SetHeartbeatManager(heartbeats);
        await controller.StartAsync(CancellationToken.None);

        Assert.True(await controller.ElectLeaderAsync(tp));
        Assert.Equal(LocalBroker, state.GetPartitionState(tp)!.LeaderBrokerId);
    }

    [Fact(Timeout = 60_000)]
    public async Task PreferredLeaderRebalance_PreferredLeaderIsTheLocalBroker_MovesLeadershipBack()
    {
        // Preferred-leader rebalance skips partitions whose preferred leader is not alive. With the
        // local broker answering "dead" about itself, leadership could move away from it and never
        // come back — the controller is routinely Replicas[0], so this is the ordinary case.
        //
        // This has to go through the controller loop and wait out its interval. Calling
        // ElectLeaderAsync(tp, preferredLeader) directly would prove nothing: that path takes the
        // preferred leader straight from the ISR without ever consulting liveness, so it passes
        // either way. The liveness question is asked only by the loop's skip guard.
        var config = NewConfig();
        var (controller, state) = NewController(config);
        var tp = new TopicPartition { Topic = "preferred", Partition = 0 };

        state.AddBroker(NewBroker(LocalBroker));
        state.AddBroker(NewBroker(Peer));
        state.AssignReplicas(tp, [LocalBroker, Peer]);
        state.ElectLeader(tp, Peer);
        state.UpdateIsr(tp, [LocalBroker, Peer]);

        var heartbeats = new HeartbeatManager(NullLogger<HeartbeatManager>.Instance, state, config);
        controller.SetHeartbeatManager(heartbeats);
        await controller.StartAsync(CancellationToken.None);

        Assert.True(config.AllowAutoLeaderRebalance, "the rebalance under test is disabled");
        Assert.Equal(LocalBroker, state.GetPartitionState(tp)!.PreferredLeader);
        Assert.Equal(Peer, state.GetPartitionState(tp)!.LeaderBrokerId);

        // The loop's interval is max(5, RebalanceCheckIntervalSeconds) seconds; give it a few
        // rounds before concluding it will never act.
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline &&
               state.GetPartitionState(tp)!.LeaderBrokerId != LocalBroker)
        {
            await Task.Delay(200);
        }

        Assert.Equal(LocalBroker, state.GetPartitionState(tp)!.LeaderBrokerId);
    }

    [Fact]
    public async Task ControllerElection_StrayHealthRecordForOurOwnId_DoesNotDisqualifyUs()
    {
        // ProcessHeartbeat keys on whatever broker id the sender claims, so a misconfigured peer can
        // plant a record under the local id. Our own liveness must not be lookup-dependent.
        var config = NewConfig();
        var (controller, state) = NewController(config);

        state.AddBroker(NewBroker(LocalBroker));
        state.AddBroker(NewBroker(Peer));

        var heartbeats = new HeartbeatManager(NullLogger<HeartbeatManager>.Instance, state, config);
        heartbeats.ProcessHeartbeat(new HeartbeatRequest(LocalBroker, 0, 0, LocalBroker, 0));
        heartbeats.GetBrokerHealth(LocalBroker)!.MarkFailed();

        controller.SetHeartbeatManager(heartbeats);
        await controller.StartAsync(CancellationToken.None);

        Assert.True(controller.IsController,
            "a health record claiming we are dead took us out of our own election");
    }

    private static ClusteringConfig NewConfig() => new()
    {
        BrokerId = LocalBroker,
        Host = "localhost",
        Port = 9092,
        ReplicationPort = 10092,
        RebalanceCheckIntervalSeconds = 5,
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
