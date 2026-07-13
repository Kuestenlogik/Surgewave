using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc6a — the per-partition leader-epoch ordering guard and the atomic broker update. Beyond the
/// controller-epoch fence (<see cref="ControllerEpochFenceTests"/>),
/// <see cref="ClusterState.TryApplyControllerPartitionState"/> / <see cref="ClusterState.ShouldStopReplica"/>
/// order partition content per partition (so disjoint partial pushes don't fence each other out), and
/// <see cref="ClusterState.UpdateBroker(int,BrokerNode,System.Func{BrokerNode,BrokerNode})"/> merges a
/// broker without a lost-update window against a concurrent registration.
/// </summary>
public class ControllerPushOrderingTests
{
    private static readonly TopicPartition Tp = new() { Topic = "orders", Partition = 0 };

    [Fact]
    public void ApplyPartitionState_NewerOrEqualLeaderEpoch_IsApplied()
    {
        var state = new ClusterState();
        Assert.True(state.TryApplyControllerPartitionState(Tp, leaderId: 4, leaderEpoch: 5, replicas: [4], isr: [4]));
        // Equal epoch re-applies (idempotent re-push).
        Assert.True(state.TryApplyControllerPartitionState(Tp, leaderId: 4, leaderEpoch: 5, replicas: [4, 1], isr: [4]));
        // Newer epoch applies.
        Assert.True(state.TryApplyControllerPartitionState(Tp, leaderId: 1, leaderEpoch: 6, replicas: [1], isr: [1]));

        var s = state.GetPartitionState(Tp)!;
        Assert.Equal(1, s.LeaderBrokerId);
        Assert.Equal(6, s.LeaderEpoch);
    }

    [Fact]
    public void ApplyPartitionState_OlderLeaderEpoch_IsSkippedWithoutRegressing()
    {
        var state = new ClusterState();
        state.TryApplyControllerPartitionState(Tp, leaderId: 4, leaderEpoch: 9, replicas: [4], isr: [4]);

        // A delayed/reordered entry at an older leader epoch must not regress the state.
        Assert.False(state.TryApplyControllerPartitionState(Tp, leaderId: 1, leaderEpoch: 2, replicas: [1], isr: [1]));

        var s = state.GetPartitionState(Tp)!;
        Assert.Equal(4, s.LeaderBrokerId);
        Assert.Equal(9, s.LeaderEpoch);
    }

    [Fact]
    public void ApplyPartitionState_DisjointPartitions_DoNotFenceEachOther()
    {
        // The regression this guards: a low-epoch update to partition B must apply even after a
        // high-epoch update to partition A (a per-PUSH version fence would drop B).
        var state = new ClusterState();
        var a = new TopicPartition { Topic = "orders", Partition = 0 };
        var b = new TopicPartition { Topic = "orders", Partition = 1 };

        Assert.True(state.TryApplyControllerPartitionState(a, 4, 9, [4], [4]));
        Assert.True(state.TryApplyControllerPartitionState(b, 5, 2, [5], [5])); // lower epoch, different partition

        Assert.Equal(4, state.GetPartitionState(a)!.LeaderBrokerId);
        Assert.Equal(5, state.GetPartitionState(b)!.LeaderBrokerId);
    }

    [Fact]
    public void ShouldStopReplica_OrdersByLeaderEpoch()
    {
        var state = new ClusterState();
        state.TryApplyControllerPartitionState(Tp, leaderId: 4, leaderEpoch: 6, replicas: [4], isr: [4]);

        Assert.True(state.ShouldStopReplica(Tp, leaderEpoch: 6));   // same epoch honored
        Assert.True(state.ShouldStopReplica(Tp, leaderEpoch: 7));   // newer honored
        Assert.False(state.ShouldStopReplica(Tp, leaderEpoch: 5));  // stale stop refused (re-assigned)
        Assert.True(state.ShouldStopReplica(Tp, leaderEpoch: -1));  // v0-2 wire (no epoch) always honored
        Assert.True(state.ShouldStopReplica(new TopicPartition { Topic = "ghost", Partition = 0 }, 3)); // unknown partition
    }

    // ── UpdateBroker CAS ─────────────────────────────────────────────────────

    [Fact]
    public void UpdateBroker_UnknownBroker_AddsIfAbsent()
    {
        var state = new ClusterState();
        var node = new BrokerNode { BrokerId = 7, Host = "h", Port = 9092, ReplicationPort = 10999, InterBrokerProtocol = 1 };

        var result = state.UpdateBroker(7, node, existing => existing with { InterBrokerProtocol = 0 });

        Assert.Equal(node, result);
        Assert.Equal(10999, state.GetBroker(7)!.ReplicationPort);
        Assert.Equal((short)1, state.GetBroker(7)!.InterBrokerProtocol);
    }

    [Fact]
    public void UpdateBroker_KnownBroker_MutatesInPlace()
    {
        var state = new ClusterState();
        state.AddBroker(new BrokerNode { BrokerId = 7, Host = "h", Port = 9092, ReplicationPort = 12345 });

        state.UpdateBroker(7, new BrokerNode { BrokerId = 7, Host = "x", Port = 1 },
            existing => existing with { InterBrokerProtocol = 1 });

        var node = state.GetBroker(7)!;
        Assert.Equal(12345, node.ReplicationPort); // endpoint preserved, not clobbered by ifAbsent
        Assert.Equal((short)1, node.InterBrokerProtocol);
    }

    [Fact]
    public async Task UpdateBroker_ConcurrentWithRegistration_DoesNotLoseThePort()
    {
        // Model the #69 hazard: a level-convergence UpdateBroker racing an authoritative full-node
        // registration (AddBroker) on the same id. Whatever order they run, the final node must have
        // BOTH the real replication port AND the converged level — never a stale-read clobber.
        for (var iteration = 0; iteration < 200; iteration++)
        {
            var state = new ClusterState();
            state.AddBroker(new BrokerNode { BrokerId = 7, Host = "old", Port = 9092, ReplicationPort = 11111 });

            var register = Task.Run(() => state.AddBroker(
                new BrokerNode { BrokerId = 7, Host = "new", Port = 9092, ReplicationPort = 22222, InterBrokerProtocol = 1 }));
            var converge = Task.Run(() => state.UpdateBroker(7,
                new BrokerNode { BrokerId = 7, Host = "new", Port = 9092, ReplicationPort = 22222 },
                existing => existing with { InterBrokerProtocol = 1 }));

            await Task.WhenAll(register, converge);

            var node = state.GetBroker(7)!;
            // Under _brokerLock the two ops serialize, and both serial orders end with the
            // registration's real port 22222 and level 1. The lost-update bug (unlocked read-then-
            // write interleave) would instead clobber the port back to the stale-read 11111 — so a
            // strict == 22222 is what actually catches a regression.
            Assert.Equal(22222, node.ReplicationPort);
            Assert.Equal((short)1, node.InterBrokerProtocol);
        }
    }
}
