using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// The half the metadata log apply was missing (#165): a committed leadership change has to
/// reach this broker's replica manager, not only its cluster state.
/// </summary>
/// <remarks>
/// The routing rule is asserted directly on <c>ReplicationTransitionQueue.Route</c>. Going
/// through the queue would need either a replica manager against real storage or a stand-in
/// that restates the rule — and a test that restates the rule tests itself.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class ReplicationTransitionQueueTests
{
    private const int LocalBroker = 1;
    private static readonly TopicPartition Tp = new() { Topic = "orders", Partition = 0 };

    [Fact]
    public void APartitionWeNeitherLeadNorReplicateImpliesNoTransition()
    {
        // Most partitions, in a cluster of any size. Each one must not cost a queued
        // transition on every broker.
        var routed = ReplicationTransitionQueue.Route(
            Tp, leaderBrokerId: 2, leaderEpoch: 5, replicas: [2, 3], localBrokerId: LocalBroker);

        Assert.Null(routed);
    }

    [Fact]
    public void BecomingTheLeaderRoutesToTheLeaderTransition()
    {
        var routed = ReplicationTransitionQueue.Route(
            Tp, leaderBrokerId: LocalBroker, leaderEpoch: 5, replicas: [LocalBroker, 2], localBrokerId: LocalBroker);

        Assert.NotNull(routed);
        Assert.Equal(-1, routed!.Value.LeaderBrokerId); // -1 = we lead it
        Assert.Equal(5, routed.Value.LeaderEpoch);
    }

    [Fact]
    public void BeingAReplicaRoutesToFollowTheNewLeader()
    {
        var routed = ReplicationTransitionQueue.Route(
            Tp, leaderBrokerId: 2, leaderEpoch: 5, replicas: [LocalBroker, 2], localBrokerId: LocalBroker);

        Assert.NotNull(routed);
        Assert.Equal(2, routed!.Value.LeaderBrokerId);
        Assert.Equal(5, routed.Value.LeaderEpoch);
    }

    [Fact]
    public void LeadingWithoutBeingListedAsAReplicaStillRoutes()
    {
        // Leadership wins over the replica list: a broker told it is the leader takes over
        // even if the assignment entry it has is older and does not list it yet. The
        // alternative — refusing — leaves the partition with no leader acting at all.
        var routed = ReplicationTransitionQueue.Route(
            Tp, leaderBrokerId: LocalBroker, leaderEpoch: 5, replicas: [2, 3], localBrokerId: LocalBroker);

        Assert.NotNull(routed);
        Assert.Equal(-1, routed!.Value.LeaderBrokerId);
    }

    [Fact]
    public void WithoutAQueueTheApplyStillUpdatesClusterState()
    {
        // Null queue is the case for a host applying the log without a replica manager, and
        // for every existing test that builds a state machine. It must not become a
        // null-reference on the apply path.
        var state = new ClusterState();
        state.SetPartitionState(Tp, new PartitionState { TopicPartition = Tp, Replicas = [LocalBroker] });
        var machine = NewStateMachine(state);

        machine.Apply(LeaderChangedEntry(newLeader: LocalBroker, leaderEpoch: 4, index: 3));

        Assert.Equal(LocalBroker, state.GetPartitionState(Tp)!.LeaderBrokerId);
    }

    [Fact]
    public void TheApplyReadsTheReplicaSetFromClusterState()
    {
        // LeaderChanged carries only the new leader — the assignment was committed
        // separately — so the apply has to look the replicas up. Pinned because getting it
        // wrong makes every follower transition silently not happen: the routing would see
        // an empty replica list and decide the partition is none of our business.
        var state = new ClusterState();
        state.SetPartitionState(Tp, new PartitionState
        {
            TopicPartition = Tp,
            LeaderBrokerId = 2,
            Replicas = [LocalBroker, 2],
        });
        var machine = NewStateMachine(state);

        machine.Apply(LeaderChangedEntry(newLeader: 2, leaderEpoch: 9, index: 12));

        var partition = state.GetPartitionState(Tp)!;
        Assert.Equal(2, partition.LeaderBrokerId);
        Assert.Contains(LocalBroker, partition.Replicas);

        // And the rule that apply would have handed to the queue.
        var routed = ReplicationTransitionQueue.Route(Tp, 2, 9, partition.Replicas, LocalBroker);
        Assert.NotNull(routed);
        Assert.Equal(2, routed!.Value.LeaderBrokerId);
    }

    private static RaftLogEntry LeaderChangedEntry(int newLeader, int leaderEpoch, long index) => new()
    {
        Index = index,
        Term = 1,
        CommandType = MetadataCommandType.LeaderChanged,
        Data = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(
            new LeaderChangedCommand(Tp.Topic, Tp.Partition, newLeader, leaderEpoch),
            ClusteringJsonContext.Default.LeaderChangedCommand),
    };

    private static MetadataStateMachine NewStateMachine(ClusterState state)
    {
        var config = new ClusteringConfig { BrokerId = LocalBroker };
        var membership = new ClusterMembershipService(
            new ClusterIdManager(config, NullLogger<ClusterIdManager>.Instance),
            state,
            NullLogger<ClusterMembershipService>.Instance);

        return new MetadataStateMachine(
            NullLogger<MetadataStateMachine>.Instance, state, membership, transitions: null);
    }
}
