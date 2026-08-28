using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// The leadership view the produce path refuses on (#164).
/// </summary>
/// <remarks>
/// The question is deliberately "does another broker lead this" and not "am I the
/// leader". A single-broker or embedded runtime has no partition states at all — they are
/// written only by clustering paths — so the second phrasing would answer no there and
/// refuse every write. These tests pin that distinction, because getting it wrong breaks
/// the deployments that have no cluster rather than the ones that do.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class PartitionLeadershipTests
{
    private const int LocalBroker = 1;
    private static readonly TopicPartition Tp = new() { Topic = "orders", Partition = 0 };

    private static (ClusterStatePartitionLeadership Leadership, ClusterState State) New()
    {
        var state = new ClusterState();
        return (new ClusterStatePartitionLeadership(state, LocalBroker), state);
    }

    [Fact]
    public void WithoutAnyPartitionState_NothingIsLedElsewhere()
    {
        // The single-broker and embedded case. No clustering path has ever written a
        // partition state, so the produce path must keep appending exactly as before.
        var (leadership, _) = New();

        Assert.False(leadership.IsLedByAnotherBroker(Tp));
    }

    [Fact]
    public void APartitionLedByAnotherBrokerIsRefused()
    {
        var (leadership, state) = New();
        state.SetPartitionState(Tp, new PartitionState { TopicPartition = Tp, LeaderBrokerId = 2 });

        Assert.True(leadership.IsLedByAnotherBroker(Tp));
    }

    [Fact]
    public void APartitionWeLeadIsNotRefused()
    {
        var (leadership, state) = New();
        state.SetPartitionState(Tp, new PartitionState { TopicPartition = Tp, LeaderBrokerId = LocalBroker });

        Assert.False(leadership.IsLedByAnotherBroker(Tp));
    }

    [Fact]
    public void APartitionMidElectionIsNotRefused()
    {
        // LeaderBrokerId -1 is "no leader yet", not "led elsewhere". Refusing here would
        // turn every election into a client-visible error, including for the broker that
        // is about to win it.
        var (leadership, state) = New();
        state.SetPartitionState(Tp, new PartitionState { TopicPartition = Tp, LeaderBrokerId = -1 });

        Assert.False(leadership.IsLedByAnotherBroker(Tp));
    }

    [Fact]
    public void AnUnrelatedPartitionOfTheSameTopicIsJudgedOnItsOwn()
    {
        // Leadership is per partition, so a topic can have some partitions here and some
        // elsewhere — the common case in any real cluster.
        var (leadership, state) = New();
        var other = new TopicPartition { Topic = "orders", Partition = 1 };
        state.SetPartitionState(Tp, new PartitionState { TopicPartition = Tp, LeaderBrokerId = LocalBroker });
        state.SetPartitionState(other, new PartitionState { TopicPartition = other, LeaderBrokerId = 2 });

        Assert.False(leadership.IsLedByAnotherBroker(Tp));
        Assert.True(leadership.IsLedByAnotherBroker(other));
    }
}
