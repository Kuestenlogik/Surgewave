using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// Which broker coordinates a group (#204): Kafka's partition formula over <c>__consumer_offsets</c>, and that
/// partition's leader — the same answer on every broker, so the members of one group meet at one coordinator.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class CoordinatorLocatorTests
{
    [Theory]
    [InlineData("", 0)]
    [InlineData("abc", 96354)]
    [InlineData("hello", 99162322)]
    [InlineData("polygenelubricants", int.MinValue)] // Java's well-known string whose hashCode is Integer.MIN_VALUE
    public void TheHashIsJavasStringHashCode(string key, int javaHashCode)
    {
        Assert.Equal(javaHashCode, CoordinatorPartitioner.JavaStringHashCode(key));
    }

    [Fact]
    public void ThePartitionIsKafkasAbsOfTheHashModuloTheCount()
    {
        // Utils.abs(Integer.MIN_VALUE) is 0 in Kafka, not an overflow.
        Assert.Equal(0, CoordinatorPartitioner.PartitionFor("polygenelubricants", 50));
        Assert.Equal(99162322 % 50, CoordinatorPartitioner.PartitionFor("hello", 50));
        Assert.All(Enumerable.Range(0, 500).Select(i => $"group-{i}"),
            g => Assert.InRange(CoordinatorPartitioner.PartitionFor(g, 7), 0, 6));
    }

    [Fact]
    public void AGroupIsCoordinatedByTheLeaderOfItsOffsetsPartition()
    {
        var state = ClusterWithOffsetsTopic(partitions: 3, localBrokerId: 1, leaders: [1, 2, 3]);
        var locator = new ClusterCoordinatorLocator(state);
        var group = GroupOnPartition(2, partitions: 3);

        var location = locator.Locate(CoordinatorKind.Group, group);

        Assert.Equal(CoordinatorLookupStatus.Ok, location.Status);
        Assert.Equal(3, location.BrokerId);
        Assert.Equal("10.0.0.3", location.Host);
        Assert.Equal(9092, location.Port);
        Assert.False(locator.IsLocal(CoordinatorKind.Group, group));
        Assert.True(locator.IsLocal(CoordinatorKind.Group, GroupOnPartition(0, partitions: 3)));
    }

    [Fact]
    public void WithoutTheOffsetsTopicOrALeaderNoBrokerCoordinates()
    {
        var noTopic = new ClusterCoordinatorLocator(ClusterWithBrokers(localBrokerId: 1));
        Assert.Equal(CoordinatorLocation.NotAvailable, noTopic.Locate(CoordinatorKind.Group, "g"));
        Assert.False(noTopic.IsLocal(CoordinatorKind.Group, "g"));

        var leaderless = new ClusterCoordinatorLocator(ClusterWithOffsetsTopic(partitions: 1, localBrokerId: 1, leaders: [-1]));
        Assert.Equal(CoordinatorLookupStatus.NotAvailable, leaderless.Locate(CoordinatorKind.Group, "g").Status);
    }

    [Fact]
    public void ABrokerOnItsOwnCoordinatesEverythingAtItsOwnAddress()
    {
        var locator = new LocalCoordinatorLocator(5, () => "10.9.9.9", () => 19092);

        Assert.Equal(new CoordinatorLocation(CoordinatorLookupStatus.Ok, 5, "10.9.9.9", 19092),
            locator.Locate(CoordinatorKind.Group, "any"));
        Assert.True(locator.IsLocal(CoordinatorKind.Transaction, "any"));
    }

    private static string GroupOnPartition(int partition, int partitions) =>
        Enumerable.Range(0, 10_000).Select(i => $"group-{i}")
            .First(g => CoordinatorPartitioner.PartitionFor(g, partitions) == partition);

    private static ClusterState ClusterWithBrokers(int localBrokerId)
    {
        var state = new ClusterState { LocalBrokerId = localBrokerId };
        foreach (var id in new[] { 1, 2, 3 })
            state.AddBroker(new BrokerNode { BrokerId = id, Host = $"10.0.0.{id}", Port = 9092 });
        return state;
    }

    private static ClusterState ClusterWithOffsetsTopic(int partitions, int localBrokerId, int[] leaders)
    {
        var state = ClusterWithBrokers(localBrokerId);
        state.AddTopic(new TopicMetadata
        {
            Name = CoordinatorPartitioner.GroupTopic,
            TopicId = Guid.NewGuid(),
            PartitionCount = partitions,
            ReplicationFactor = 3,
            Config = [],
            CreatedAt = DateTime.UtcNow,
        });
        for (var p = 0; p < partitions; p++)
        {
            var tp = new TopicPartition { Topic = CoordinatorPartitioner.GroupTopic, Partition = p };
            state.AssignReplicas(tp, [1, 2, 3], 1);
            if (leaders[p] >= 0)
                state.ElectLeader(tp, leaders[p]);
            else
                state.UpdatePartitionState(tp, s => s.LeaderBrokerId = -1); // assigning replicas names a leader
        }
        return state;
    }
}
