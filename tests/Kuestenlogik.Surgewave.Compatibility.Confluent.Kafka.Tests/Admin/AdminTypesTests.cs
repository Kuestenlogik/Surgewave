using Confluent.Kafka.Admin;

namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Admin;

/// <summary>
/// Pins the admin request types that carry behavior: TopicSpecification creation
/// defaults (1 partition, replication factor 1) and TopicCollection's snapshot
/// semantics when created from a mutable source.
/// </summary>
public class AdminTypesTests
{
    [Fact]
    public void TopicSpecification_DefaultsToSinglePartitionAndReplica()
    {
        var spec = new TopicSpecification();

        Assert.Equal(string.Empty, spec.Name);
        Assert.Equal(1, spec.NumPartitions);
        Assert.Equal((short)1, spec.ReplicationFactor);
        Assert.Null(spec.ReplicasAssignments);
        Assert.Null(spec.Configs);
    }

    [Fact]
    public void TopicCollection_OfTopicNames_PreservesNames()
    {
        var collection = TopicCollection.OfTopicNames(new[] { "orders", "payments" });

        Assert.Equal(new[] { "orders", "payments" }, collection.TopicNames);
    }

    [Fact]
    public void TopicCollection_OfTopicNames_SnapshotsSource()
    {
        var names = new List<string> { "orders" };

        var collection = TopicCollection.OfTopicNames(names);
        names.Add("added-later");

        Assert.Equal(new[] { "orders" }, collection.TopicNames);
    }
}
