using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// A topic created through any broker is a topic of the cluster (#207). A broker that was not the controller created
/// the topic for itself alone — no metadata log entry, no replicas — and answered success, so the rest of the cluster
/// never knew it. It now hands the creation to the controller.
/// </summary>
/// <remarks>
/// Which broker a client's request reaches is the client's choice — an admin client sends CreateTopics to the
/// controller it learned from Metadata — so these show the result end to end; <c>NonControllerTopicCreationTests</c>
/// and <c>NativeControllerClientTests</c> pin the path through a broker that is not the controller.
/// </remarks>
[Trait("Category", TestCategories.Integration)]
[Collection(nameof(BrokerSpawningCollection))]
public sealed class TopicCreationClusterTests(ITestOutputHelper output) : IAsyncLifetime
{
    private CoordinatorCluster _cluster = null!;

    public async ValueTask InitializeAsync() => _cluster = await CoordinatorCluster.StartAsync(output);

    public async ValueTask DisposeAsync() => await _cluster.DisposeAsync();

    [Fact(Timeout = 120_000)]
    public async Task CreateTopicsThroughABrokerThatIsNotTheController_CreatesItInTheCluster()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = $"orders-{Guid.NewGuid():N}";
        var follower = _cluster.Brokers.First(b => !b.IsController);

        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = follower.BootstrapServers }).Build())
        {
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 2, ReplicationFactor = 3 }]);
        }

        await AssertEveryBrokerLeadsOrFollowsAsync(topic, partitions: 2, ct);
    }

    [Fact(Timeout = 120_000)]
    public async Task AutoCreationThroughABrokerThatIsNotTheController_CreatesItInTheCluster()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = $"orders-{Guid.NewGuid():N}";
        var follower = _cluster.Brokers.First(b => !b.IsController);

        // A producer's first metadata request for an unknown topic creates it (the broker allows auto-creation).
        using (var producer = new ProducerBuilder<Null, string>(new ProducerConfig
               {
                   BootstrapServers = follower.BootstrapServers,
                   AllowAutoCreateTopics = true,
               }).Build())
        {
            await producer.ProduceAsync(topic, new Message<Null, string> { Value = "first" }, ct);
        }

        await AssertEveryBrokerLeadsOrFollowsAsync(topic, partitions: 1, ct);
    }

    /// <summary>
    /// Every broker reports a topic's full partition count (#208). With one replica a partition lives on one broker,
    /// and Metadata counted only the partitions the broker asked hosted: a group leader that asked such a broker
    /// assigned one partition of two.
    /// </summary>
    [Fact(Timeout = 120_000)]
    public async Task EveryBrokerReportsEveryPartitionOfATopicWithOneReplica()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = $"orders-{Guid.NewGuid():N}";
        var controller = _cluster.Brokers.Single(b => b.IsController);
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = controller.BootstrapServers }).Build())
        {
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 3, ReplicationFactor = 1 }]);
        }
        await AssertEveryBrokerLeadsOrFollowsAsync(topic, partitions: 3, ct);

        foreach (var broker in _cluster.Brokers)
        {
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = broker.BootstrapServers }).Build();
            var reported = admin.GetMetadata(topic, TimeSpan.FromSeconds(10)).Topics.Single();
            Assert.True(reported.Partitions.Count == 3, $"broker {broker.BrokerId} reports {reported.Partitions.Count} partition(s) of 3");
        }
    }

    /// <summary>
    /// A topic created with the replicas the client chose has them, in the client's order, the first the leader
    /// (#202). The assignment was dropped and the topic got the controller's own placement, without an error.
    /// </summary>
    [Fact(Timeout = 120_000)]
    public async Task CreateTopicsWithAReplicaAssignment_PlacesTheReplicasAsTheClientChose()
    {
        var topic = $"orders-{Guid.NewGuid():N}";
        // Not the controller's placement for two partitions with two replicas on brokers 1..3 ([1,2], [2,3]).
        List<int>[] chosen = [[3, 1], [1, 3]];
        var bootstrap = _cluster.Brokers.First(b => !b.IsController);

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap.BootstrapServers }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification
        {
            Name = topic,
            // librdkafka wants the count beside an assignment and sends NumPartitions and ReplicationFactor -1 for it.
            NumPartitions = chosen.Length,
            ReplicationFactor = -1,
            ReplicasAssignments = new Dictionary<int, List<int>> { [0] = chosen[0], [1] = chosen[1] },
        }]);

        // The client asks whichever broker it likes, so every broker has to know the placement first.
        await EveryBrokerKnowsTheReplicasAsync(topic, TestContext.Current.CancellationToken);
        var described = await admin.DescribeTopicsAsync(TopicCollection.OfTopicNames([topic]));
        var partitions = Assert.Single(described.TopicDescriptions).Partitions.OrderBy(p => p.Partition).ToList();
        Assert.Equal(chosen, partitions.Select(p => p.Replicas.Select(r => r.Id).ToList()));
        Assert.Equal(chosen.Select(c => c[0]), partitions.Select(p => p.Leader.Id));
    }

    /// <summary>A replica assignment naming a broker the cluster does not have is refused, as Kafka refuses it (#202).</summary>
    [Fact(Timeout = 120_000)]
    public async Task CreateTopicsWithAReplicaAssignmentOfAnUnknownBroker_IsRefused()
    {
        var topic = $"orders-{Guid.NewGuid():N}";
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _cluster.Brokers[0].BootstrapServers }).Build();

        var refused = await Assert.ThrowsAsync<CreateTopicsException>(() => admin.CreateTopicsAsync([new TopicSpecification
        {
            Name = topic,
            NumPartitions = 1,
            ReplicationFactor = -1,
            ReplicasAssignments = new Dictionary<int, List<int>> { [0] = [1, 9] },
        }]).WaitAsync(TestContext.Current.CancellationToken));

        var error = Assert.Single(refused.Results).Error;
        Assert.Equal(ErrorCode.InvalidReplicaAssignment, error.Code);
        Assert.Equal("The manual partition assignment includes broker 9, but no such broker is registered.", error.Reason);
    }

    // Until every broker has applied the topic and the replicas of each of its partitions from the metadata log: each
    // look is taken after the signal of the next change, so none is missed.
    private async Task EveryBrokerKnowsTheReplicasAsync(string topic, CancellationToken ct)
    {
        foreach (var clusterState in _cluster.Brokers.Select(b => b.ClusterState!))
        {
            while (true)
            {
                var changed = clusterState.TopicsChanged;
                if (clusterState.GetTopic(topic) is { } known && Enumerable.Range(0, known.PartitionCount).All(p =>
                        clusterState.ReplicasOf(new Kuestenlogik.Surgewave.Core.Models.TopicPartition { Topic = topic, Partition = p }).Count > 0))
                    break;
                await changed.WaitAsync(ct);
            }
        }
    }

    // Every broker knows the topic with a leader for each partition — what only a topic in the metadata log has.
    private async Task AssertEveryBrokerLeadsOrFollowsAsync(string topic, int partitions, CancellationToken ct)
    {
        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => _cluster.Brokers.All(b => Enumerable.Range(0, partitions).All(p => CoordinatorCluster.LeaderOf(b, topic, p) > 0)),
                timeout: TimeSpan.FromSeconds(60), ct: ct, output: output),
            $"{topic} is not a topic of the cluster: " + string.Join("; ", _cluster.Brokers.Select(b =>
                $"broker {b.BrokerId}{(b.IsController ? " (controller)" : "")}: " + string.Join(",",
                    Enumerable.Range(0, partitions).Select(p => $"{p}->{CoordinatorCluster.LeaderOf(b, topic, p)?.ToString() ?? "unknown"}")))));
    }
}
