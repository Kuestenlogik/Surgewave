using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// One transactional id, one coordinator, across a three-broker cluster — and the coordinator's state outlives it
/// (#197). Every broker coordinated every id it was asked about, from a file of its own, so a broker that took over
/// after a failover knew nothing of the id: the producer that had used it before was not fenced.
/// </summary>
[Trait("Category", TestCategories.Integration)]
[Collection(nameof(BrokerSpawningCollection))]
public sealed class TransactionCoordinatorClusterTests(ITestOutputHelper output) : IAsyncLifetime
{
    private CoordinatorCluster _cluster = null!;

    public async ValueTask InitializeAsync() => _cluster = await CoordinatorCluster.StartAsync(output);

    public async ValueTask DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public void EveryBrokerNamesTheSameCoordinatorForATransactionalId()
    {
        var coordinators = _cluster.Brokers
            .Select(b => new Kuestenlogik.Surgewave.Clustering.Cluster.ClusterCoordinatorLocator(b.ClusterState!)
                .Locate(CoordinatorKind.Transaction, "orders-tx"))
            .ToList();

        Assert.All(coordinators, c => Assert.Equal(CoordinatorLookupStatus.Ok, c.Status));
        Assert.Single(coordinators.Select(c => c.BrokerId).Distinct());
    }

    [Fact(Timeout = 240_000)]
    public async Task AfterTheCoordinatorIsStopped_TheProducerThatTakesTheIdOverFencesThePreviousOne()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = $"orders-{Guid.NewGuid():N}";

        var controller = _cluster.Brokers.Single(b => b.IsController);
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = controller.BootstrapServers }).Build())
        {
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 3 }]);
        }
        Assert.True(await TestWaitHelpers.WaitForConditionAsync(
            () => CoordinatorCluster.LeaderOf(controller, topic, 0) > 0, timeout: TimeSpan.FromSeconds(30), ct: ct, output: output));
        var leader = _cluster.Brokers.Single(b => b.BrokerId == CoordinatorCluster.LeaderOf(controller, topic, 0));

        // A transactional id the topic's leader coordinates: a produce is verified on the broker that coordinates the
        // id. Verifying through the coordinator from another broker is the next slice of #197.
        var transactionalId = Enumerable.Range(0, 1000).Select(i => $"orders-tx-{i}")
            .First(id => _cluster.CoordinatorOf(CoordinatorKind.Transaction, id) == leader);
        var survivor = _cluster.Brokers.First(b => b != leader);
        output.WriteLine($"{transactionalId} is coordinated by broker {leader.BrokerId}, which leads {topic}; clients use broker {survivor.BrokerId}");

        using var first = NewProducer(survivor, transactionalId);
        first.InitTransactions(TimeSpan.FromSeconds(60));
        first.BeginTransaction();
        await first.ProduceAsync(topic, new Message<Null, string> { Value = "first" }, ct);
        first.CommitTransaction(TimeSpan.FromSeconds(60));

        await _cluster.StopAsync(leader);

        // The id's coordinator is another broker now; it answers COORDINATOR_LOAD_IN_PROGRESS until it has read the
        // partition, and the client retries.
        using var second = NewProducer(survivor, transactionalId);
        second.InitTransactions(TimeSpan.FromSeconds(150));

        // The new coordinator took the id over with its producer id and bumped the epoch, so the first producer is
        // an older epoch of the same producer id: fenced. A coordinator that knew nothing of the id would not know
        // the first producer's id at all, and answer INVALID_PRODUCER_ID_MAPPING, which is no fence.
        first.BeginTransaction();
        var failed = Assert.ThrowsAny<KafkaException>(() =>
        {
            first.Produce(topic, new Message<Null, string> { Value = "zombie" });
            first.CommitTransaction(TimeSpan.FromSeconds(60));
        });
        output.WriteLine($"first producer: {failed.Error.Code} ({failed.Error.Reason}), fatal={failed.Error.IsFatal}");
        Assert.True(failed.Error.Code is ErrorCode.Local_Fenced or ErrorCode.ProducerFenced or ErrorCode.InvalidProducerEpoch,
            $"the previous producer was not fenced: {failed.Error.Code} ({failed.Error.Reason})");
    }

    private static IProducer<Null, string> NewProducer(SurgewaveRuntime broker, string transactionalId) =>
        new ProducerBuilder<Null, string>(new ProducerConfig
        {
            BootstrapServers = broker.BootstrapServers,
            TransactionalId = transactionalId,
            TransactionTimeoutMs = 60_000,
        }).Build();
}
