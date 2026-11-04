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

        // A transactional id the topic's leader coordinates, so stopping that one broker takes both away.
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

    /// <summary>
    /// A transaction writes to partitions led by brokers other than its coordinator (#197): each leader verifies the
    /// batch with the coordinator, and each gets its marker. A leader used to ask only its own broker's coordinator,
    /// which answered NOT_COORDINATOR, and the batch was never appended.
    /// </summary>
    [Fact(Timeout = 240_000)]
    public async Task ATransactionAcrossPartitionsLedByOtherBrokersCommitsAndAborts()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = $"orders-{Guid.NewGuid():N}";
        const int partitions = 3;

        var controller = _cluster.Brokers.Single(b => b.IsController);
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = controller.BootstrapServers }).Build())
        {
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 3 }]);
        }
        Assert.True(await TestWaitHelpers.WaitForConditionAsync(
            () => _cluster.Brokers.All(b => Enumerable.Range(0, partitions).All(p => CoordinatorCluster.LeaderOf(b, topic, p) > 0)),
            timeout: TimeSpan.FromSeconds(30), ct: ct, output: output));

        var leaders = Enumerable.Range(0, partitions).Select(p => CoordinatorCluster.LeaderOf(controller, topic, p)!.Value).ToList();
        var coordinator = _cluster.CoordinatorOf(CoordinatorKind.Transaction, "orders-tx");
        output.WriteLine($"orders-tx is coordinated by broker {coordinator.BrokerId}; {topic} is led by {string.Join(",", leaders)}");
        Assert.Contains(leaders, l => l != coordinator.BrokerId);

        using var producer = NewProducer(controller, "orders-tx");
        producer.InitTransactions(TimeSpan.FromSeconds(60));

        producer.BeginTransaction();
        for (var p = 0; p < partitions; p++)
            await producer.ProduceAsync(new TopicPartition(topic, p), new Message<Null, string> { Value = $"aborted-{p}" }, ct);
        producer.AbortTransaction(TimeSpan.FromSeconds(60));

        producer.BeginTransaction();
        for (var p = 0; p < partitions; p++)
            await producer.ProduceAsync(new TopicPartition(topic, p), new Message<Null, string> { Value = $"committed-{p}" }, ct);
        producer.CommitTransaction(TimeSpan.FromSeconds(60));

        // A read-committed consumer gets past the aborted batches only once their markers are on the partitions'
        // leaders, and sees the committed ones only once theirs are.
        using var consumer = new ConsumerBuilder<Null, string>(new ConsumerConfig
        {
            BootstrapServers = controller.BootstrapServers,
            GroupId = $"readers-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            IsolationLevel = IsolationLevel.ReadCommitted,
            EnableAutoCommit = false,
        }).Build();
        consumer.Assign(Enumerable.Range(0, partitions).Select(p => new TopicPartitionOffset(topic, p, Offset.Beginning)));

        var seen = new List<string>();
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (seen.Count < partitions && DateTime.UtcNow < deadline)
        {
            var record = consumer.Consume(TimeSpan.FromSeconds(5));
            if (record?.Message is { } message)
                seen.Add(message.Value);
        }

        Assert.Equal(Enumerable.Range(0, partitions).Select(p => $"committed-{p}").Order(), seen.Order());
    }

    private static IProducer<Null, string> NewProducer(SurgewaveRuntime broker, string transactionalId) =>
        new ProducerBuilder<Null, string>(new ProducerConfig
        {
            BootstrapServers = broker.BootstrapServers,
            TransactionalId = transactionalId,
            TransactionTimeoutMs = 60_000,
        }).Build();
}
