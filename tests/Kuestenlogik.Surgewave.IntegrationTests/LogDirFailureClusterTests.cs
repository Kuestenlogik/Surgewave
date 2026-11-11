using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Kuestenlogik.Surgewave.Testing.Chaos;
using Xunit;
using SurgewaveTopicPartition = Kuestenlogik.Surgewave.Core.Models.TopicPartition;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// A broker whose disk fails while it keeps heartbeating (#162). Its storage takes the log directory offline, its
/// heartbeat reports it as Kafka's OfflineLogDirs, and the controller moves the leadership to an in-sync replica — so
/// a producer that was writing to the broker keeps working, and a consumer reads everything it wrote. Before, the
/// broker kept its leadership for good: none of the liveness mechanisms sees a broker that is there but not working.
/// </summary>
[Trait("Category", TestCategories.Integration)]
[Collection(nameof(BrokerSpawningCollection))]
public sealed class LogDirFailureClusterTests(ITestOutputHelper output) : IAsyncLifetime
{
    private const string Topic = "logdir-failover";
    private const int Partitions = 3;

    private readonly ChaosEngine _chaos = new();
    private CoordinatorCluster _cluster = null!;

    public async ValueTask InitializeAsync()
    {
        // Every broker's storage can be made to fail on its own: the fault is scoped to the broker id.
        _cluster = await CoordinatorCluster.StartAsync(output, configure: (builder, brokerId) =>
            builder.WithStorage(() => new ChaosLogSegmentFactory(new MemoryLogSegmentFactory(), _chaos, brokerId)));
    }

    public async ValueTask DisposeAsync()
    {
        _chaos.DeactivateAll();
        await _cluster.DisposeAsync();
    }

    [Fact(Timeout = 180_000)]
    public async Task ABrokerWhoseLogDirectoryFailsLosesItsLeadershipAndTheClusterKeepsServing()
    {
        var ct = TestContext.Current.CancellationToken;
        var bootstrap = string.Join(",", _cluster.Brokers.Select(b => b.BootstrapServers));
        var controller = _cluster.Brokers.Single(b => b.IsController);

        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
        {
            await admin.CreateTopicsAsync([new TopicSpecification { Name = Topic, NumPartitions = Partitions, ReplicationFactor = 3 }]);
        }

        // Every replica in sync first, so each partition has successors to move to.
        await UntilAsync(controller.ClusterState!, () => Enumerable.Range(0, Partitions)
            .All(p => controller.ClusterState!.GetIsrSnapshot(Tp(p)).Count == 3), ct);

        // A partition led by a broker other than the controller: the controller hears of its own broker's disk only
        // through the heartbeat it does not send itself, which is not what this test is about.
        var (partition, failing) = Enumerable.Range(0, Partitions)
            .Select(p => (p, Leader: controller.ClusterState!.GetPartitionState(Tp(p))!.LeaderBrokerId))
            .Where(x => x.Leader != controller.BrokerId)
            .Select(x => (x.p, _cluster.Brokers.Single(b => b.BrokerId == x.Leader)))
            .First();
        output.WriteLine($"partition {partition} is led by broker {failing.BrokerId}; controller is {controller.BrokerId}");

        var target = new TopicPartition(Topic, new Partition(partition));
        using var producer = new ProducerBuilder<Null, string>(new ProducerConfig
        {
            BootstrapServers = bootstrap,
            Acks = Acks.All,
        }).Build();

        var written = new List<string>();
        for (var i = 0; i < 5; i++)
        {
            var value = $"before-{i}";
            var delivered = await producer.ProduceAsync(target, new Message<Null, string> { Value = value }, ct);
            Assert.Equal(PersistenceStatus.Persisted, delivered.Status);
            written.Add(value);
        }

        // The disk of the leader fails. The next write to it is what the storage notices.
        using var failure = DiskFailureScenario.Create(_chaos, failing.BrokerId);
        var afterFailure = new List<Task<DeliveryResult<Null, string>>>();
        for (var i = 0; i < 5; i++)
        {
            var value = $"after-{i}";
            afterFailure.Add(producer.ProduceAsync(target, new Message<Null, string> { Value = value }, ct));
            written.Add(value);
        }

        await failing.LogManager.LogDirectory.Offline.WaitAsync(ct);

        // The controller moves the leadership away and takes the broker out of every ISR.
        await UntilAsync(controller.ClusterState!, () => Enumerable.Range(0, Partitions).All(p =>
            controller.ClusterState!.GetPartitionState(Tp(p))!.LeaderBrokerId != failing.BrokerId
            && !controller.ClusterState!.GetIsrSnapshot(Tp(p)).Contains(failing.BrokerId)), ct);
        var successor = controller.ClusterState!.GetPartitionState(Tp(partition))!.LeaderBrokerId;
        output.WriteLine($"partition {partition} moved to broker {successor}");

        // The producer kept working: what it sent into the failure is delivered by the successor.
        foreach (var delivery in await Task.WhenAll(afterFailure))
            Assert.Equal(PersistenceStatus.Persisted, delivery.Status);

        // And a consumer reads all of it, before and after, from the partition's new leader.
        using var consumer = new ConsumerBuilder<Ignore, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = "logdir-failover-reader",
            EnableAutoCommit = false,
        }).Build();
        consumer.Assign(new TopicPartitionOffset(target, Offset.Beginning));

        var read = new List<string>();
        while (read.Count < written.Count)
        {
            var record = consumer.Consume(ct);
            if (record?.Message is not null)
                read.Add(record.Message.Value);
        }

        Assert.Equal(written.Order(), read.Order());
        Assert.NotEqual(failing.BrokerId, successor);
    }

    private static SurgewaveTopicPartition Tp(int partition) => new() { Topic = Topic, Partition = partition };

    // The handshake on applied metadata: the signal is taken before each look, so no change in between is missed.
    private static async Task UntilAsync(ClusterState state, Func<bool> condition, CancellationToken ct)
    {
        while (true)
        {
            var next = state.PartitionsChanged;
            if (condition())
                return;
            await next.WaitAsync(ct);
        }
    }
}
