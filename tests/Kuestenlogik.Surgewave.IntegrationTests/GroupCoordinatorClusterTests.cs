using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// One group, one coordinator, across a three-broker cluster (#204). Every broker used to answer FindCoordinator with
/// itself and coordinate every group, so two consumers of one group bootstrapped at different brokers joined two
/// separate copies of it.
/// </summary>
[Trait("Category", TestCategories.Integration)]
[Collection(nameof(BrokerSpawningCollection))]
public sealed class GroupCoordinatorClusterTests : IAsyncLifetime
{
    private const int OffsetsPartitions = 3;

    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;
    private readonly List<SurgewaveRuntime> _brokers = [];

    public GroupCoordinatorClusterTests(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddProvider(new XunitLoggerProvider(output));
            builder.SetMinimumLevel(LogLevel.Warning);
        });
    }

    public async ValueTask InitializeAsync()
    {
        // A declared quorum needs every address before any broker starts, so the ports are reserved up front.
        var ports = ReservePorts(6);
        var quorum = string.Join(",", Enumerable.Range(0, 3).Select(i => $"{i + 1}@127.0.0.1:{ports[3 + i]}"));
        var nodes = Enumerable.Range(0, 3).Select(i => $"{i + 1}:127.0.0.1:{ports[i]}:{ports[3 + i]}").ToArray();

        for (var i = 0; i < 3; i++)
        {
            var self = i;
            _brokers.Add(await SurgewaveRuntime.CreateBuilder()
                .WithBrokerId(i + 1)
                .WithPort(ports[i])
                .WithReplicationPort(ports[3 + i])
                .WithCluster([.. nodes.Where((_, n) => n != self)])
                .WithControllerQuorum(quorum)
                .WithOffsetsTopicPartitions(OffsetsPartitions)
                .WithStorageEngine(StorageEngines.Memory)
                .WithLogging(_loggerFactory)
                .WithShutdownTimeout(10)
                .Build()
                .StartAsync());
        }

        foreach (var self in _brokers)
        {
            foreach (var peer in _brokers.Where(p => !ReferenceEquals(p, self)))
            {
                self.ClusterState!.AddBroker(new BrokerNode
                {
                    BrokerId = peer.BrokerId,
                    Host = peer.Host,
                    Port = peer.Port,
                    ReplicationPort = peer.ReplicationPort,
                });
            }
        }

        Assert.True(
            await TestWaitHelpers.WaitForClusterStabilizationAsync(_brokers, output: _output),
            "the three-broker cluster did not stabilise");

        // Every broker has to know __consumer_offsets and a leader for each partition before the group can meet.
        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => _brokers.All(b => Enumerable.Range(0, OffsetsPartitions).All(p => LeaderOf(b, p) > 0)),
                timeout: TimeSpan.FromSeconds(60), output: _output),
            "__consumer_offsets did not get a leader for every partition on every broker: " + string.Join("; ",
                _brokers.Select(b => $"broker {b.BrokerId}{(b.IsController ? " (controller)" : "")}: " + string.Join(",",
                    Enumerable.Range(0, OffsetsPartitions).Select(p => $"{p}->{LeaderOf(b, p)?.ToString() ?? "unknown"}")))));
    }

    // The leader a broker knows for a __consumer_offsets partition, null while it does not know the partition.
    private static int? LeaderOf(SurgewaveRuntime broker, int partition) =>
        broker.ClusterState!.GetPartitionState(new Kuestenlogik.Surgewave.Core.Models.TopicPartition
        {
            Topic = CoordinatorPartitioner.GroupTopic,
            Partition = partition,
        })?.LeaderBrokerId;

    public async ValueTask DisposeAsync()
    {
        foreach (var broker in _brokers)
        {
            try { await broker.DisposeAsync(); } catch (Exception ex) { _output.WriteLine($"dispose: {ex.Message}"); }
        }
        _loggerFactory.Dispose();
    }

    [Fact]
    public void EveryBrokerNamesTheSameCoordinatorForAGroup()
    {
        var located = _brokers
            .Select(b => new ClusterCoordinatorLocator(b.ClusterState!).Locate(CoordinatorKind.Group, "orders-app"))
            .ToList();

        Assert.All(located, l => Assert.Equal(CoordinatorLookupStatus.Ok, l.Status));
        Assert.Single(located.Select(l => l.BrokerId).Distinct());
    }

    /// <summary>
    /// A group resumes from its committed offsets on a new coordinator once the old one is gone (#205). The offsets
    /// used to live in the coordinator's local file, so the broker that took the group over knew none, and the group
    /// read its partition again from the start.
    /// </summary>
    [Fact(Timeout = 240_000)]
    public async Task AGroupResumesFromItsCommittedOffsetsAfterItsCoordinatorIsStopped()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = $"orders-{Guid.NewGuid():N}";
        var group = $"orders-app-{Guid.NewGuid():N}";
        const int records = 20;
        const int consumedBefore = 10;

        var controller = _brokers.Single(b => b.IsController);
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = controller.BootstrapServers }).Build())
        {
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 3 }]);
        }

        // The group's coordinator, and a broker that is not it to talk to throughout.
        var coordinatorId = new ClusterCoordinatorLocator(controller.ClusterState!).Locate(CoordinatorKind.Group, group).BrokerId;
        var coordinator = _brokers.Single(b => b.BrokerId == coordinatorId);
        var survivor = _brokers.First(b => b.BrokerId != coordinatorId);
        _output.WriteLine($"group {group} is coordinated by broker {coordinatorId}; clients use broker {survivor.BrokerId}");

        using (var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = survivor.BootstrapServers, Acks = Acks.All }).Build())
        {
            for (var i = 0; i < records; i++)
                await producer.ProduceAsync(topic, new Message<Null, string> { Value = $"record-{i}" }, ct);
        }

        // Half of the records consumed and their offset committed, then the consumer leaves.
        using (var first = NewConsumer(survivor.BootstrapServers, group, autoCommit: false))
        {
            first.Subscribe(topic);
            var consumed = 0;
            Assert.True(await TestWaitHelpers.WaitForConditionAsync(
                () =>
                {
                    if (first.Consume(TimeSpan.FromMilliseconds(100)) is { } result)
                    {
                        consumed++;
                        if (consumed == consumedBefore)
                            first.Commit(result); // synchronous: answered once the coordinator has it
                    }
                    return consumed >= consumedBefore;
                },
                timeout: TimeSpan.FromSeconds(90), pollInterval: TimeSpan.FromMilliseconds(10), ct: ct, output: _output),
                $"the first consumer read {consumed} of {consumedBefore} records");
            first.CloseWithinBound();
        }

        await coordinator.DisposeAsync();
        _output.WriteLine($"coordinator broker {coordinatorId} stopped");

        // A new member of the group, on a surviving broker: it must start where the group left off, not from the
        // beginning that auto.offset.reset would give a group without offsets.
        using var second = NewConsumer(survivor.BootstrapServers, group, autoCommit: false);
        second.Subscribe(topic);
        ConsumeResult<Ignore, string>? resumed = null;
        Assert.True(await TestWaitHelpers.WaitForConditionAsync(
            () =>
            {
                try { resumed = second.Consume(TimeSpan.FromMilliseconds(200)); }
                catch (ConsumeException ex) { _output.WriteLine($"consume: {ex.Error}"); }
                return resumed is not null;
            },
            timeout: TimeSpan.FromSeconds(150), pollInterval: TimeSpan.FromMilliseconds(10), ct: ct, output: _output),
            "the group did not resume on its new coordinator");

        Assert.Equal(consumedBefore, resumed!.Offset.Value);
        Assert.Equal($"record-{consumedBefore}", resumed.Message.Value);
    }

    [Fact(Timeout = 180_000)]
    public async Task TwoConsumersBootstrappedAtDifferentBrokersShareOneGroup()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = $"orders-{Guid.NewGuid():N}";
        var group = $"orders-app-{Guid.NewGuid():N}";

        // Through the controller: a broker that is not the controller creates a topic only for itself (#207).
        var controller = _brokers.Single(b => b.IsController);
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = controller.BootstrapServers }).Build())
        {
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 2, ReplicationFactor = 3 }]);
        }

        using var first = NewConsumer(_brokers[1].BootstrapServers, group);
        using var second = NewConsumer(_brokers[2].BootstrapServers, group);
        first.Subscribe(topic);
        second.Subscribe(topic);

        // Asked through the third broker, the group has both members, and they split the two partitions. With a
        // coordinator per broker each consumer met its own copy of the group, and the broker the admin asked had a
        // copy with nobody in it; two copies would also each have handed out both partitions.
        using var observer = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _brokers[0].BootstrapServers }).Build();
        var members = 0;
        var oneGroup = await TestWaitHelpers.WaitForConditionAsync(
            () =>
            {
                first.Consume(TimeSpan.FromMilliseconds(100));
                second.Consume(TimeSpan.FromMilliseconds(100));
                var described = observer.DescribeConsumerGroupsAsync([group]).GetAwaiter().GetResult();
                members = described.ConsumerGroupDescriptions.Single().Members.Count;
                return members == 2
                    && first.Assignment.Count == 1 && second.Assignment.Count == 1
                    && first.Assignment[0].Partition != second.Assignment[0].Partition;
            },
            timeout: TimeSpan.FromSeconds(90), pollInterval: TimeSpan.FromMilliseconds(50), ct: ct, output: _output);

        Assert.True(oneGroup,
            $"the group had {members} member(s); first [{string.Join(",", first.Assignment)}], second [{string.Join(",", second.Assignment)}]");
    }

    private static IConsumer<Ignore, string> NewConsumer(string bootstrap, string group, bool autoCommit = true) =>
        new ConsumerBuilder<Ignore, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = group,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            SessionTimeoutMs = 10_000,
            EnableAutoCommit = autoCommit,
        }).Build();

    private static int[] ReservePorts(int count)
    {
        var listeners = new List<System.Net.Sockets.TcpListener>(count);
        try
        {
            for (var i = 0; i < count; i++)
            {
                var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
                listener.Start();
                listeners.Add(listener);
            }
            return [.. listeners.Select(l => ((System.Net.IPEndPoint)l.LocalEndpoint).Port)];
        }
        finally
        {
            foreach (var listener in listeners) listener.Stop();
        }
    }
}
