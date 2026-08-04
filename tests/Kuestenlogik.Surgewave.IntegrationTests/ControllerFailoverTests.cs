using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Xunit;
using SwTopicPartition = Kuestenlogik.Surgewave.Core.Models.TopicPartition;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// What happens to the controller role when the broker holding it goes away.
///
/// <para><b>Why this is not covered by the partition-failover tests.</b> Those shut the leader down
/// gracefully, and a graceful shutdown hands each partition's leadership to an ISR member before the
/// broker dies — so they prove that partition leadership moves, and say nothing about the controller
/// role, which is not transferred by that path. Everything that elects a leader afterwards needs a
/// controller: <c>ElectLeaderAsync</c> returns immediately on a broker that is not one. A cluster
/// that survives the crash with no controller can serve what it already has and can never repair
/// itself.</para>
///
/// <para>The timings here are deliberately short — failure detection has to fire inside the test —
/// which is also why this class runs its own brokers instead of sharing the replication fixture.</para>
/// </summary>
[Trait("Category", TestCategories.Integration)]
[Collection(nameof(BrokerSpawningCollection))]
public sealed class ControllerFailoverTests : IAsyncLifetime
{
    // Short enough that a dead peer is noticed within the test, long enough to survive a loaded
    // runner without brokers declaring each other dead mid-setup.
    private const int HeartbeatIntervalMs = 1_000;
    private const int HeartbeatTimeoutMs = 5_000;

    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    private SurgewaveRuntime? _controller;
    private SurgewaveRuntime? _survivor;
    private bool _controllerDisposed;

    public ControllerFailoverTests(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddProvider(new XunitLoggerProvider(output));
            builder.SetMinimumLevel(LogLevel.Information);
        });
    }

    public async ValueTask InitializeAsync()
    {
        // Broker 1 wins the controller election: it has the lowest id.
        _controller = await BuildBrokerAsync(brokerId: 1);
        _survivor = await BuildBrokerAsync(brokerId: 2,
            $"1:{_controller.Host}:{_controller.Port}:{_controller.ReplicationPort}");

        StitchMesh(_controller, _survivor);
        StitchMesh(_survivor, _controller);

        Assert.True(
            await TestWaitHelpers.WaitForClusterStabilizationAsync([_controller, _survivor], output: _output),
            "the two-broker cluster did not stabilise");

        Assert.True(_controller.IsController, "broker 1 should hold the controller role");
        Assert.False(_survivor.IsController);

        // The failure detector only reports brokers it is actually tracking, so a broker killed
        // before the survivor ever saw it would go unnoticed for reasons that have nothing to do
        // with what these tests are about. Make the precondition explicit rather than racing it.
        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => _survivor.PeerHealth.TryGetValue(1, out var health) && health.IsAlive,
                timeout: TimeSpan.FromSeconds(30), output: _output),
            "the survivor never saw broker 1 as a live peer, so it could never detect its death");
    }

    public async ValueTask DisposeAsync()
    {
        if (_controller is not null && !_controllerDisposed)
        {
            try { await _controller.DisposeAsync(); } catch (Exception ex) { _output.WriteLine($"controller dispose: {ex.Message}"); }
        }

        if (_survivor is not null)
        {
            try { await _survivor.DisposeAsync(); } catch (Exception ex) { _output.WriteLine($"survivor dispose: {ex.Message}"); }
        }

        _loggerFactory.Dispose();
    }

    [Fact(Timeout = 180_000)]
    public async Task WhenTheControllerDies_TheSurvivorTakesTheRole()
    {
        var controller = _controller!;
        var survivor = _survivor!;

        await controller.DisposeAsync();
        _controllerDisposed = true;
        _output.WriteLine("controller broker disposed; waiting for the survivor to notice");

        // Heartbeat detection has to fire and the survivor has to win the election. Generous
        // timeout: this asserts that it happens at all, not how fast.
        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => survivor.IsController,
                timeout: TimeSpan.FromSeconds(90), pollInterval: TimeSpan.FromMilliseconds(250), output: _output),
            "the surviving broker never became controller, so nothing in this cluster can elect a partition leader again");
    }

    [Fact(Timeout = 180_000)]
    public async Task AfterTheControllerDies_TheClusterCanStillCreateATopic()
    {
        // The operational consequence of the role not moving: topic creation is a controller
        // operation, so a cluster without one cannot even be extended, let alone repair a partition.
        var controller = _controller!;
        var survivor = _survivor!;

        await controller.DisposeAsync();
        _controllerDisposed = true;

        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => survivor.IsController,
                timeout: TimeSpan.FromSeconds(90), pollInterval: TimeSpan.FromMilliseconds(250), output: _output),
            "the surviving broker never became controller");

        var topic = $"after-failover-{Guid.NewGuid():N}";
        using var admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = survivor.BootstrapServers }).Build();

        await admin.CreateTopicsAsync([
            new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }
        ]);

        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => survivor.ClusterState!.GetTopic(topic) is not null,
                timeout: TimeSpan.FromSeconds(60), output: _output),
            "the new controller did not register the topic it was asked to create");
    }

    [Fact(Timeout = 180_000)]
    public async Task ThePromotedControllerHandlesTheFailureThatPromotedIt()
    {
        // Taking the role is not the same as doing the job. The failure of the old controller is
        // reported exactly once, and it is reported to a broker that is not yet controller; if the
        // promotion is where the handling stops, the controller-side repair for that failure —
        // dropping the dead broker out of every ISR — never runs, and no second event ever comes.
        var controller = _controller!;
        var survivor = _survivor!;
        var topic = $"controller-failover-{Guid.NewGuid():N}";
        var tp = new SwTopicPartition { Topic = topic, Partition = 0 };

        using (var admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = controller.BootstrapServers }).Build())
        {
            await admin.CreateTopicsAsync([
                new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 2 }
            ]);
        }

        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => survivor.ClusterState!.GetIsrSnapshot(tp).Contains(1),
                timeout: TimeSpan.FromSeconds(60), output: _output),
            "the survivor never saw broker 1 in the ISR, so its removal would prove nothing");

        await controller.DisposeAsync();
        _controllerDisposed = true;

        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => survivor.IsController,
                timeout: TimeSpan.FromSeconds(90), pollInterval: TimeSpan.FromMilliseconds(250), output: _output),
            "the surviving broker never became controller");

        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => !survivor.ClusterState!.GetIsrSnapshot(tp).Contains(1),
                timeout: TimeSpan.FromSeconds(60), output: _output),
            $"the dead broker is still in the ISR ({string.Join(",", survivor.ClusterState!.GetIsrSnapshot(tp))}) — " +
            "the promoted controller never processed the failure it was promoted for");
    }

    private async Task<SurgewaveRuntime> BuildBrokerAsync(int brokerId, params string[] clusterNodes)
        => await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(brokerId)
            .WithPort(0)
            .WithReplicationPort(0)
            .WithCluster(clusterNodes)
            .WithPartitions(1)
            .WithReplicationFactor(2)
            .WithAutoCreateTopics()
            .WithStorageEngine(StorageEngines.Memory)
            .WithLogging(_loggerFactory)
            .WithShutdownTimeout(20)
            .WithHeartbeatInterval(HeartbeatIntervalMs)
            .WithHeartbeatTimeout(HeartbeatTimeoutMs)
            .Build()
            .StartAsync();

    private static void StitchMesh(SurgewaveRuntime self, SurgewaveRuntime peer)
        => self.ClusterState!.AddBroker(new BrokerNode
        {
            BrokerId = peer.BrokerId,
            Host = peer.Host,
            Port = peer.Port,
            ReplicationPort = peer.ReplicationPort
        });
}
