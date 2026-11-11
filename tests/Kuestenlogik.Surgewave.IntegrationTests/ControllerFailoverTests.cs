using Confluent.Kafka;
using Confluent.Kafka.Admin;
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
    private SurgewaveRuntime? _secondSurvivor;
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
        // THREE brokers, not two. Losing one of two leaves one of two, which is not a majority,
        // so a two-node cluster cannot elect a new controller — that is Raft's guarantee, not a
        // defect to test around (#172). Three voters survive one loss, which is the situation
        // this class is actually about.
        //
        // The quorum has to be declared, and a declaration needs addresses known before any
        // broker starts — so the replication ports are reserved up front instead of asking the
        // OS for one per broker as it starts.
        // Client ports as well as replication ports: a ClusterNodes entry names both, and a peer
        // told "port 0" can never be dialled.
        var reserved = ReservePorts(6);
        var clientPorts = reserved[..3];
        var replicationPorts = reserved[3..];

        var quorum = string.Join(",", replicationPorts.Select((port, i) => $"{i + 1}@127.0.0.1:{port}"));
        var nodes = Enumerable.Range(0, 3)
            .Select(i => $"{i + 1}:127.0.0.1:{clientPorts[i]}:{replicationPorts[i]}")
            .ToArray();

        var brokers = new[]
        {
            await BuildBrokerAsync(1, clientPorts[0], replicationPorts[0], quorum, nodes[1], nodes[2]),
            await BuildBrokerAsync(2, clientPorts[1], replicationPorts[1], quorum, nodes[0], nodes[2]),
            await BuildBrokerAsync(3, clientPorts[2], replicationPorts[2], quorum, nodes[0], nodes[1]),
        };

        // The brokers know each other from their cluster nodes and their registrations, and nothing is added to that
        // here. This fixture used to write each peer into each broker's cluster state once more, as a node without
        // the inter-broker protocol level its registration had given it: whenever that came after the last
        // registration, the cluster was finalized to the Kafka wire for the rest of the test — which a cluster of
        // these brokers is only until the last of them has registered.

        Assert.True(
            await TestWaitHelpers.WaitForClusterStabilizationAsync(brokers, output: _output),
            "the three-broker cluster did not stabilise");

        // Whichever broker won the election is the one to kill. Raft does not promise the lowest
        // id wins, and asserting that it does would be testing an accident of timing.
        _controller = brokers.Single(b => b.IsController);
        var survivors = brokers.Where(b => !b.IsController).ToArray();
        _survivor = survivors[0];
        _secondSurvivor = survivors[1];
        _output.WriteLine($"broker {_controller.BrokerId} holds the controller role");

        // No wait for the survivors to have heard from the controller: a broker watches every peer it sends
        // heartbeats to, from the first one on. This fixture used to wait until ONE survivor had a health record of
        // the controller. The other one, promoted with none, never reported the controller failed and left it in
        // every ISR — the product's defect, which that wait hid for one survivor and
        // HeartbeatTracksEveryPeerTests pins.
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

        if (_secondSurvivor is not null)
        {
            try { await _secondSurvivor.DisposeAsync(); } catch (Exception ex) { _output.WriteLine($"second survivor dispose: {ex.Message}"); }
        }

        _loggerFactory.Dispose();
    }

    [Fact(Timeout = 180_000)]
    public async Task WhenTheControllerDies_TheSurvivorTakesTheRole()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var controller = _controller!;

        await controller.DisposeAsync();
        _controllerDisposed = true;
        _output.WriteLine("controller broker disposed; waiting for a survivor to take the role");

        // Two of three remain, which is a majority, so an election can conclude. Generous
        // timeout: this asserts that it happens at all, not how fast.
        var promoted = await WaitForNewControllerAsync(cancellationToken);
        _output.WriteLine($"broker {promoted.BrokerId} took the controller role");
    }

    [Fact(Timeout = 180_000)]
    public async Task AfterTheControllerDies_TheClusterCanStillCreateATopic()
    {
        // The operational consequence of the role not moving: topic creation is a controller
        // operation, so a cluster without one cannot even be extended, let alone repair a partition.
        var cancellationToken = TestContext.Current.CancellationToken;
        var controller = _controller!;

        await controller.DisposeAsync();
        _controllerDisposed = true;

        var survivor = await WaitForNewControllerAsync(cancellationToken);

        var topic = $"after-failover-{Guid.NewGuid():N}";
        using var admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = survivor.BootstrapServers }).Build();

        await admin.CreateTopicsAsync([
            new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }
        ]);

        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => survivor.ClusterState!.GetTopic(topic) is not null,
                timeout: TimeSpan.FromSeconds(60), ct: cancellationToken, output: _output),
            "the new controller did not register the topic it was asked to create");
    }

    [Fact(Timeout = 180_000)]
    public async Task ThePromotedControllerHandlesTheFailureThatPromotedIt()
    {
        // Taking the role is not the same as doing the job. The broker that takes over has to notice that the
        // controller before it is gone, and do the controller's repair for that failure — dropping the dead broker
        // out of every ISR — although it was not the controller while that broker was alive.
        var cancellationToken = TestContext.Current.CancellationToken;
        var controller = _controller!;
        var survivors = new[] { _survivor!, _secondSurvivor! };
        var topic = $"controller-failover-{Guid.NewGuid():N}";
        var tp = new SwTopicPartition { Topic = topic, Partition = 0 };

        // RF=3: every broker is a replica, so the controller is one whichever broker won the election.
        // With RF=2 the partition went to the first two brokers by id, and when broker 3 was controller
        // it was in no replica set: the precondition below could never hold, about one run in three
        // (#184). Choosing the replicas explicitly is not an option — CreateTopics ignores replica
        // assignments (#202).
        using (var admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = controller.BootstrapServers }).Build())
        {
            await admin.CreateTopicsAsync([
                new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 3 }
            ]);
        }

        // Every replica in the ISR, on both survivors, before the controller leaves. The controller leads the new
        // partition, and until its followers have fetched once the ISR is the controller alone: "the ISR contains
        // the controller", which this used to wait for, held from the first moment on. The controller then left
        // with no in-sync successor ("No eligible leader in ISR"), and what the test saw afterwards was an unclean
        // election, not a dead broker leaving the ISR of a partition that goes on.
        foreach (var survivor in survivors)
        {
            Assert.True(
                await IsrBecomesAsync(survivor, tp, isr => isr.Count == 3, TimeSpan.FromSeconds(60), cancellationToken),
                $"broker {survivor.BrokerId} never saw all three replicas in the ISR " +
                $"({string.Join(",", survivor.ClusterState!.GetIsrSnapshot(tp))}), so the removal of broker " +
                $"{controller.BrokerId} would prove nothing");
        }

        await controller.DisposeAsync();
        _controllerDisposed = true;

        var promoted = await WaitForNewControllerAsync(cancellationToken);
        _output.WriteLine($"broker {promoted.BrokerId} took the controller role");

        // What a controller does about a failed broker begins with its failure detector reporting it. Watched for
        // itself, and before the ISR: the partition's new leader drops a follower it has not heard from for 40 s on
        // its own, so the dead broker leaves the ISR below under a controller that never noticed the failure too.
        Assert.True(
            await IsReportedFailedAsync(promoted, controller.BrokerId, TimeSpan.FromSeconds(60), cancellationToken),
            $"the promoted controller's failure detector never reported broker {controller.BrokerId} failed; it watches " +
            $"[{string.Join(",", promoted.PeerHealth.Select(peer => $"{peer.Key}:{(peer.Value.IsAlive ? "alive" : "failed")}"))}]");

        Assert.True(
            await IsrBecomesAsync(
                promoted, tp, isr => !isr.Contains(controller.BrokerId), TimeSpan.FromSeconds(60), cancellationToken),
            $"the dead broker is still in the ISR ({string.Join(",", promoted.ClusterState!.GetIsrSnapshot(tp))}) — " +
            "the promoted controller never processed the failure it was promoted for");

        // And on the broker that is NOT the controller: every broker ends up without the dead one in the
        // ISR, not only the controller. (That the controller's removal is itself a log entry, #124, is
        // pinned by IsrRemovalThroughTheLogTests; here the partition leader's own ISR shrink would get the
        // other survivor there too, only later.)
        var otherSurvivor = survivors.Single(b => !ReferenceEquals(b, promoted));
        Assert.True(
            await IsrBecomesAsync(
                otherSurvivor, tp, isr => !isr.Contains(controller.BrokerId), TimeSpan.FromSeconds(60), cancellationToken),
            $"broker {otherSurvivor.BrokerId} still has the dead broker in the ISR " +
            $"({string.Join(",", otherSurvivor.ClusterState!.GetIsrSnapshot(tp))}) — the removal never reached the log");
    }

    /// <summary>
    /// The survivor that won the election — either of them may, and which one is not the point.
    /// </summary>
    private async Task<SurgewaveRuntime> WaitForNewControllerAsync(CancellationToken ct)
    {
        var survivors = new[] { _survivor!, _secondSurvivor! };

        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => survivors.Any(b => b.IsController),
                timeout: TimeSpan.FromSeconds(90), pollInterval: TimeSpan.FromMilliseconds(250),
                ct: ct, output: _output),
            "no surviving broker became controller, so nothing in this cluster can elect a partition leader again");

        return survivors.First(b => b.IsController);
    }

    /// <summary>
    /// Whether <paramref name="broker"/> sees the ISR of <paramref name="tp"/> as <paramref name="expected"/> within
    /// <paramref name="timeout"/>.
    /// </summary>
    /// <remarks>
    /// The handshake on applied metadata: the signal is taken before each look, so no change in between is missed.
    /// </remarks>
    private static async Task<bool> IsrBecomesAsync(
        SurgewaveRuntime broker, SwTopicPartition tp, Func<List<int>, bool> expected, TimeSpan timeout, CancellationToken ct)
    {
        var state = broker.ClusterState!;
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(timeout);
        try
        {
            while (true)
            {
                var next = state.PartitionsChanged;
                if (expected(state.GetIsrSnapshot(tp)))
                    return true;
                await next.WaitAsync(bounded.Token);
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return false;
        }
    }

    /// <summary>
    /// Whether the failure detector of <paramref name="broker"/> reports <paramref name="peerId"/> failed within
    /// <paramref name="timeout"/>.
    /// </summary>
    /// <remarks>The handshake on the detector: the signal is taken before each look.</remarks>
    private static async Task<bool> IsReportedFailedAsync(
        SurgewaveRuntime broker, int peerId, TimeSpan timeout, CancellationToken ct)
    {
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(timeout);
        try
        {
            while (true)
            {
                var next = broker.PeerHealthChanged;
                if (broker.PeerHealth.TryGetValue(peerId, out var health) && !health.IsAlive)
                    return true;
                await next.WaitAsync(bounded.Token);
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return false;
        }
    }

    /// <summary>Ports nothing is listening on yet, so a quorum can name them before they exist.</summary>
    /// <remarks>
    /// Reserved by binding and releasing, which races with anything else on the machine grabbing
    /// the same port in between. Accepted: the alternative is a quorum that cannot be declared,
    /// and a declared quorum is the whole point of this fixture.
    /// </remarks>
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

    private async Task<SurgewaveRuntime> BuildBrokerAsync(
        int brokerId, int clientPort, int replicationPort, string quorum, params string[] clusterNodes)
        => await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(brokerId)
            .WithPort(clientPort)
            .WithReplicationPort(replicationPort)
            .WithCluster(clusterNodes)
            .WithControllerQuorum(quorum)
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
}
