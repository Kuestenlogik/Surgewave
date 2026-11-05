using System.Text.Json;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// A topic's config is in the metadata log (#119), so every broker hosting the topic's replicas runs it with it: a
/// change reaches them, and a broker that takes a partition over is not left with its own defaults. A change was made
/// on the broker that was asked alone, and a topic created through the controller had its config there only.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TopicConfigMetadataTests : IAsyncDisposable
{
    private readonly string _raftDir = Path.Combine(Path.GetTempPath(), "surgewave-topic-config-" + Guid.NewGuid().ToString("N"));
    private readonly string _logDir = Path.Combine(Path.GetTempPath(), "surgewave-topic-config-logs-" + Guid.NewGuid().ToString("N"));
    private RaftNode? _raftNode;
    private LogManager? _logs;
    private ReplicationTransitionQueue? _transitions;

    [Fact]
    public async Task EveryBrokerReadsAChangeFromTheLogAndASnapshotKeepsIt()
    {
        var state = new ClusterState();
        var machine = Machine(state);
        machine.Apply(TopicCreated(1, "orders", replicas: [5]));

        machine.Apply(Entry(2, new ConfigChangedCommand("orders", new() { ["retention.ms"] = "5000", ["segment.bytes"] = "1024" }, [])));
        machine.Apply(Entry(3, new ConfigChangedCommand("orders", [], ["segment.bytes"])));

        Assert.Equal(new Dictionary<string, string> { ["cleanup.policy"] = "delete", ["retention.ms"] = "5000" }, state.GetTopic("orders")!.Config);

        var restored = new ClusterState();
        await Machine(restored).RestoreFromSnapshotAsync(await machine.CreateSnapshotAsync(TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
        Assert.Equal("5000", restored.GetTopic("orders")!.Config["retention.ms"]);
    }

    [Fact]
    public async Task ABrokerHostingAReplicaRunsTheTopicWithItsConfigAndEveryChange()
    {
        var ct = TestContext.Current.CancellationToken;
        var state = new ClusterState();
        _logs = new LogManager(_logDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        var config = new ClusteringConfig { BrokerId = 5 };
        var replicas = new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, _logs, config, new TcpPeerTransport());
        _transitions = new ReplicationTransitionQueue(replicas, 5, NullLogger<ReplicationTransitionQueue>.Instance);
        var machine = Machine(state, _transitions);

        // The topic's config from its creation, not this broker's defaults (#214).
        machine.Apply(TopicCreated(1, "orders", replicas: [5]));
        Assert.True(await WaitForAsync(() => _logs.GetTopicMetadata("orders")?.Config.GetValueOrDefault("cleanup.policy") == "delete", ct));

        // A change of it, in log order with the leadership (#119).
        machine.Apply(Entry(2, new ConfigChangedCommand("orders", new() { ["retention.ms"] = "5000" }, ["cleanup.policy"])));
        Assert.True(await WaitForAsync(() => _logs.GetTopicMetadata("orders")!.Config is var c
            && c.GetValueOrDefault("retention.ms") == "5000" && !c.ContainsKey("cleanup.policy"), ct));
    }

    [Fact]
    public async Task TheControllerWritesAChangeToTheLogAndAnswersOnceCommitted()
    {
        var ct = TestContext.Current.CancellationToken;
        var (controller, state) = await ControllerAsync(ct);
        state.AddTopic(Topic("orders"));

        Assert.True(await controller.ChangeTopicConfigAsync("orders", new Dictionary<string, string> { ["retention.ms"] = "5000" }, [], ct));
        Assert.Equal("5000", state.GetTopic("orders")!.Config["retention.ms"]);
    }

    [Fact]
    public async Task ABrokerThatIsNotTheControllerAsksIt()
    {
        var ct = TestContext.Current.CancellationToken;
        var (controller, state) = await ControllerAsync(ct);
        state.AddTopic(Topic("orders"));
        var server = Server(state, controller);

        var answer = await server.ProcessAsync(SurgewaveOpCode.InterBrokerTopicConfig,
            InterBrokerFrameCodec.EncodePayload(new TopicConfigChangePayload("orders", new Dictionary<string, string> { ["retention.ms"] = "7000" }, ["cleanup.policy"])), ct);

        Assert.Equal(ClusterRpcStatus.None, StatusOf(answer));
        Assert.Equal(new Dictionary<string, string> { ["retention.ms"] = "7000" }, state.GetTopic("orders")!.Config);
    }

    [Fact]
    public async Task ABrokerThatIsNotTheControllerRefusesToWriteTheLog()
    {
        var state = new ClusterState();
        state.AddTopic(Topic("orders"));
        _logs = new LogManager(_logDir, new MemoryLogSegmentFactory());
        var config = new ClusteringConfig { BrokerId = 0 };
        var notController = new ClusterController(NullLogger<ClusterController>.Instance, state,
            new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, _logs, config, new TcpPeerTransport()), config);

        var answer = await Server(state, notController).ProcessAsync(SurgewaveOpCode.InterBrokerTopicConfig,
            InterBrokerFrameCodec.EncodePayload(new TopicConfigChangePayload("orders", new Dictionary<string, string> { ["retention.ms"] = "7000" }, [])),
            TestContext.Current.CancellationToken);

        Assert.Equal(ClusterRpcStatus.NotController, StatusOf(answer));
        Assert.False(state.GetTopic("orders")!.Config.ContainsKey("retention.ms"));
    }

    public async ValueTask DisposeAsync()
    {
        if (_transitions is not null)
            await _transitions.DisposeAsync();
        if (_raftNode is not null)
            await _raftNode.DisposeAsync();
        _logs?.Dispose();
        foreach (var dir in new[] { _raftDir, _logDir })
        {
            try { if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true); }
            catch (IOException) { /* a file still held is not a failure */ }
        }
    }

    // Waits on the log manager's change signal, not a poll: each signal is taken before the look.
    private async Task<bool> WaitForAsync(Func<bool> condition, CancellationToken ct)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        deadline.CancelAfter(TimeSpan.FromSeconds(10));
        try
        {
            while (true)
            {
                var changed = _logs!.TopicsChanged;
                if (condition())
                    return true;
                await changed.WaitAsync(deadline.Token);
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return false;
        }
    }

    private NativeInterBrokerServer Server(ClusterState state, ClusterController controller) =>
        new(NullLogger<NativeInterBrokerServer>.Instance,
            new ClusterStateInterBrokerService(NullLogger<ClusterStateInterBrokerService>.Instance, state,
                new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, _logs!, new ClusteringConfig { BrokerId = 0 }, new TcpPeerTransport()),
                _logs!, 0, new NativeInterBrokerServerTests.StubIsrApplier(), topicConfigs: controller));

    private static ClusterRpcStatus StatusOf(byte[] answer)
    {
        var reader = new SurgewavePayloadReader(answer.AsSpan(4));
        Assert.Equal(SurgewaveOpCode.InterBrokerTopicConfig, (SurgewaveOpCode)reader.ReadUInt16());
        return InterBrokerStatusPayload.Read(ref reader).Status;
    }

    // A controller of a cluster of one, on a Raft log of its own.
    private async Task<(ClusterController Controller, ClusterState State)> ControllerAsync(CancellationToken ct)
    {
        var config = new ClusteringConfig
        {
            BrokerId = 0, Host = "localhost", Port = 9092, ReplicationPort = 10092,
            RaftElectionTimeoutMinMs = 150, RaftElectionTimeoutMaxMs = 300, RaftHeartbeatIntervalMs = 50,
            RaftDataDirectory = _raftDir, OffsetsTopicNumPartitions = 1, TransactionStateLogNumPartitions = 1, ShareStateTopicNumPartitions = 1,
        };
        var state = new ClusterState();
        _logs = new LogManager(_logDir, new MemoryLogSegmentFactory());
        var replicas = new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, _logs, config, new TcpPeerTransport());
        var controller = new ClusterController(NullLogger<ClusterController>.Instance, state, replicas, config);
        _raftNode = new RaftNode(NullLogger<RaftNode>.Instance, config, new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            new SingleNodeRaftTransport(), Machine(state));
        controller.SetRaftNode(_raftNode);
        state.AddBroker(new BrokerNode { BrokerId = 0, Host = "localhost", Port = 9092, ReplicationPort = 10092 });
        await controller.StartAsync(ct);
        Assert.True(await TestWaitHelpers.WaitForConditionAsync(() => controller.IsController, timeout: TimeSpan.FromSeconds(10), ct: ct));
        return (controller, state);
    }

    private static MetadataStateMachine Machine(ClusterState state, ReplicationTransitionQueue? transitions = null)
    {
        var config = new ClusteringConfig { BrokerId = 0 };
        return new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, state,
            new ClusterMembershipService(new ClusterIdManager(config, NullLogger<ClusterIdManager>.Instance), state, NullLogger<ClusterMembershipService>.Instance),
            transitions);
    }

    private static TopicMetadata Topic(string name) => new()
    {
        Name = name, TopicId = Guid.NewGuid(), PartitionCount = 1, ReplicationFactor = 1,
        Config = new Dictionary<string, string> { ["cleanup.policy"] = "delete" }, CreatedAt = DateTime.UtcNow,
    };

    private static RaftLogEntry TopicCreated(long index, string topic, List<int> replicas) => new()
    {
        Term = 1,
        Index = index,
        CommandType = MetadataCommandType.TopicCreated,
        Data = JsonSerializer.SerializeToUtf8Bytes(
            new TopicCreatedCommand(topic, Guid.NewGuid(), 1, 1, new Dictionary<string, string> { ["cleanup.policy"] = "delete" },
                [new PartitionAssignment(0, replicas, 1)]),
            ClusteringJsonContext.Default.TopicCreatedCommand),
    };

    private static RaftLogEntry Entry(long index, ConfigChangedCommand command) => new()
    {
        Term = 1,
        Index = index,
        CommandType = MetadataCommandType.ConfigChanged,
        Data = JsonSerializer.SerializeToUtf8Bytes(command, ClusteringJsonContext.Default.ConfigChangedCommand),
    };
}
