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
/// A group's configs are entries of the metadata log (#228), so every broker — a share partition's leader as much as
/// the group's coordinator — applies a share group's limits. They lived with the group on its coordinator only, and a
/// leader elsewhere ran the group with Kafka's defaults.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class GroupConfigMetadataTests : IAsyncDisposable
{
    private readonly string _raftDir = Path.Combine(Path.GetTempPath(), "surgewave-group-config-" + Guid.NewGuid().ToString("N"));
    private readonly string _logDir = Path.Combine(Path.GetTempPath(), "surgewave-group-config-logs-" + Guid.NewGuid().ToString("N"));
    private RaftNode? _raftNode;
    private LogManager? _logs;

    [Fact]
    public async Task EveryBrokerReadsTheConfigsFromTheLogAndASnapshotKeepsThem()
    {
        var state = new ClusterState();
        var machine = Machine(state);

        machine.Apply(Entry(1, new GroupConfigChangedCommand("share-g", "share.delivery.count.limit", "3")));
        machine.Apply(Entry(2, new GroupConfigChangedCommand("share-g", "share.renew.acknowledge.enable", "false")));
        machine.Apply(Entry(3, new GroupConfigChangedCommand("share-g", "share.renew.acknowledge.enable", null)));

        Assert.Equal(new Dictionary<string, string> { ["share.delivery.count.limit"] = "3" }, state.GroupConfigsOf("share-g"));

        var restored = new ClusterState();
        await Machine(restored).RestoreFromSnapshotAsync(await machine.CreateSnapshotAsync(TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
        Assert.Equal("3", restored.GroupConfigsOf("share-g")["share.delivery.count.limit"]);
    }

    [Fact]
    public async Task TheControllerWritesAChangeToTheLogAndAnswersOnceCommitted()
    {
        var ct = TestContext.Current.CancellationToken;
        var (controller, state) = await ControllerAsync(ct);
        var store = new ClusterGroupConfigStore(state, controller);

        Assert.Equal(GroupConfigWriteStatus.Ok, await store.SetAsync("share-g", "share.delivery.count.limit", "4", ct));
        Assert.Equal("4", store.ConfigsOf("share-g")["share.delivery.count.limit"]);

        Assert.Equal(GroupConfigWriteStatus.Ok, await store.SetAsync("share-g", "share.delivery.count.limit", null, ct));
        Assert.Empty(store.ConfigsOf("share-g"));
    }

    [Fact]
    public async Task ABrokerThatIsNotTheControllerAsksIt()
    {
        var ct = TestContext.Current.CancellationToken;
        var (controller, state) = await ControllerAsync(ct);
        var server = new NativeInterBrokerServer(NullLogger<NativeInterBrokerServer>.Instance,
            new ClusterStateInterBrokerService(NullLogger<ClusterStateInterBrokerService>.Instance, state,
                new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, _logs!, new ClusteringConfig { BrokerId = 0 }, new TcpPeerTransport()),
                _logs!, 0, new NativeInterBrokerServerTests.StubIsrApplier(), groupConfigs: controller));

        var answer = await server.ProcessAsync(SurgewaveOpCode.InterBrokerGroupConfig,
            InterBrokerFrameCodec.EncodePayload(new GroupConfigPayload("share-g", "share.partition.max.record.locks", "500")), ct);

        var reader = new SurgewavePayloadReader(answer.AsSpan(4));
        Assert.Equal(SurgewaveOpCode.InterBrokerGroupConfig, (SurgewaveOpCode)reader.ReadUInt16());
        Assert.Equal(ClusterRpcStatus.None, InterBrokerStatusPayload.Read(ref reader).Status);
        Assert.Equal("500", state.GroupConfigsOf("share-g")["share.partition.max.record.locks"]);
    }

    [Fact]
    public async Task ABrokerThatIsNotTheControllerRefusesToWriteTheLog()
    {
        var state = new ClusterState();
        _logs = new LogManager(_logDir, new MemoryLogSegmentFactory());
        var config = new ClusteringConfig { BrokerId = 0 };
        var replicas = new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, _logs, config, new TcpPeerTransport());
        var notController = new ClusterController(NullLogger<ClusterController>.Instance, state, replicas, config);
        var server = new NativeInterBrokerServer(NullLogger<NativeInterBrokerServer>.Instance,
            new ClusterStateInterBrokerService(NullLogger<ClusterStateInterBrokerService>.Instance, state, replicas, _logs, 0,
                new NativeInterBrokerServerTests.StubIsrApplier(), groupConfigs: notController));

        var answer = await server.ProcessAsync(SurgewaveOpCode.InterBrokerGroupConfig,
            InterBrokerFrameCodec.EncodePayload(new GroupConfigPayload("share-g", "share.delivery.count.limit", "3")), TestContext.Current.CancellationToken);

        var reader = new SurgewavePayloadReader(answer.AsSpan(4));
        reader.ReadUInt16();
        Assert.Equal(ClusterRpcStatus.NotController, InterBrokerStatusPayload.Read(ref reader).Status);
        Assert.Empty(state.GroupConfigsOf("share-g"));
    }

    public async ValueTask DisposeAsync()
    {
        if (_raftNode is not null)
            await _raftNode.DisposeAsync();
        _logs?.Dispose();
        foreach (var dir in new[] { _raftDir, _logDir })
        {
            try { if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true); }
            catch (IOException) { /* a file still held is not a failure */ }
        }
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

    private static MetadataStateMachine Machine(ClusterState state)
    {
        var config = new ClusteringConfig { BrokerId = 0 };
        return new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, state,
            new ClusterMembershipService(new ClusterIdManager(config, NullLogger<ClusterIdManager>.Instance), state, NullLogger<ClusterMembershipService>.Instance));
    }

    private static RaftLogEntry Entry(long index, GroupConfigChangedCommand command) => new()
    {
        Term = 1,
        Index = index,
        CommandType = MetadataCommandType.GroupConfigChanged,
        Data = JsonSerializer.SerializeToUtf8Bytes(command, ClusteringJsonContext.Default.GroupConfigChangedCommand),
    };
}
