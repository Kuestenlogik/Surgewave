using System.Text.Json;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// A topic is in the metadata log whole or not at all (#217): one entry carries the topic and the replicas of every
/// partition. The assignments used to follow as entries of their own, and a controller that lost its leadership
/// between them left a topic whose partitions no broker led — and a retried CreateTopics got TOPIC_ALREADY_EXISTS.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class AtomicTopicCreationTests : IAsyncDisposable
{
    private readonly string _raftDir = Path.Combine(Path.GetTempPath(), "surgewave-atomic-topic-" + Guid.NewGuid().ToString("N"));
    private readonly string _logDir = Path.Combine(Path.GetTempPath(), "surgewave-atomic-logs-" + Guid.NewGuid().ToString("N"));
    private RaftNode? _raftNode;
    private LogManager? _logs;

    [Fact]
    public void TheTopicsEntryAloneGivesEveryPartitionItsReplicasAndLeader()
    {
        var state = new ClusterState();
        var config = new ClusteringConfig { BrokerId = 0 };
        var machine = new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, state,
            new ClusterMembershipService(new ClusterIdManager(config, NullLogger<ClusterIdManager>.Instance), state, NullLogger<ClusterMembershipService>.Instance));

        // The log cut right after the topic's entry: nothing after it committed.
        machine.Apply(new RaftLogEntry
        {
            Term = 1,
            Index = 1,
            CommandType = MetadataCommandType.TopicCreated,
            Data = JsonSerializer.SerializeToUtf8Bytes(new TopicCreatedCommand("orders", Guid.NewGuid(), 2, 2, null,
                [new PartitionAssignment(0, [1, 2], 1), new PartitionAssignment(1, [2, 1], 1)]), ClusteringJsonContext.Default.TopicCreatedCommand),
        });

        Assert.Equal(2, state.GetTopic("orders")!.PartitionCount);
        Assert.Equal(1, state.GetPartitionState(new TopicPartition { Topic = "orders", Partition = 0 })!.LeaderBrokerId);
        Assert.Equal(2, state.GetPartitionState(new TopicPartition { Topic = "orders", Partition = 1 })!.LeaderBrokerId);
    }

    [Fact]
    public async Task ACreationIsOneEntryOfTheMetadataLog()
    {
        var ct = TestContext.Current.CancellationToken;
        var config = new ClusteringConfig
        {
            BrokerId = 0, Host = "localhost", Port = 9092, ReplicationPort = 10092,
            RaftElectionTimeoutMinMs = 150, RaftElectionTimeoutMaxMs = 300, RaftHeartbeatIntervalMs = 50,
            RaftDataDirectory = _raftDir, OffsetsTopicNumPartitions = 1, TransactionStateLogNumPartitions = 1,
        };
        var state = new ClusterState();
        _logs = new LogManager(_logDir, new MemoryLogSegmentFactory());
        var replicas = new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, _logs, config, new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());
        var controller = new ClusterController(NullLogger<ClusterController>.Instance, state, replicas, config);
        var machine = new RecordingStateMachine(new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, state,
            new ClusterMembershipService(new ClusterIdManager(config, NullLogger<ClusterIdManager>.Instance), state, NullLogger<ClusterMembershipService>.Instance)));
        _raftNode = new RaftNode(NullLogger<RaftNode>.Instance, config, new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            new SingleNodeRaftTransport(), machine);
        controller.SetRaftNode(_raftNode);
        state.AddBroker(new BrokerNode { BrokerId = 0, Host = "localhost", Port = 9092, ReplicationPort = 10092 });
        await controller.StartAsync(ct);
        Assert.True(await TestWaitHelpers.WaitForConditionAsync(() => controller.IsController, timeout: TimeSpan.FromSeconds(10), ct: ct));

        var before = machine.Applied.Count;
        Assert.True(await controller.CreateTopicAsync("orders", 3, 1, null, ct));

        var created = machine.Applied.Skip(before).Where(e => e.CommandType is MetadataCommandType.TopicCreated or MetadataCommandType.PartitionAssigned).ToList();
        Assert.Equal(MetadataCommandType.TopicCreated, Assert.Single(created).CommandType);
        Assert.All(Enumerable.Range(0, 3), p => Assert.Equal(0, state.GetPartitionState(new TopicPartition { Topic = "orders", Partition = p })!.LeaderBrokerId));
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

    // The entries the log applied, in order.
    private sealed class RecordingStateMachine(IRaftStateMachine inner) : IRaftStateMachine
    {
        public List<RaftLogEntry> Applied { get; } = [];

        public void Apply(RaftLogEntry entry)
        {
            lock (Applied)
                Applied.Add(entry);
            inner.Apply(entry);
        }

        public Task<byte[]> CreateSnapshotAsync(CancellationToken ct) => inner.CreateSnapshotAsync(ct);

        public Task RestoreFromSnapshotAsync(byte[] snapshot, CancellationToken ct) => inner.RestoreFromSnapshotAsync(snapshot, ct);
    }
}
