using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// The controller creates <c>__consumer_offsets</c>, whose partition leaders coordinate groups (#204): as soon as it
/// is controller and knows enough brokers for the replication factor, which is capped at the configured cluster size
/// — a broker on its own gets 1, not a topic that waits for two brokers that will never come. Compacted, as Kafka's.
/// </summary>
[Trait("Category", TestCategories.Integration)]
public sealed class OffsetsTopicCreationTests : IAsyncLifetime
{
    private const int SelfBrokerId = 0;

    private readonly string _raftDir = Path.Combine(Path.GetTempPath(), "surgewave-offsets-topic-" + Guid.NewGuid().ToString("N"));
    private readonly string _logDir = Path.Combine(Path.GetTempPath(), "surgewave-offsets-logs-" + Guid.NewGuid().ToString("N"));

    private ClusterState _state = null!;
    private LogManager _logs = null!;
    private ClusterController _controller = null!;
    private RaftNode _raftNode = null!;

    public ValueTask InitializeAsync()
    {
        var config = new ClusteringConfig
        {
            BrokerId = SelfBrokerId,
            Host = "localhost",
            Port = 9092,
            ReplicationPort = 10092,
            RaftElectionTimeoutMinMs = 150,
            RaftElectionTimeoutMaxMs = 300,
            RaftHeartbeatIntervalMs = 50,
            RaftDataDirectory = _raftDir,
            OffsetsTopicNumPartitions = 4,
        };

        _state = new ClusterState();
        _logs = new LogManager(_logDir, new MemoryLogSegmentFactory());
        var replicaManager = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, _state, _logs, config,
            new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());
        _controller = new ClusterController(NullLogger<ClusterController>.Instance, _state, replicaManager, config);
        var membership = new ClusterMembershipService(
            new ClusterIdManager(config, NullLogger<ClusterIdManager>.Instance), _state,
            NullLogger<ClusterMembershipService>.Instance);
        _raftNode = new RaftNode(
            NullLogger<RaftNode>.Instance, config,
            new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            new SingleNodeRaftTransport(),
            new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, _state, membership));

        _controller.SetRaftNode(_raftNode);
        return ValueTask.CompletedTask;
    }

    // The controller starts the Raft node and the watch that turns Raft leadership into the controller role.
    private async Task StartControllerAsync()
    {
        await _controller.StartAsync(CancellationToken.None);

        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => _raftNode.IsLeader && _controller.IsController,
                timeout: TimeSpan.FromSeconds(10), pollInterval: TimeSpan.FromMilliseconds(50)),
            "the single-node Raft never became leader, so the controller path was never reached");
    }

    public async ValueTask DisposeAsync()
    {
        await _raftNode.DisposeAsync();
        await _controller.DisposeAsync();
        _logs.Dispose();
        try { Directory.Delete(_raftDir, recursive: true); } catch { /* best-effort cleanup */ }
        try { Directory.Delete(_logDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task TheControllerCreatesTheCompactedOffsetsTopicAndLeadsEveryPartition()
    {
        await StartControllerAsync();

        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => _state.GetTopic(CoordinatorPartitioner.GroupTopic) is not null,
                timeout: TimeSpan.FromSeconds(10), pollInterval: TimeSpan.FromMilliseconds(20)),
            "the controller never created __consumer_offsets");

        var topic = _state.GetTopic(CoordinatorPartitioner.GroupTopic)!;
        Assert.Equal(4, topic.PartitionCount);
        Assert.Equal(1, topic.ReplicationFactor);
        Assert.Equal("compact", topic.Config["cleanup.policy"]);

        var locator = new ClusterCoordinatorLocator(_state);
        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => Enumerable.Range(0, 4).All(p =>
                    _state.GetPartitionState(new TopicPartition { Topic = CoordinatorPartitioner.GroupTopic, Partition = p })
                        ?.LeaderBrokerId == SelfBrokerId),
                timeout: TimeSpan.FromSeconds(10), pollInterval: TimeSpan.FromMilliseconds(20)),
            "not every __consumer_offsets partition has this broker as leader");
        Assert.True(locator.IsLocal(CoordinatorKind.Group, "any-group"));
    }

    [Fact]
    public async Task AControllerAssignsThePartitionsAnEarlierControllerLeftWithout()
    {
        // The earlier controller committed TopicCreated and lost its leadership before the partition assignments.
        _state.AddTopic(new TopicMetadata
        {
            Name = CoordinatorPartitioner.GroupTopic,
            TopicId = Guid.NewGuid(),
            PartitionCount = 4,
            ReplicationFactor = 1,
            Config = new Dictionary<string, string> { ["cleanup.policy"] = "compact" },
            CreatedAt = DateTime.UtcNow,
        });

        await StartControllerAsync();

        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => Enumerable.Range(0, 4).All(p =>
                    _state.GetPartitionState(new TopicPartition { Topic = CoordinatorPartitioner.GroupTopic, Partition = p })
                        ?.LeaderBrokerId == SelfBrokerId),
                timeout: TimeSpan.FromSeconds(10), pollInterval: TimeSpan.FromMilliseconds(20)),
            "the controller left __consumer_offsets without partition assignments");
    }
}
