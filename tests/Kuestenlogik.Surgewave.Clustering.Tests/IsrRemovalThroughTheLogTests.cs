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
/// When the controller drops a broker from an ISR — it failed, or it is leaving — the change is a metadata log
/// entry (#124). It used to be a write to the controller's own ClusterState: every other broker kept reporting
/// the broker in-sync until the partition leader shrank its ISR on its own, and a controller elected meanwhile
/// rebuilt the ISR with it from the log. A real single-node Raft, so the entry is proposed, committed and applied.
/// </summary>
[Trait("Category", TestCategories.Integration)]
public sealed class IsrRemovalThroughTheLogTests : IAsyncLifetime
{
    private const int SelfBrokerId = 0;
    private const int LeavingBrokerId = 7;

    private readonly string _raftDir = Path.Combine(Path.GetTempPath(), "surgewave-isr-log-" + Guid.NewGuid().ToString("N"));
    private readonly string _logDir = Path.Combine(Path.GetTempPath(), "surgewave-isr-logs-" + Guid.NewGuid().ToString("N"));

    private ClusterState _state = null!;
    private LogManager _logs = null!;
    private ClusterController _controller = null!;
    private RaftNode _raftNode = null!;

    public async ValueTask InitializeAsync()
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

        // The controller starts the Raft node and the watch that turns Raft leadership into the controller role.
        _controller.SetRaftNode(_raftNode);
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
    public async Task ALeavingBrokerIsDroppedFromTheIsrThroughACommittedEntry()
    {
        var tp = new TopicPartition { Topic = "orders", Partition = 0 };
        _state.AssignReplicas(tp, [SelfBrokerId, LeavingBrokerId], 1);
        _state.UpdateIsr(tp, [SelfBrokerId, LeavingBrokerId]);
        _state.ElectLeader(tp, SelfBrokerId);
        var indexBefore = _raftNode.LastLogIndex;

        var stillLed = await _controller.MoveLeadershipAwayAsync(LeavingBrokerId, TestContext.Current.CancellationToken);

        Assert.Empty(stillLed);
        Assert.True(_raftNode.LastLogIndex > indexBefore,
            "no metadata log entry was written: the ISR change stayed on this controller and no other broker learns it");
        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => !_state.GetIsrSnapshot(tp).Contains(LeavingBrokerId),
                timeout: TimeSpan.FromSeconds(10), pollInterval: TimeSpan.FromMilliseconds(20)),
            "the committed ISR change was not applied");
        Assert.Equal([SelfBrokerId], _state.GetIsrSnapshot(tp));
    }
}
