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
/// The controller half of #162: a heartbeat reporting a failed log directory moves the broker's leaderships to in-sync
/// replicas elsewhere, takes it out of the ISRs, and keeps it from being elected or readmitted while the directory is
/// offline — what Kafka's controller does with the OfflineLogDirs of a BrokerHeartbeat. A real single-node Raft, so
/// every decision is proposed, committed and applied.
/// </summary>
[Trait("Category", TestCategories.Integration)]
public sealed class OfflineLogDirControllerTests : IAsyncLifetime
{
    private const int SelfBrokerId = 0;
    private const int FailedBrokerId = 7;
    private const int SurvivorBrokerId = 8;

    private readonly string _raftDir = Path.Combine(Path.GetTempPath(), "surgewave-offline-dir-raft-" + Guid.NewGuid().ToString("N"));
    private readonly string _logDir = Path.Combine(Path.GetTempPath(), "surgewave-offline-dir-logs-" + Guid.NewGuid().ToString("N"));

    private ClusterState _state = null!;
    private LogManager _logs = null!;
    private ReplicaManager _replicas = null!;
    private ClusterMembershipService _membership = null!;
    private ClusterController _controller = null!;
    private RaftNode _raftNode = null!;
    private long _failedBrokerEpoch;

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
        _replicas = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, _state, _logs, config,
            new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());
        _controller = new ClusterController(NullLogger<ClusterController>.Instance, _state, _replicas, config);
        _membership = new ClusterMembershipService(
            new ClusterIdManager(config, NullLogger<ClusterIdManager>.Instance), _state,
            NullLogger<ClusterMembershipService>.Instance);
        _controller.SetMembership(_membership);
        _raftNode = new RaftNode(
            NullLogger<RaftNode>.Instance, config,
            new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            new SingleNodeRaftTransport(),
            new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, _state, _membership));

        // The broker whose directory fails, registered as a committed entry would register it.
        _membership.ApplyReplicatedRegistration(
            FailedBrokerId, Guid.NewGuid(), epoch: 3, "10.0.0.7", 9092, rack: null,
            InterBrokerProtocolFeature.Native, replicationPort: null);
        Assert.True(_membership.TryGetBrokerEpoch(FailedBrokerId, out _failedBrokerEpoch));

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
        await _replicas.DisposeAsync();
        _logs.Dispose();
        try { Directory.Delete(_raftDir, recursive: true); } catch { /* best-effort cleanup */ }
        try { Directory.Delete(_logDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private TopicPartition Partition(string topic, int leader, params int[] isr)
    {
        var tp = new TopicPartition { Topic = topic, Partition = 0 };
        _state.AssignReplicas(tp, [FailedBrokerId, SurvivorBrokerId], 1);
        _state.UpdateIsr(tp, [.. isr]);
        _state.ElectLeader(tp, leader);
        return tp;
    }

    private void ReportOffline()
    {
        var outcome = _membership.Heartbeat(new BrokerHeartbeatInput(
            FailedBrokerId, _failedBrokerEpoch, CurrentMetadataOffset: _failedBrokerEpoch,
            WantFence: false, WantShutDown: false, OfflineLogDirs: [Guid.NewGuid()]));
        Assert.Equal(ClusterRpcStatus.None, outcome.Status);
    }

    // The handshake on applied metadata: the signal is taken before each look, so no change in between is missed.
    private async Task UntilAsync(Func<bool> condition, CancellationToken ct)
    {
        while (true)
        {
            var next = _state.PartitionsChanged;
            if (condition())
                return;
            await next.WaitAsync(ct);
        }
    }

    [Fact(Timeout = 60_000)]
    public async Task AHeartbeatReportingAFailedDirectoryMovesTheLeadershipAndShrinksTheIsr()
    {
        var ct = TestContext.Current.CancellationToken;
        var led = Partition("led-by-failed", leader: FailedBrokerId, FailedBrokerId, SurvivorBrokerId);
        var followed = Partition("followed-by-failed", leader: SurvivorBrokerId, SurvivorBrokerId, FailedBrokerId);

        ReportOffline();

        await UntilAsync(() =>
            _state.GetPartitionState(led)!.LeaderBrokerId == SurvivorBrokerId
            && !_state.GetIsrSnapshot(led).Contains(FailedBrokerId)
            && !_state.GetIsrSnapshot(followed).Contains(FailedBrokerId), ct);

        Assert.Equal([SurvivorBrokerId], _state.GetIsrSnapshot(led));
        Assert.Equal([SurvivorBrokerId], _state.GetIsrSnapshot(followed));
        Assert.Equal(SurvivorBrokerId, _state.GetPartitionState(followed)!.LeaderBrokerId);
    }

    [Fact(Timeout = 60_000)]
    public async Task APartitionWithoutAnInSyncSuccessorStaysWithTheBroker()
    {
        var ct = TestContext.Current.CancellationToken;
        var tp = Partition("only-in-sync-on-failed", leader: FailedBrokerId, FailedBrokerId);
        ReportOffline();

        // Called directly as well: it is idempotent, and its answer names what could not move.
        var stillLed = await _controller.MoveReplicasOffOfflineLogDirAsync(FailedBrokerId, ct);

        // The controller's internal topics have replicas on the broker too; this test is about its own partition.
        Assert.Equal([tp], stillLed.Where(p => p.Topic == tp.Topic));
        Assert.Equal(FailedBrokerId, _state.GetPartitionState(tp)!.LeaderBrokerId);
        Assert.Equal([FailedBrokerId], _state.GetIsrSnapshot(tp));
    }

    [Fact(Timeout = 60_000)]
    public async Task ABrokerWithAFailedDirectoryIsNotElectedEvenAsThePreferredLeader()
    {
        var ct = TestContext.Current.CancellationToken;
        var tp = Partition("preferred-failed", leader: SurvivorBrokerId, SurvivorBrokerId, FailedBrokerId);
        ReportOffline();
        var epochBefore = _state.GetPartitionState(tp)!.LeaderEpoch;

        Assert.True(await _controller.ElectLeaderAsync(tp, preferredLeader: FailedBrokerId, ct));

        await UntilAsync(() => _state.GetPartitionState(tp)!.LeaderEpoch > epochBefore, ct);
        Assert.Equal(SurvivorBrokerId, _state.GetPartitionState(tp)!.LeaderBrokerId);
    }

    [Fact(Timeout = 60_000)]
    public async Task ALeadersIsrReportDoesNotReadmitABrokerWithAFailedDirectory()
    {
        var ct = TestContext.Current.CancellationToken;
        var tp = new TopicPartition { Topic = "readmit", Partition = 0 };
        _state.AssignReplicas(tp, [SelfBrokerId, FailedBrokerId], 1);
        _state.UpdateIsr(tp, [SelfBrokerId]);
        _state.ElectLeader(tp, SelfBrokerId);
        ReportOffline();
        var leaderEpoch = _state.GetPartitionState(tp)!.LeaderEpoch;

        // The ISR apply is the only partition change left to happen here, so its signal is the handshake.
        var applied = _state.PartitionsChanged;
        await _controller.ApplyIsrUpdateAsync(tp, SelfBrokerId, leaderEpoch, [SelfBrokerId, FailedBrokerId], ct);
        await applied.WaitAsync(ct);

        Assert.Equal([SelfBrokerId], _state.GetIsrSnapshot(tp));
    }
}
