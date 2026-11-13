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
/// A controller that takes office handles the peers its failure detector already holds as failed (#252).
/// </summary>
/// <remarks>
/// The detector reports a failure once, and a broker that is not the controller drops the report: handling it is the
/// controller's. A broker that saw the old controller fail before it was promoted itself had dropped it by the time
/// it took office, and nothing looked at the peers' health then — the dead broker stayed in every ISR and kept its
/// leaderships. The failure is raised here before the controller is started, so before its node leads; a real
/// single-node Raft, and the wait is for the cluster state's own signal.
/// </remarks>
[Trait("Category", TestCategories.Integration)]
public sealed class ControllerTakesOfficeWithFailedPeersTests : IAsyncDisposable
{
    private const int SelfBrokerId = 0;
    private const int FailedBrokerId = 7;

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private readonly string _raftDir = Path.Combine(Path.GetTempPath(), "surgewave-office-failed-" + Guid.NewGuid().ToString("N"));
    private readonly string _logDir = Path.Combine(Path.GetTempPath(), "surgewave-office-failed-logs-" + Guid.NewGuid().ToString("N"));

    private readonly ClusteringConfig _config;
    private readonly ClusterState _state = new();
    private readonly LogManager _logs;
    private readonly ClusterController _controller;
    private readonly HeartbeatManager _heartbeats;
    private readonly RaftNode _raftNode;

    public ControllerTakesOfficeWithFailedPeersTests()
    {
        _config = new ClusteringConfig
        {
            BrokerId = SelfBrokerId,
            Host = "localhost",
            Port = 9092,
            ReplicationPort = 10092,
            RaftElectionTimeoutMinMs = 50,
            RaftElectionTimeoutMaxMs = 100,
            RaftHeartbeatIntervalMs = 25,
            RaftPeerDiscoveryTimeoutSeconds = 0,
            RaftDataDirectory = _raftDir,
        };

        _logs = new LogManager(_logDir, new MemoryLogSegmentFactory());
        var replicaManager = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, _state, _logs, _config,
            new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());
        _controller = new ClusterController(NullLogger<ClusterController>.Instance, _state, replicaManager, _config);
        _raftNode = new RaftNode(
            NullLogger<RaftNode>.Instance, _config,
            new RaftPersistence(NullLogger<RaftPersistence>.Instance, _config),
            new SingleNodeRaftTransport(),
            new MetadataStateMachine(
                NullLogger<MetadataStateMachine>.Instance, _state, TestRaftNode.NewMembership(_config, _state)));

        // Never started: the test runs the health check itself, so the failure is raised when the test says.
        _heartbeats = new HeartbeatManager(NullLogger<HeartbeatManager>.Instance, _state, _config);
        _controller.SetHeartbeatManager(_heartbeats);
        _controller.SetRaftNode(_raftNode);
    }

    public async ValueTask DisposeAsync()
    {
        await _controller.DisposeAsync();
        await _heartbeats.DisposeAsync();
        await _raftNode.DisposeAsync();
        _logs.Dispose();
        try { Directory.Delete(_raftDir, recursive: true); } catch { /* best-effort cleanup */ }
        try { Directory.Delete(_logDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task AFailureReportedBeforeTheBrokerWasTheController_IsHandledWhenItTakesOffice()
    {
        var ct = TestContext.Current.CancellationToken;
        var led = new TopicPartition { Topic = "orders", Partition = 0 };
        var followed = new TopicPartition { Topic = "orders", Partition = 1 };
        foreach (var tp in new[] { led, followed })
        {
            _state.AssignReplicas(tp, [FailedBrokerId, SelfBrokerId], 1);
            _state.UpdateIsr(tp, [FailedBrokerId, SelfBrokerId]);
        }
        _state.ElectLeader(led, FailedBrokerId);
        _state.ElectLeader(followed, SelfBrokerId);

        // A watched peer past the failure count: the health check reports it failed, to a broker that is not the
        // controller — its node has not been started.
        _ = _heartbeats.ProcessHeartbeat(new HeartbeatRequest(FailedBrokerId, 0, 0, FailedBrokerId, 0));
        for (var i = 0; i < _config.MaxHeartbeatFailures; i++)
            _heartbeats.AllBrokerHealth[FailedBrokerId].RecordFailure();
        await _heartbeats.CheckBrokerHealthAsync(ct);

        Assert.False(_heartbeats.IsBrokerAlive(FailedBrokerId));
        Assert.False(_controller.IsController);
        Assert.Contains(FailedBrokerId, _state.GetIsrSnapshot(led));

        await _controller.StartAsync(ct);

        // The office is taken by a node that is its own majority; what it decides shows in the partitions.
        while (true)
        {
            var changed = _state.PartitionsChanged;
            if (!_state.GetIsrSnapshot(led).Contains(FailedBrokerId)
                && !_state.GetIsrSnapshot(followed).Contains(FailedBrokerId)
                && _state.GetPartitionState(led)!.LeaderBrokerId == SelfBrokerId)
            {
                break;
            }

            await changed.WaitAsync(Bound, ct);
        }

        Assert.True(_controller.IsController);
        Assert.Equal([SelfBrokerId], _state.GetIsrSnapshot(led));
        Assert.Equal([SelfBrokerId], _state.GetIsrSnapshot(followed));
        Assert.Equal(SelfBrokerId, _state.GetPartitionState(followed)!.LeaderBrokerId);
    }
}
