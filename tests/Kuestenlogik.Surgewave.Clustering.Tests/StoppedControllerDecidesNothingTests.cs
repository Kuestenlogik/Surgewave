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
/// A controller that has stopped handles no broker failure: it writes nothing to the metadata log and waits for no
/// commit.
///
/// <para>The handling of a failure runs for as long as the controller does (#234) — and took that from a field the
/// stop clears, so a failure raised after the stop ran with no end at all. Each of its decisions waits five seconds
/// for a commit, which a controller that lost its quorum never gets, once per partition the failed broker is in the
/// ISR of: a broker stopping as the last of its cluster hung in its own shutdown for minutes, the heartbeat monitor
/// awaiting the handler and the shutdown awaiting the monitor (<c>ControllerFailoverTests</c>, "Test execution timed
/// out after 180000 milliseconds" while the last broker was disposed). A real single-node Raft, so a decision this
/// controller made would show in the log.</para>
/// </summary>
[Trait("Category", TestCategories.Integration)]
public sealed class StoppedControllerDecidesNothingTests : IAsyncLifetime
{
    private const int SelfBrokerId = 0;
    private const int FailedBrokerId = 7;

    private readonly string _raftDir = Path.Combine(Path.GetTempPath(), "surgewave-stopped-ctl-" + Guid.NewGuid().ToString("N"));
    private readonly string _logDir = Path.Combine(Path.GetTempPath(), "surgewave-stopped-ctl-logs-" + Guid.NewGuid().ToString("N"));

    private ClusteringConfig _config = null!;
    private ClusterState _state = null!;
    private LogManager _logs = null!;
    private ClusterController _controller = null!;
    private HeartbeatManager _heartbeats = null!;
    private RaftNode _raftNode = null!;

    public async ValueTask InitializeAsync()
    {
        _config = new ClusteringConfig
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
            NullLogger<ReplicaManager>.Instance, _state, _logs, _config,
            new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());
        _controller = new ClusterController(NullLogger<ClusterController>.Instance, _state, replicaManager, _config);
        var membership = new ClusterMembershipService(
            new ClusterIdManager(_config, NullLogger<ClusterIdManager>.Instance), _state,
            NullLogger<ClusterMembershipService>.Instance);
        _raftNode = new RaftNode(
            NullLogger<RaftNode>.Instance, _config,
            new RaftPersistence(NullLogger<RaftPersistence>.Instance, _config),
            new SingleNodeRaftTransport(),
            new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, _state, membership));

        // Never started: the test runs the health check itself, so the failure is raised when the test says.
        _heartbeats = new HeartbeatManager(NullLogger<HeartbeatManager>.Instance, _state, _config);
        _controller.SetHeartbeatManager(_heartbeats);

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
        await _controller.DisposeAsync();
        await _heartbeats.DisposeAsync();
        await _raftNode.DisposeAsync();
        _logs.Dispose();
        try { Directory.Delete(_raftDir, recursive: true); } catch { /* best-effort cleanup */ }
        try { Directory.Delete(_logDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact(Timeout = 60_000)]
    public async Task ABrokerFailureRaisedAfterTheControllerStopped_WritesNothingToTheMetadataLog()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var tp = new TopicPartition { Topic = "orders", Partition = 0 };
        _state.AssignReplicas(tp, [SelfBrokerId, FailedBrokerId], 1);
        _state.UpdateIsr(tp, [SelfBrokerId, FailedBrokerId]);
        _state.ElectLeader(tp, SelfBrokerId);

        // A watched peer past the failure count: the next health check reports it failed.
        _ = _heartbeats.ProcessHeartbeat(new HeartbeatRequest(FailedBrokerId, 0, 0, SelfBrokerId, 0));
        for (var i = 0; i < _config.MaxHeartbeatFailures; i++)
            _heartbeats.AllBrokerHealth[FailedBrokerId].RecordFailure();

        // A broker's shutdown stops the controller before the heartbeat manager, whose monitor may be about to raise
        // a failure. The Raft node still leads: stopping the controller does not end its term.
        await _controller.DisposeAsync();
        var indexBefore = _raftNode.LastLogIndex;

        await _heartbeats.CheckBrokerHealthAsync(cancellationToken);

        Assert.False(_heartbeats.IsBrokerAlive(FailedBrokerId), "the failure was never raised, so nothing was tested");
        Assert.Equal(indexBefore, _raftNode.LastLogIndex);
        Assert.Contains(FailedBrokerId, _state.GetIsrSnapshot(tp));
    }
}
