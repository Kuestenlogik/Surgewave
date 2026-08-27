using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// The guards on broker removal (#129).
/// </summary>
/// <remarks>
/// <para>
/// <c>RemoveBrokerViaRaftAsync</c> proposed a <c>BrokerRemoved</c> entry with nothing
/// but a leader check: it would have removed the controller itself, and would have
/// committed an entry for an id that names no broker. It has no callers yet, which is
/// exactly why the guards went in now — they exist before an admin surface or KIP-853
/// voter removal reaches them, rather than after the first accident.
/// </para>
/// <para>
/// These run a real single-node Raft rather than faking leadership, so the accepted
/// case actually proposes, commits and applies. A test that only asserted the refusals
/// would leave the interesting half — that removal still works — unpinned.
/// </para>
/// </remarks>
[Trait("Category", TestCategories.Integration)]
public sealed class BrokerRemovalGuardTests : IAsyncLifetime
{
    private const int SelfBrokerId = 0;
    private const int PeerBrokerId = 7;

    private readonly string _raftDir = Path.Combine(
        Path.GetTempPath(), "surgewave-removal-guard-" + Guid.NewGuid().ToString("N"));
    private readonly string _logDir = Path.Combine(
        Path.GetTempPath(), "surgewave-removal-logs-" + Guid.NewGuid().ToString("N"));

    private ClusteringConfig _config = null!;
    private ClusterState _state = null!;
    private LogManager _logs = null!;
    private ClusterController _controller = null!;
    private RaftNode _raftNode = null!;

    public async ValueTask InitializeAsync()
    {
        _config = new ClusteringConfig
        {
            BrokerId = SelfBrokerId,
            Host = "localhost",
            Port = 9092,
            ReplicationPort = 10092,
            RebalanceCheckIntervalSeconds = 5,
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

        _controller = new ClusterController(
            NullLogger<ClusterController>.Instance, _state, replicaManager, _config);

        var membership = new ClusterMembershipService(
            new ClusterIdManager(_config, NullLogger<ClusterIdManager>.Instance),
            _state,
            NullLogger<ClusterMembershipService>.Instance);

        _raftNode = new RaftNode(
            NullLogger<RaftNode>.Instance,
            _config,
            new RaftPersistence(NullLogger<RaftPersistence>.Instance, _config),
            new SingleNodeRaftTransport(),
            new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, _state, membership));

        await _raftNode.StartAsync(CancellationToken.None);
        _controller.SetRaftNode(_raftNode);

        // A single voter elects itself once the election timeout expires. Without
        // leadership every call answers NotController and the guards never run, which
        // would make these tests pass for the wrong reason.
        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => _raftNode.IsLeader,
                timeout: TimeSpan.FromSeconds(10), pollInterval: TimeSpan.FromMilliseconds(50)),
            "the single-node Raft never became leader, so the guards were never reached");
    }

    public async ValueTask DisposeAsync()
    {
        await _raftNode.DisposeAsync();
        await _controller.DisposeAsync();
        _logs.Dispose();
        try { Directory.Delete(_raftDir, recursive: true); } catch { /* best-effort */ }
        try { Directory.Delete(_logDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task TheActiveControllerRefusesToRemoveItself()
    {
        // Kafka refuses the same thing ("Controller cannot unregister itself while it is
        // active"). The node proposing the entry is the node that has to replicate and
        // apply it — it would be dismantling the identity doing the work.
        _state.UpdateBroker(SelfBrokerId, Broker(SelfBrokerId), _ => Broker(SelfBrokerId));

        var outcome = await _controller.RemoveBrokerViaRaftAsync(SelfBrokerId, CancellationToken.None);

        Assert.Equal(BrokerRemovalOutcome.CannotRemoveSelf, outcome);
        Assert.NotNull(_state.GetBroker(SelfBrokerId));
    }

    [Fact]
    public async Task AnUnknownBrokerIsRefusedRatherThanCommitted()
    {
        // Committing here would replicate an entry that does nothing, and report success
        // for it — a typo would read as a completed decommission.
        var outcome = await _controller.RemoveBrokerViaRaftAsync(4242, CancellationToken.None);

        Assert.Equal(BrokerRemovalOutcome.UnknownBroker, outcome);
    }

    [Fact]
    public async Task AKnownPeerIsRemovedAndForgotten()
    {
        // The half that has to keep working. The entry commits, the state machine applies
        // it through the membership service, and both the node and its registration go.
        _state.UpdateBroker(PeerBrokerId, Broker(PeerBrokerId), _ => Broker(PeerBrokerId));
        Assert.NotNull(_state.GetBroker(PeerBrokerId));

        var outcome = await _controller.RemoveBrokerViaRaftAsync(PeerBrokerId, CancellationToken.None);

        Assert.Equal(BrokerRemovalOutcome.Removed, outcome);
        Assert.Null(_state.GetBroker(PeerBrokerId));
    }

    /// <summary>
    /// A transport with no peers, so the single voter elects itself and commits on its
    /// own. Copied rather than shared because the one in RaftIntegrationTests is private
    /// to that class; sharing it would mean making a test helper public across a file
    /// whose tests are otherwise unrelated to these.
    /// </summary>
    private sealed class SingleNodeRaftTransport : IRaftTransport
    {
        public IReadOnlyList<int> GetPeerIds() => [];

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct) => Task.FromResult(false);

        public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
            => throw new NotSupportedException("no peers in a single-node test");

        public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
            => throw new NotSupportedException("no peers in a single-node test");

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
            => throw new NotSupportedException("no peers in a single-node test");
    }

    private static BrokerNode Broker(int id) => new()
    {
        BrokerId = id,
        Host = "localhost",
        Port = 9092 + id,
    };
}
