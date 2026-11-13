using System.Text.Json;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// The startup handshake (#250): a broker says it has caught up with the metadata log only once it has applied it.
/// </summary>
/// <remarks>
/// A node starts with its log loaded and none of it applied, because the commit index is not persisted. A broker that
/// reported ready in that state answered a client from an empty state machine — a topic created before the restart
/// "does not exist" — or, while the log was being applied, from the middle of it: the topic without a config change
/// made after its creation ("retention.ms did not survive the restart"). Nothing here waits for time to pass: a
/// follower is handed its leader's requests one by one, and the lone voter is awaited on the handshake itself.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class MetadataCatchUpTests : IAsyncDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "surgewave-catch-up-" + Guid.NewGuid().ToString("N"));
    private readonly List<IAsyncDisposable> _disposables = [];
    private readonly List<IDisposable> _logManagers = [];

    public async ValueTask DisposeAsync()
    {
        foreach (var disposable in _disposables)
            await disposable.DisposeAsync();
        foreach (var logs in _logManagers)
            logs.Dispose();

        try { if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true); }
        catch (IOException) { /* a test host still holding the log file is not a failure */ }
    }

    [Fact]
    public async Task ARestartedLoneBrokerIsCaughtUpOnceItLeadsAndHasAppliedTheLogItStartedWith()
    {
        var ct = TestContext.Current.CancellationToken;
        var config = NewConfig(brokerId: 5);

        // What an earlier run of the broker left on disk: a topic, and a config change made after its creation.
        await new RaftPersistence(NullLogger<RaftPersistence>.Instance, config).AppendEntriesAsync(
        [
            Noop(index: 1, term: 1),
            TopicCreated(index: 2, term: 1, "payments", replicas: [5]),
            ConfigChanged(index: 3, term: 1, "payments", "retention.ms", "987654321"),
        ]);

        var broker = NewBroker(config, new SingleNodeRaftTransport());
        var leadership = new List<RaftLeadership>();
        broker.Node.LeadershipChanged += leadership.Add;

        // Not started, so nothing is applied, and the broker does not say otherwise.
        var caughtUp = broker.Controller.WaitForMetadataCaughtUpAsync(ct);
        Assert.False(caughtUp.IsCompleted);
        Assert.False(broker.Controller.IsMetadataCaughtUp);
        Assert.Null(broker.State.GetTopic("payments"));

        await broker.Controller.StartAsync(ct);
        await caughtUp;

        // Everything a client asks about is there on the line after the handshake: the broker leads and is the
        // controller, the topic has the config changed after its creation, and the broker runs the partition with it.
        Assert.True(broker.Controller.IsMetadataCaughtUp);
        Assert.True(broker.Node.IsCaughtUp);
        Assert.Contains(leadership, l => l.IsLeader);
        Assert.True(broker.Controller.IsController);
        Assert.Equal("987654321", broker.State.GetTopic("payments")!.Config["retention.ms"]);
        Assert.Equal("987654321", broker.Logs.GetTopicMetadata("payments")!.Config["retention.ms"]);
        Assert.True(broker.Replicas.IsLeader(new() { Topic = "payments", Partition = 0 }));
    }

    [Fact]
    public async Task AFollowerIsCaughtUpOnlyByACommitIndexOfItsLeadersOwnTerm()
    {
        var ct = TestContext.Current.CancellationToken;
        // Never started, so it does not campaign: what it knows is what the leader's requests below tell it.
        var follower = NewNode(NewConfig(brokerId: 2, quorum: "1@localhost:19091,2@localhost:19092,3@localhost:19093"), out var applied);
        var caughtUp = follower.WaitForCaughtUpAsync(ct);

        // The whole cluster restarted and node 1 won term 2. Its first request brings its log, with the no-op of its
        // term at the end, and the commit index of a node that has only just started: 0.
        RaftLogEntry[] log =
        [
            Noop(index: 1, term: 1),
            TopicCreated(index: 2, term: 1, "payments", replicas: [1]),
            ConfigChanged(index: 3, term: 1, "payments", "retention.ms", "987654321"),
            Noop(index: 4, term: 2),
        ];
        Assert.True((await follower.HandleAppendEntriesAsync(new AppendEntriesRequest(2, 1, 0, 0, log, LeaderCommit: 0), ct)).Success);
        Assert.False(caughtUp.IsCompleted);
        Assert.Empty(applied);

        // A commit index short of the leader's no-op is what the leader knew as a follower of the term before. What
        // that term's leader committed after it is in the leader's log, and not known to be committed yet.
        Assert.True((await follower.HandleAppendEntriesAsync(new AppendEntriesRequest(2, 1, 4, 2, [], LeaderCommit: 2), ct)).Success);
        Assert.False(caughtUp.IsCompleted);
        Assert.False(follower.IsCaughtUp);

        // The no-op committed: the leader's log up to it is committed, and the follower has applied all of it.
        Assert.True((await follower.HandleAppendEntriesAsync(new AppendEntriesRequest(2, 1, 4, 2, [], LeaderCommit: 4), ct)).Success);
        await caughtUp;
        Assert.True(follower.IsCaughtUp);
        Assert.Equal([2L, 3L], applied);
        Assert.Equal(1, follower.LeaderId);
    }

    [Fact]
    public async Task ABrokerThatStopsBeforeItCaughtUpTellsWhoeverWaits()
    {
        // A voter of three that is alone: no quorum, no leader, nothing to catch up with.
        var broker = NewBroker(
            NewConfig(brokerId: 2, quorum: "1@localhost:19091,2@localhost:19092,3@localhost:19093"), new UnreachablePeersTransport());
        await broker.Controller.StartAsync(TestContext.Current.CancellationToken);
        var caughtUp = broker.Controller.WaitForMetadataCaughtUpAsync(CancellationToken.None);
        var node = broker.Node.WaitForCaughtUpAsync(CancellationToken.None);
        Assert.False(caughtUp.IsCompleted);

        await broker.Controller.DisposeAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => caughtUp);
        Assert.False(broker.Controller.IsMetadataCaughtUp);

        await broker.Node.DisposeAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => node);
    }

    [Fact]
    public async Task TheTransitionQueueTellsWhenWhatWasQueuedIsCarriedOut()
    {
        var ct = TestContext.Current.CancellationToken;
        var broker = NewBroker(NewConfig(brokerId: 5), new SingleNodeRaftTransport());
        broker.State.AddBroker(new BrokerNode { BrokerId = 5, Host = "localhost", Port = 9092 });
        var machine = new MetadataStateMachine(
            NullLogger<MetadataStateMachine>.Instance, broker.State, TestRaftNode.NewMembership(NewConfig(5), broker.State), broker.Transitions);

        machine.Apply(TopicCreated(index: 1, term: 1, "orders", replicas: [5]));
        machine.Apply(ConfigChanged(index: 2, term: 1, "orders", "retention.ms", "5000"));
        await broker.Transitions.WhenCarriedOutAsync(ct);

        Assert.True(broker.Replicas.IsLeader(new() { Topic = "orders", Partition = 0 }));
        Assert.Equal("5000", broker.Logs.GetTopicMetadata("orders")!.Config["retention.ms"]);

        // A queue that stopped carries nothing out any more, and says so.
        _disposables.Remove(broker.Transitions);
        await broker.Transitions.DisposeAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => broker.Transitions.WhenCarriedOutAsync(ct));
    }

    private ClusteringConfig NewConfig(int brokerId, string quorum = "") => new()
    {
        BrokerId = brokerId,
        Host = "localhost",
        Port = 9092,
        ReplicationPort = 10092,
        RebalanceCheckIntervalSeconds = 5,
        RaftElectionTimeoutMinMs = 50,
        RaftElectionTimeoutMaxMs = 100,
        RaftHeartbeatIntervalMs = 25,
        RaftPeerDiscoveryTimeoutSeconds = 0,
        RaftDataDirectory = Path.Combine(_root, "raft-" + Guid.NewGuid().ToString("N")),
        ControllerQuorumVoters = quorum,
    };

    // A node on its own, with a state machine that records what it is given to apply.
    private RaftNode NewNode(ClusteringConfig config, out List<long> applied)
    {
        var recording = new RecordingStateMachine();
        applied = recording.Applied;
        var node = new RaftNode(
            NullLogger<RaftNode>.Instance, config, new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            new UnreachablePeersTransport(), recording);
        _disposables.Add(node);
        return node;
    }

    // A broker's metadata side as a host wires it: node, state machine, transition queue, replica manager, controller.
    private Broker NewBroker(ClusteringConfig config, IRaftTransport transport)
    {
        var state = new ClusterState();
        var logs = new LogManager(Path.Combine(_root, "logs-" + Guid.NewGuid().ToString("N")), new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _logManagers.Add(logs);
        var replicas = new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, logs, config, new TcpPeerTransport());
        var transitions = new ReplicationTransitionQueue(replicas, config.BrokerId, NullLogger<ReplicationTransitionQueue>.Instance);
        var node = new RaftNode(
            NullLogger<RaftNode>.Instance, config, new RaftPersistence(NullLogger<RaftPersistence>.Instance, config), transport,
            new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, state, TestRaftNode.NewMembership(config, state), transitions));
        var controller = new ClusterController(NullLogger<ClusterController>.Instance, state, replicas, config);
        controller.SetRaftNode(node);
        controller.SetReplicationTransitions(transitions);

        // The controller before the node it follows, the queue after both.
        _disposables.Add(controller);
        _disposables.Add(node);
        _disposables.Add(transitions);
        return new Broker(node, controller, state, logs, replicas, transitions);
    }

    private sealed record Broker(
        RaftNode Node, ClusterController Controller, ClusterState State, LogManager Logs, ReplicaManager Replicas,
        ReplicationTransitionQueue Transitions);

    private static RaftLogEntry Noop(long index, int term) => new()
    {
        Term = term, Index = index, CommandType = MetadataCommandType.Noop, Data = [],
    };

    private static RaftLogEntry TopicCreated(long index, int term, string topic, List<int> replicas) => new()
    {
        Term = term,
        Index = index,
        CommandType = MetadataCommandType.TopicCreated,
        Data = JsonSerializer.SerializeToUtf8Bytes(
            new TopicCreatedCommand(topic, Guid.NewGuid(), 1, 1, [], [new PartitionAssignment(0, replicas, 1)]),
            ClusteringJsonContext.Default.TopicCreatedCommand),
    };

    private static RaftLogEntry ConfigChanged(long index, int term, string topic, string name, string value) => new()
    {
        Term = term,
        Index = index,
        CommandType = MetadataCommandType.ConfigChanged,
        Data = JsonSerializer.SerializeToUtf8Bytes(
            new ConfigChangedCommand(topic, new Dictionary<string, string> { [name] = value }, []),
            ClusteringJsonContext.Default.ConfigChangedCommand),
    };

    private sealed class RecordingStateMachine : IRaftStateMachine
    {
        public List<long> Applied { get; } = [];

        public void Apply(RaftLogEntry entry) => Applied.Add(entry.Index);

        public Task<byte[]> CreateSnapshotAsync(CancellationToken ct) => Task.FromResult(Array.Empty<byte>());

        public Task RestoreFromSnapshotAsync(byte[] snapshot, CancellationToken ct) => Task.CompletedTask;
    }

    /// <summary>The other voters of a quorum, none of them running: every request to them fails.</summary>
    private sealed class UnreachablePeersTransport : IRaftTransport
    {
        public IReadOnlyList<int> GetPeerIds() => [1, 3];

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct) => Task.FromResult(false);

        public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
            => Task.FromException<PreVoteResponse>(new RaftTransportException("not running"));

        public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
            => Task.FromException<RequestVoteResponse>(new RaftTransportException("not running"));

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
            => Task.FromException<AppendEntriesResponse>(new RaftTransportException("not running"));
    }
}
