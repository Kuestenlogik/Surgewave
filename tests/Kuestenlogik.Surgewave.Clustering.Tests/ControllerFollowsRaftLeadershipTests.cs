using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// The controller a broker reports, and whether it is the controller itself, follow its Raft node's leadership the
/// moment the node knows it (#180).
/// </summary>
/// <remarks>
/// A loop used to carry the node's leader over to <see cref="ClusterState.ControllerId"/> every 100 ms. Until its next
/// look a broker that had already heard from the leader reported no controller: Metadata answered -1, on which an
/// admin client waits for a controller until its request times out ("Failed while waiting for controller: Local: Timed
/// out"), and since #235 it names the asked broker itself. Nothing here waits: every assertion is made on the line
/// after the node learned of the change.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class ControllerFollowsRaftLeadershipTests : IAsyncDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private readonly string _root = Path.Combine(Path.GetTempPath(), "surgewave-leadership-" + Guid.NewGuid().ToString("N"));
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
    public async Task AFollowerNamesTheLeaderTheMomentItsNodeHearsFromIt()
    {
        var broker = await StartFollowerAsync();
        Assert.Equal(-1, broker.State.ControllerId);

        // Leaders change from term to term; after each first word from the new one the broker names it. Twenty in a
        // row, each asked at once: a look every 100 ms cannot have fallen between every one of them and its question.
        for (var term = 1; term <= 20; term++)
        {
            var leader = term % 2 == 1 ? 1 : 3;

            var response = await broker.Node.HandleAppendEntriesAsync(Heartbeat(term, leader), TestContext.Current.CancellationToken);

            Assert.True(response.Success);
            Assert.Equal(leader, broker.Node.LeaderId);
            Assert.Equal(leader, broker.State.ControllerId);
            Assert.False(broker.Controller.IsController);
        }
    }

    [Fact]
    public async Task MetadataNamesTheLeaderTheMomentTheNodeHearsFromIt()
    {
        var broker = await StartFollowerAsync();
        var handler = new MetadataApiHandler(
            new BrokerConfig { BrokerId = broker.Node.NodeId }, broker.Logs, NullLogger<MetadataApiHandler>.Instance);
        handler.SetClusterState(broker.State);

        // Before the first word from a leader the broker names itself (#235) — it knows no other.
        Assert.Equal(broker.Node.NodeId, (await MetadataAsync(handler)).ControllerId);

        await broker.Node.HandleAppendEntriesAsync(Heartbeat(term: 1, leader: 1), TestContext.Current.CancellationToken);

        Assert.Equal(1, (await MetadataAsync(handler)).ControllerId);
    }

    [Fact]
    public async Task ALeadershipLostAndWonAgainIsANewReignFromTheMomentTheNodeLeads()
    {
        var config = NewConfig(brokerId: 1);
        var state = new ClusterState();
        var node = TestRaftNode.ForSingleNode(config, state, TestRaftNode.NewMembership(config, state));
        _disposables.Add(node);
        var controller = NewController(config, state, node, out _);
        await controller.StartAsync(TestContext.Current.CancellationToken);

        // The first election, awaited on the cluster state's own signal.
        while (true)
        {
            var changed = state.MembershipChanged;
            if (controller.IsController)
                break;
            await changed.WaitAsync(Timeout, TestContext.Current.CancellationToken);
        }

        Assert.Equal(1, state.ControllerId);
        var firstReign = state.ControllerEpoch;

        // Subscribed after the controller, so told after it: what this sees is what the controller made of the change.
        var reelected = new TaskCompletionSource<(bool IsController, int Epoch)>(TaskCreationOptions.RunContinuationsAsynchronously);
        node.LeadershipChanged += leadership =>
        {
            if (leadership.IsLeader)
                reelected.TrySetResult((controller.IsController, state.ControllerEpoch));
        };

        await node.StepDownAsync();

        Assert.False(controller.IsController, "a broker whose node has stepped down is not the controller");

        // The node is its own majority and wins the next term.
        var (isController, epoch) = await reelected.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.True(isController, "the node led the term, and its broker was not the controller yet");
        Assert.True(epoch > firstReign, $"the second reign has epoch {epoch}, the first had {firstReign}");
    }

    [Fact]
    public async Task TheControllerLoopEndsWithTheReign_AndEachReignHasOne()
    {
        // The loop that looks at the balance was started with each office won and never stopped with an office lost
        // (#250): a broker in its fifth reign ran five of them, four as a controller it no longer was.
        var ct = TestContext.Current.CancellationToken;
        var config = NewConfig(brokerId: 1);
        var state = new ClusterState();
        var node = TestRaftNode.ForSingleNode(config, state, TestRaftNode.NewMembership(config, state));
        _disposables.Add(node);
        var controller = NewController(config, state, node, out _);

        // Completed by each term the node leads, after the controller has followed it: subscribed after it.
        var leads = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await controller.LoadAsync(ct);
        node.LeadershipChanged += leadership =>
        {
            if (leadership.IsLeader)
                Volatile.Read(ref leads).TrySetResult();
        };
        await controller.StartAsync(ct);
        await Volatile.Read(ref leads).Task.WaitAsync(Timeout, ct);

        for (var reign = 1; reign <= 5; reign++)
        {
            var loop = controller.ControllerLoop;
            Assert.NotNull(loop);
            Assert.False(loop.IsCompleted, $"the loop of reign {reign} ended while the broker was the controller");

            var next = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Volatile.Write(ref leads, next);
            await node.StepDownAsync();

            // The loop of the reign that is over ends with it, and is not waited out: it looks every five seconds.
            // Ended or never begun: a reign that ends before its loop has run cancels it.
            await Task.WhenAny(loop).WaitAsync(Timeout, ct);

            // The node is its own majority and wins the next term.
            await next.Task.WaitAsync(Timeout, ct);
            Assert.True(controller.IsController);
            Assert.NotSame(loop, controller.ControllerLoop);
            Assert.InRange(controller.ControllerLoopsRunning, 0, 1);
        }
    }

    [Fact]
    public async Task ABrokerIsTheControllerBeforeItsFirstHeartbeatLeaves()
    {
        // A follower names the leader it hears from, and a client sends its admin requests there. The broker that
        // sends the heartbeat has taken the role by then: a new leader used to answer NOT_CONTROLLER for up to 100 ms.
        var config = NewConfig(brokerId: 1, quorum: "1@localhost:19093,2@localhost:19094");
        config.RaftElectionTimeoutMinMs = 50;
        config.RaftElectionTimeoutMaxMs = 100;
        config.RaftHeartbeatIntervalMs = 25;
        var state = new ClusterState();
        var transport = new GrantingTransport();
        var node = NewNode(config, state, transport);
        var controller = NewController(config, state, node, out _);
        var atFirstHeartbeat = new TaskCompletionSource<(bool IsController, int ControllerId)>(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.OnAppendEntries = () => atFirstHeartbeat.TrySetResult((controller.IsController, state.ControllerId));

        await controller.StartAsync(TestContext.Current.CancellationToken);

        var (isController, controllerId) = await atFirstHeartbeat.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.True(isController, "the leader's heartbeat left before its broker was the controller");
        Assert.Equal(1, controllerId);
    }

    [Fact]
    public async Task TheNodeAnnouncesEachChangeOfLeadershipOnceAndInOrder()
    {
        var config = NewConfig(brokerId: 2, observerOfVoter: 1);
        var state = new ClusterState();
        var node = NewNode(config, state);
        var announced = new List<RaftLeadership>();
        node.LeadershipChanged += announced.Add;

        await node.HandleAppendEntriesAsync(Heartbeat(term: 1, leader: 1), TestContext.Current.CancellationToken);
        await node.HandleAppendEntriesAsync(Heartbeat(term: 1, leader: 1), TestContext.Current.CancellationToken);
        await node.HandleAppendEntriesAsync(Heartbeat(term: 2, leader: 3), TestContext.Current.CancellationToken);
        await node.HandleRequestVoteAsync(
            new RequestVoteRequest(Term: 5, CandidateId: 1, LastLogIndex: 0, LastLogTerm: 0), TestContext.Current.CancellationToken);

        Assert.Equal(
            [
                new RaftLeadership(Term: 1, LeaderId: 1, IsLeader: false),
                new RaftLeadership(Term: 2, LeaderId: 3, IsLeader: false),
                new RaftLeadership(Term: 5, LeaderId: null, IsLeader: false),
            ],
            announced);
        Assert.Equal(announced[^1], node.Leadership);
    }

    private static AppendEntriesRequest Heartbeat(int term, int leader)
        => new(term, leader, PrevLogIndex: 0, PrevLogTerm: 0, Entries: [], LeaderCommit: 0);

    private static async Task<MetadataResponse> MetadataAsync(MetadataApiHandler handler)
        => Assert.IsType<MetadataResponse>(await handler.HandleAsync(
            new MetadataRequest
            {
                ApiKey = ApiKey.Metadata,
                ApiVersion = 9,
                CorrelationId = 1,
                ClientId = "admin",
                Topics = [],
            },
            new RequestContext { ConnectionState = new ConnectionState("test-host"), ClientId = "admin" },
            TestContext.Current.CancellationToken));

    // A broker outside the controller quorum: it follows whoever leads and never campaigns, so the only leadership
    // changes it sees are the ones a test hands it.
    private async Task<(RaftNode Node, ClusterController Controller, ClusterState State, LogManager Logs)> StartFollowerAsync()
    {
        var config = NewConfig(brokerId: 2, observerOfVoter: 1);
        var state = new ClusterState();
        var node = NewNode(config, state);
        var controller = NewController(config, state, node, out var logs);
        await controller.StartAsync(TestContext.Current.CancellationToken);
        return (node, controller, state, logs);
    }

    private ClusteringConfig NewConfig(int brokerId, int? observerOfVoter = null, string? quorum = null) => new()
    {
        BrokerId = brokerId,
        Host = "localhost",
        Port = 9092,
        ReplicationPort = 10092,
        RebalanceCheckIntervalSeconds = 5,
        RaftPeerDiscoveryTimeoutSeconds = 0,
        RaftDataDirectory = Path.Combine(_root, "raft-" + Guid.NewGuid().ToString("N")),
        ControllerQuorumVoters = quorum ?? (observerOfVoter is { } voter ? $"{voter}@localhost:19093" : ""),
    };

    private RaftNode NewNode(ClusteringConfig config, ClusterState state, IRaftTransport? transport = null)
    {
        var node = new RaftNode(
            NullLogger<RaftNode>.Instance,
            config,
            new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            transport ?? new SilentTransport(),
            new MetadataStateMachine(
                NullLogger<MetadataStateMachine>.Instance, state, TestRaftNode.NewMembership(config, state)));
        _disposables.Add(node);
        return node;
    }

    private ClusterController NewController(ClusteringConfig config, ClusterState state, RaftNode node, out LogManager logs)
    {
        logs = new LogManager(Path.Combine(_root, "logs-" + Guid.NewGuid().ToString("N")), new MemoryLogSegmentFactory());
        _logManagers.Add(logs);
        var replicaManager = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, state, logs, config,
            new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());
        var controller = new ClusterController(NullLogger<ClusterController>.Instance, state, replicaManager, config);
        controller.SetRaftNode(node);
        // Disposed before the node it follows, which the fixture disposes after it.
        _disposables.Insert(0, controller);
        return controller;
    }

    /// <summary>A transport whose one peer, voter 2, grants every vote and takes every entry.</summary>
    private sealed class GrantingTransport : IRaftTransport
    {
        /// <summary>Called for each AppendEntries, before it is answered.</summary>
        public Action OnAppendEntries { get; set; } = () => { };

        public IReadOnlyList<int> GetPeerIds() => [2];

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct) => Task.FromResult(true);

        public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
            => Task.FromResult(new PreVoteResponse(request.ProposedTerm - 1, VoteGranted: true));

        public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
            => Task.FromResult(new RequestVoteResponse(request.Term, VoteGranted: true));

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
        {
            OnAppendEntries();
            return Task.FromResult(new AppendEntriesResponse(request.Term, true, request.PrevLogIndex + request.Entries.Length));
        }
    }

    /// <summary>A transport whose one peer, the quorum's voter, is never spoken to: an observer sends nothing.</summary>
    private sealed class SilentTransport : IRaftTransport
    {
        public IReadOnlyList<int> GetPeerIds() => [1];

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct) => Task.FromResult(true);

        public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
            => throw new NotSupportedException();

        public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
            => throw new NotSupportedException();

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
            => throw new NotSupportedException();
    }
}
