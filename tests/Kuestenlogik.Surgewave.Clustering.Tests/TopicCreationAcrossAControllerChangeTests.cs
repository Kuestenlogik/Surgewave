using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// A topic creation that finds the controller's office changing hands is neither reported as done nor lost (#268).
/// </summary>
/// <remarks>
/// <para>
/// A controller that loses its leadership with the topic's entry proposed and not yet on a majority has the entry cut
/// off by the next leader, which writes its own entries from that index on. The commit index passes the index all the
/// same, and the controller took that for its own entry: it answered "created" for a topic that is in no broker's
/// metadata log and made itself the leader of its partitions. The broker that had asked then waited for the topic to
/// arrive for as long as it ran — in a cluster whose brokers start an embedded Connect worker each, all asking for the
/// worker's topics while the first elections are still under way, that was the broker's start.
/// </para>
/// <para>
/// Nothing here waits for a time: the peer of the node under test takes entries up to an index the test sets, and the
/// next leader's entries are handed to the node by the test.
/// </para>
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class TopicCreationAcrossAControllerChangeTests : IAsyncDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    private readonly string _root = Path.Combine(Path.GetTempPath(), "surgewave-creation-" + Guid.NewGuid().ToString("N"));
    private readonly List<IAsyncDisposable> _disposables = [];
    private readonly List<IDisposable> _logManagers = [];
    private readonly CancellationToken _ct = TestContext.Current.CancellationToken;

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
    public async Task AnEntryTheNextLeaderCutOffIsNotTheProposersCommit()
    {
        var config = LeaderConfig();
        var transport = new CappedPeerTransport();
        var node = NewNode(config, new ClusterState(), transport);
        await node.StartAsync(_ct);
        await node.WaitForCaughtUpAsync(_ct).WaitAsync(Timeout, _ct);

        // An entry the peer takes commits, and is the proposer's.
        byte[] taken = [1];
        var takenAt = await node.ProposeAsync(MetadataCommandType.Noop, taken, _ct);
        Assert.True(takenAt > 0, "the node does not lead");
        Assert.True(await node.WaitForCommitOfAsync(takenAt, taken, Timeout, _ct));

        // One the peer never takes, and then the next leader's entry at its index.
        transport.TakesUpTo = node.LastLogIndex;
        byte[] cut = [2];
        var cutAt = await node.ProposeAsync(MetadataCommandType.Noop, cut, _ct);
        Assert.True(cutAt > transport.TakesUpTo);
        var waiting = node.WaitForCommitOfAsync(cutAt, cut, Timeout, _ct);

        await DeposeAsync(node, transport, upTo: cutAt);

        Assert.False(await waiting, "an entry the next leader replaced was reported as the proposer's commit");
        // What the commit index alone says, and what the controller went by.
        Assert.True(await node.WaitForCommitAsync(cutAt, Timeout, _ct));
    }

    [Fact]
    public async Task AControllerDeposedWhileItCreatesATopicAsksTheNextOneAndTakesNoPartInWhatItDidNotCommit()
    {
        var config = LeaderConfig();
        var state = new ClusterState();
        var transport = new CappedPeerTransport();
        var node = NewNode(config, state, transport);
        var controller = NewController(config, state, node, out var logs);
        var nextController = new AnsweringRequester(TopicCreationAnswer.Created);
        controller.SetTopicCreationRequester(nextController);
        await controller.StartAsync(_ct);
        await TheCoordinatorsTopicsAsync(state);

        transport.TakesUpTo = node.LastLogIndex;
        var creating = controller.CreateTopicAsync("orders", 1, 1, config: null, replicaAssignment: null, _ct);
        var entry = await transport.HeldTopic.Task.WaitAsync(Timeout, _ct);
        Assert.False(creating.IsCompleted);

        await DeposeAsync(node, transport, upTo: entry.Index);

        Assert.True(await creating.WaitAsync(Timeout, _ct), "the creation was refused although the next controller took it");
        var asked = Assert.Single(nextController.Asked);
        Assert.Equal("orders", asked);
        // The entry is in no metadata log: this broker leads no partition of the topic and holds no log for it.
        Assert.Null(state.GetTopic("orders"));
        Assert.Null(logs.GetTopicMetadata("orders"));
    }

    [Fact]
    public async Task ABrokerWhoseControllerSaysItIsNoneAsksAgainOnceItsNodeHearsOfTheNextLeader()
    {
        // Outside the quorum: it follows whoever leads and never campaigns, so the leaders are the ones handed to it.
        var config = NewConfig(brokerId: 2, quorum: "1@localhost:19093");
        var state = new ClusterState();
        var node = NewNode(config, state, new CappedPeerTransport());
        var controller = NewController(config, state, node, out _);
        var asked = new AnsweringRequester(TopicCreationAnswer.NotController, TopicCreationAnswer.Created);
        controller.SetTopicCreationRequester(asked);
        await controller.StartAsync(_ct);
        await node.HandleAppendEntriesAsync(Heartbeat(term: 1, leader: 1), _ct);

        var creating = controller.CreateTopicAsync("orders", 1, 1, config: null, replicaAssignment: null, _ct);
        await asked.Answered(1).WaitAsync(Timeout, _ct);

        // Broker 1 stepped down and said so; the node has not heard of a successor. A word from the term it knows
        // changes nothing.
        await node.HandleAppendEntriesAsync(Heartbeat(term: 1, leader: 1), _ct);
        Assert.False(creating.IsCompleted);
        Assert.Single(asked.Asked);

        // The same broker wins the next term: the controller id this broker names never changed, the office did.
        await node.HandleAppendEntriesAsync(Heartbeat(term: 2, leader: 1), _ct);

        Assert.True(await creating.WaitAsync(Timeout, _ct));
        Assert.Equal(2, asked.Asked.Count);
    }

    [Fact]
    public async Task AControllerInOfficeThatRefusesIsNotAskedAgain()
    {
        var config = NewConfig(brokerId: 2, quorum: "1@localhost:19093");
        var state = new ClusterState();
        var node = NewNode(config, state, new CappedPeerTransport());
        var controller = NewController(config, state, node, out _);
        var asked = new AnsweringRequester(TopicCreationAnswer.NotCreated);
        controller.SetTopicCreationRequester(asked);
        await controller.StartAsync(_ct);
        await node.HandleAppendEntriesAsync(Heartbeat(term: 1, leader: 1), _ct);

        Assert.False(await controller.CreateTopicAsync("orders", 1, 1, config: null, replicaAssignment: null, _ct).WaitAsync(Timeout, _ct));
        Assert.Single(asked.Asked);
    }

    [Fact]
    public async Task ATopicAskedForManyTimesAtOnceIsOneEntryOfTheMetadataLog()
    {
        var config = NewConfig(brokerId: 1);
        var state = new ClusterState();
        var machine = new RecordingStateMachine(new MetadataStateMachine(
            NullLogger<MetadataStateMachine>.Instance, state, TestRaftNode.NewMembership(config, state)));
        var node = new RaftNode(
            NullLogger<RaftNode>.Instance, config, new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            new SingleNodeRaftTransport(), machine);
        _disposables.Add(node);
        var controller = NewController(config, state, node, out _);
        await controller.StartAsync(_ct);
        await TheCoordinatorsTopicsAsync(state);

        // Three brokers' Connect workers ask the controller for the same topic in the same moment.
        var created = await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => Task.Run(
            () => controller.CreateTopicAsync("connect-configs", 1, 1, config: null, replicaAssignment: null, _ct), _ct)));

        Assert.All(created, Assert.True);
        Assert.Single(machine.Applied(MetadataCommandType.TopicCreated), e => System.Text.Encoding.UTF8.GetString(e.Data).Contains("connect-configs", StringComparison.Ordinal));
    }

    // The next leader, broker 2 in the next term, with entries of its own from the first index its peer had not taken
    // up to <paramref name="upTo"/>, all of them committed; from then on the node under test is refused every vote.
    private async Task DeposeAsync(RaftNode node, CappedPeerTransport transport, long upTo)
    {
        transport.GrantsVotes = false;
        var agreedAt = transport.TakesUpTo;
        var term = node.CurrentTerm + 1;
        var entries = Enumerable.Range(1, (int)(upTo - agreedAt))
            .Select(i => new RaftLogEntry { Term = term, Index = agreedAt + i, CommandType = MetadataCommandType.Noop, Data = [] })
            .ToArray();

        var response = await node.HandleAppendEntriesAsync(
            new AppendEntriesRequest(term, LeaderId: 2, agreedAt, node.LogEntryAt(agreedAt)!.Term, entries, LeaderCommit: upTo), _ct);
        Assert.True(response.Success);
    }

    // Until the controller has created the coordinators' topics, which it does on taking office and through the same
    // creation path: what follows is then the only creation under way.
    private async Task TheCoordinatorsTopicsAsync(ClusterState state)
    {
        string[] topics = [CoordinatorPartitioner.GroupTopic, CoordinatorPartitioner.TransactionTopic, CoordinatorPartitioner.ShareStateTopic];
        while (true)
        {
            var changed = state.TopicsChanged;
            if (topics.All(t => state.GetTopic(t) is not null))
                return;
            await changed.WaitAsync(Timeout, _ct);
        }
    }

    private static AppendEntriesRequest Heartbeat(int term, int leader)
        => new(term, leader, PrevLogIndex: 0, PrevLogTerm: 0, Entries: [], LeaderCommit: 0);

    // Voter 1 of two: it leads once its peer, the transport, grants it the vote.
    private ClusteringConfig LeaderConfig()
    {
        var config = NewConfig(brokerId: 1, quorum: "1@localhost:19093,2@localhost:19094");
        config.RaftElectionTimeoutMinMs = 50;
        config.RaftElectionTimeoutMaxMs = 100;
        config.RaftHeartbeatIntervalMs = 25;
        return config;
    }

    private ClusteringConfig NewConfig(int brokerId, string quorum = "") => new()
    {
        BrokerId = brokerId,
        Host = "localhost",
        Port = 9092,
        ReplicationPort = 10092,
        RebalanceCheckIntervalSeconds = 5,
        RaftPeerDiscoveryTimeoutSeconds = 0,
        RaftDataDirectory = Path.Combine(_root, "raft-" + Guid.NewGuid().ToString("N")),
        ControllerQuorumVoters = quorum,
        OffsetsTopicNumPartitions = 1,
        TransactionStateLogNumPartitions = 1,
        ShareStateTopicNumPartitions = 1,
    };

    private RaftNode NewNode(ClusteringConfig config, ClusterState state, IRaftTransport transport)
    {
        var node = new RaftNode(
            NullLogger<RaftNode>.Instance,
            config,
            new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            transport,
            new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, state, TestRaftNode.NewMembership(config, state)));
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

    /// <summary>
    /// A transport whose one peer, voter 2, grants votes while it is told to and takes entries up to
    /// <see cref="TakesUpTo"/>: what the leader proposes past that index is never on a majority.
    /// </summary>
    private sealed class CappedPeerTransport : IRaftTransport
    {
        private long _takesUpTo = long.MaxValue;
        private volatile bool _grantsVotes = true;

        public long TakesUpTo
        {
            get => Interlocked.Read(ref _takesUpTo);
            set => Interlocked.Exchange(ref _takesUpTo, value);
        }

        public bool GrantsVotes
        {
            get => _grantsVotes;
            set => _grantsVotes = value;
        }

        /// <summary>The first topic creation the leader sent that the peer did not take.</summary>
        public TaskCompletionSource<RaftLogEntry> HeldTopic { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public IReadOnlyList<int> GetPeerIds() => [2];

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct) => Task.FromResult(true);

        public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
            => Task.FromResult(new PreVoteResponse(request.ProposedTerm - 1, VoteGranted: GrantsVotes));

        public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
            => Task.FromResult(new RequestVoteResponse(request.Term, VoteGranted: GrantsVotes));

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
        {
            var takesUpTo = TakesUpTo;
            if (request.Entries.FirstOrDefault(e => e.Index > takesUpTo && e.CommandType == MetadataCommandType.TopicCreated) is { } held)
                HeldTopic.TrySetResult(held);
            return Task.FromResult(new AppendEntriesResponse(
                request.Term, true, Math.Min(request.PrevLogIndex + request.Entries.Length, takesUpTo)));
        }
    }

    /// <summary>The controller as the broker under test reaches it: its answers in order, the last one from then on.</summary>
    private sealed class AnsweringRequester(params TopicCreationAnswer[] answers) : ITopicCreationRequester
    {
        private readonly Lock _lock = new();
        private readonly List<string> _asked = [];
        private readonly List<TaskCompletionSource> _answered =
            [.. answers.Select(_ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously))];

        public IReadOnlyList<string> Asked
        {
            get { lock (_lock) return [.. _asked]; }
        }

        /// <summary>Completes when the answer to the <paramref name="nth"/> request, counted from 1, was given.</summary>
        public Task Answered(int nth) => _answered[nth - 1].Task;

        public Task<TopicCreationAnswer> RequestCreateTopicAsync(
            string topic, int partitions, short replicationFactor, IReadOnlyDictionary<string, string>? config,
            IReadOnlyList<IReadOnlyList<int>>? replicaAssignment, CancellationToken ct = default)
        {
            int nth;
            lock (_lock)
            {
                _asked.Add(topic);
                nth = _asked.Count;
            }

            var answer = answers[Math.Min(nth, answers.Length) - 1];
            if (nth <= _answered.Count)
                _answered[nth - 1].TrySetResult();
            return Task.FromResult(answer);
        }
    }

    // The entries the log applied, in order.
    private sealed class RecordingStateMachine(IRaftStateMachine inner) : IRaftStateMachine
    {
        private readonly List<RaftLogEntry> _applied = [];

        public IReadOnlyList<RaftLogEntry> Applied(MetadataCommandType type)
        {
            lock (_applied)
                return [.. _applied.Where(e => e.CommandType == type)];
        }

        public void Apply(RaftLogEntry entry)
        {
            lock (_applied)
                _applied.Add(entry);
            inner.Apply(entry);
        }

        public Task<byte[]> CreateSnapshotAsync(CancellationToken ct) => inner.CreateSnapshotAsync(ct);

        public Task RestoreFromSnapshotAsync(byte[] snapshot, CancellationToken ct) => inner.RestoreFromSnapshotAsync(snapshot, ct);
    }
}
