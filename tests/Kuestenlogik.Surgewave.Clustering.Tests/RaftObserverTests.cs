using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// Observers: nodes that receive the metadata log without deciding it (#167).
/// </summary>
/// <remarks>
/// <para>
/// Until now every node the transport could reach was a voter, so "who gets the log" and
/// "who decides it" were the same set. Separating them is what a controller quorum (#168)
/// needs: the brokers outside the quorum must still hold the metadata they serve, or they
/// would have nothing to answer a client with.
/// </para>
/// <para>
/// Nothing here changes combined mode, where the voter set contains every node and the
/// observer set is therefore empty — which is the shape a single broker and an embedded host
/// run in.
/// </para>
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftObserverTests : IAsyncDisposable
{
    // Every wait here exits the moment its condition holds, so the ceiling only matters when
    // the machine is loaded — and these tests share a runner with ones that sleep. Five
    // seconds was enough alone and not enough alongside them.
    private static readonly TimeSpan LeadershipTimeout = TimeSpan.FromSeconds(30);

    private readonly List<string> _dataDirectories = [];

    [Fact]
    public async Task AnObserverDoesNotGrantAPreVote()
    {
        // The candidate does not ask an observer. Refusing is the guard for the other side
        // being wrong: a node with a stale voter list that still counts this answer would
        // reach a "majority" that no quorum agreed to.
        await using var observer = NewNode(nodeId: 3, voters: [1, 2], reachable: [1, 2]);

        var response = await observer.HandlePreVoteAsync(
            new PreVoteRequest(ProposedTerm: 5, CandidateId: 1, LastLogIndex: 0, LastLogTerm: 0),
            CancellationToken.None);

        Assert.False(response.VoteGranted);
    }

    [Fact]
    public async Task AnObserverDoesNotVoteButStillAdoptsTheNewerTerm()
    {
        // Both halves matter. No vote, because it is outside the quorum — but the term still
        // moves, because an observer that keeps serving an abandoned term would answer with
        // metadata the cluster has already replaced.
        await using var observer = NewNode(nodeId: 3, voters: [1, 2], reachable: [1, 2]);

        var response = await observer.HandleRequestVoteAsync(
            new RequestVoteRequest(Term: 7, CandidateId: 1, LastLogIndex: 0, LastLogTerm: 0),
            CancellationToken.None);

        Assert.False(response.VoteGranted);
        Assert.Equal(7, observer.CurrentTerm);
    }

    [Fact]
    public async Task AVoterInTheSamePositionDoesGrantTheVote()
    {
        // The contrast that shows the refusals above come from being an observer and not from
        // the request being malformed.
        await using var voter = NewNode(nodeId: 3, voters: [1, 2, 3], reachable: [1, 2]);

        var response = await voter.HandleRequestVoteAsync(
            new RequestVoteRequest(Term: 7, CandidateId: 1, LastLogIndex: 0, LastLogTerm: 0),
            CancellationToken.None);

        Assert.True(response.VoteGranted);
    }

    [Fact]
    public async Task AnObserverNeverCampaigns()
    {
        // An observer cannot win, so campaigning only costs the cluster a term bump and a
        // disrupted leader. It waits instead — indefinitely, which is correct: an observer
        // with no leader has nothing to do but wait for one.
        await using var observer = NewNode(nodeId: 3, voters: [1, 2], reachable: []);
        await observer.StartAsync(CancellationToken.None);

        var campaigned = await TestUtilities.WaitForCondition(
            () => observer.State != RaftState.Follower,
            TimeSpan.FromSeconds(2));

        Assert.False(campaigned, "an observer must not start an election");
        Assert.Equal(0, observer.CurrentTerm);
    }

    [Fact]
    public async Task TheLeaderReplicatesToObservers()
    {
        // The point of the whole change: a broker outside the quorum still gets the log.
        // Without it, a restricted controller quorum would leave every other broker with no
        // metadata at all.
        var transport = new RecordingTransport(reachable: [2, 3], voters: [1, 2]);
        await using var leader = NewNode(nodeId: 1, voters: [1, 2], transport: transport);
        await leader.StartAsync(CancellationToken.None);

        var replicated = await TestUtilities.WaitForCondition(
            () => transport.AppendedTo(3) > 0,
            LeadershipTimeout);

        Assert.True(leader.IsLeader, "the node under test has to reach leadership first");
        Assert.True(replicated, "observer 3 received no AppendEntries");
    }

    [Fact]
    public async Task ObserverAcknowledgementsDoNotCommit()
    {
        // Voters 2 and 3 reject; observers 4 and 5 accept everything. Counting the
        // acknowledgements rather than the votes would commit an entry that only the leader
        // holds — and losing the leader would then lose committed metadata.
        var transport = new RecordingTransport(reachable: [2, 3, 4, 5], voters: [1, 2, 3]);
        transport.AppendSucceedsFor = nodeId => nodeId is 4 or 5;

        await using var leader = NewNode(nodeId: 1, voters: [1, 2, 3], transport: transport);
        await leader.StartAsync(CancellationToken.None);

        await TestUtilities.WaitForCondition(
            () => transport.AppendedTo(4) >= 2 && transport.AppendedTo(5) >= 2,
            LeadershipTimeout);

        Assert.True(leader.IsLeader, "the node under test has to reach leadership first");
        Assert.Equal(0, leader.CommitIndex);
    }

    [Fact]
    public async Task AVoterMajorityStillCommits()
    {
        // The other half of the rule: with the voters acknowledging, the entry commits even
        // though the observers are the ones answering fastest. Pins that the voter-only
        // filter did not simply stop commits from happening.
        var transport = new RecordingTransport(reachable: [2, 3, 4, 5], voters: [1, 2, 3]);

        await using var leader = NewNode(nodeId: 1, voters: [1, 2, 3], transport: transport);
        await leader.StartAsync(CancellationToken.None);

        var committed = await TestUtilities.WaitForCondition(
            () => leader.CommitIndex > 0,
            LeadershipTimeout);

        Assert.True(committed, "a voter majority acknowledged but nothing committed");
    }

    private RaftNode NewNode(
        int nodeId,
        int[] voters,
        int[]? reachable = null,
        RecordingTransport? transport = null)
    {
        var directory = Path.Combine(Path.GetTempPath(), "surgewave-raft-observer-" + Guid.NewGuid().ToString("N"));
        _dataDirectories.Add(directory);

        var config = new ClusteringConfig
        {
            BrokerId = nodeId,
            RaftElectionTimeoutMinMs = 100,
            RaftElectionTimeoutMaxMs = 200,
            RaftHeartbeatIntervalMs = 25,
            RaftPeerDiscoveryTimeoutSeconds = 0,
            RaftDataDirectory = directory,
        };

        return new RaftNode(
            NullLogger<RaftNode>.Instance,
            config,
            new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            transport ?? new RecordingTransport(reachable ?? [], voters),
            new NoOpStateMachine(),
            new FixedVoterSet(voters));
    }

    public ValueTask DisposeAsync()
    {
        foreach (var directory in _dataDirectories)
        {
            try { if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true); }
            catch (IOException) { /* a test host still holding the log file is not a failure */ }
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>The explicit quorum #168 will configure, stated directly.</summary>
    private sealed class FixedVoterSet(IReadOnlyList<int> voterIds) : IRaftVoterSet
    {
        public IReadOnlyList<int> VoterIds { get; } = voterIds;

        public int Majority => (VoterIds.Count / 2) + 1;
    }

    /// <summary>
    /// A transport where voters answer as a healthy cluster would and every node records what
    /// it was sent, so the tests can ask who was replicated to rather than inferring it.
    /// </summary>
    private sealed class RecordingTransport(IReadOnlyList<int> reachable, IReadOnlyList<int> voters) : IRaftTransport
    {
        private readonly ConcurrentDictionary<int, int> _appends = new();

        /// <summary>Which nodes accept AppendEntries; all of them unless a test says otherwise.</summary>
        public Func<int, bool> AppendSucceedsFor { get; set; } = _ => true;

        public int AppendedTo(int nodeId) => _appends.TryGetValue(nodeId, out var count) ? count : 0;

        public IReadOnlyList<int> GetPeerIds() => reachable;

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct)
            => Task.FromResult(reachable.Contains(peerId));

        public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
            => Task.FromResult(new PreVoteResponse(request.ProposedTerm - 1, voters.Contains(peerId)));

        public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
            => Task.FromResult(new RequestVoteResponse(request.Term, voters.Contains(peerId)));

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
        {
            _appends.AddOrUpdate(peerId, 1, (_, count) => count + 1);

            var success = AppendSucceedsFor(peerId);
            var matchIndex = success ? request.PrevLogIndex + request.Entries.Length : 0;
            return Task.FromResult(new AppendEntriesResponse(request.Term, success, matchIndex));
        }
    }

    private sealed class NoOpStateMachine : IRaftStateMachine
    {
        public void Apply(RaftLogEntry entry) { }

        public Task<byte[]> CreateSnapshotAsync(CancellationToken ct) => Task.FromResult(Array.Empty<byte>());

        public Task RestoreFromSnapshotAsync(byte[] snapshot, CancellationToken ct) => Task.CompletedTask;
    }
}
