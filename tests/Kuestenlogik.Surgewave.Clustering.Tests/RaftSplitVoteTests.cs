using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// Two of three voters that split the vote elect a leader on their election timeouts, whatever the third voter does:
/// here it is down and its requests are never answered.
/// </summary>
/// <remarks>
/// A split vote is what Raft's randomised election timeout is for: each candidate waits out its own and starts the
/// next election, and the one whose timeout is shorter asks first and wins. The node waited for the third voter's
/// answer instead, as it could still have decided the round, and an unreachable peer answers when its connect
/// attempts gave up — the same seconds later for both candidates, long past either timeout. Both started the next
/// election on their next tick, each voting for itself again: two brokers started together with the third one down
/// won the pre-vote and lost the election for ten terms in 55 s, and neither became ready.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftSplitVoteTests : IAsyncDisposable
{
    private readonly List<string> _directories = [];

    [Fact]
    public async Task TwoVotersThatSplitTheVote_ElectALeaderWhileTheThirdNeverAnswers()
    {
        var ct = TestContext.Current.CancellationToken;
        var cluster = new TwoOfThree();
        await using var first = NewNode(1, cluster);
        await using var second = NewNode(2, cluster);
        cluster.Add(first);
        cluster.Add(second);

        await Task.WhenAll(first.StartAsync(ct), second.StartAsync(ct));

        // Each is a candidate for term 1 with its own vote and was refused the other's.
        await Within(cluster.VoteSplit, () => "the two voters did not both ask for a vote in term 1", ct);

        // The node's own startup handshake, which a broker's ready line waits for: a leader's no-op is committed, so
        // one of the two leads and the other follows it.
        await Within(
            Task.WhenAll(first.WaitForCaughtUpAsync(ct), second.WaitForCaughtUpAsync(ct)),
            () => $"no leader after the split vote: node 1 is {first.State} in term {first.CurrentTerm}, "
                + $"node 2 is {second.State} in term {second.CurrentTerm}",
            ct);

        var leader = first.IsLeader ? first : second;
        var follower = first.IsLeader ? second : first;
        Assert.True(leader.IsLeader);
        Assert.False(follower.IsLeader);
        Assert.Equal(leader.NodeId, follower.LeaderId);
        Assert.Equal(leader.CurrentTerm, follower.CurrentTerm);

        // Voter 3 has answered nothing, and was not asked again for it round after round.
        Assert.Equal(0, cluster.AnsweredByTheThird);
        Assert.Equal(1, cluster.MostPreVotesOutToTheThird);
        Assert.Equal(1, cluster.MostVoteRequestsOutToTheThird);
    }

    private static async Task Within(Task handshake, Func<string> otherwise, CancellationToken ct)
    {
        try
        {
            await handshake.WaitAsync(TimeSpan.FromSeconds(30), ct);
        }
        catch (TimeoutException)
        {
            Assert.Fail(otherwise());
        }
    }

    private RaftNode NewNode(int brokerId, TwoOfThree cluster)
    {
        var directory = Path.Combine(Path.GetTempPath(), "surgewave-raft-split-" + Guid.NewGuid().ToString("N"));
        _directories.Add(directory);
        var config = new ClusteringConfig
        {
            BrokerId = brokerId,
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
            new Link(brokerId, cluster),
            new NoOpStateMachine(),
            new FixedVoterSet([1, 2, 3]));
    }

    public ValueTask DisposeAsync()
    {
        foreach (var directory in _directories)
        {
            try { if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true); }
            catch (IOException) { /* a test host still holding the log file is not a failure */ }
        }
        return ValueTask.CompletedTask;
    }

    private sealed class FixedVoterSet(IReadOnlyList<int> voterIds) : IRaftVoterSet
    {
        public IReadOnlyList<int> VoterIds { get; } = voterIds;

        public int Majority => (VoterIds.Count / 2) + 1;
    }

    /// <summary>
    /// Voters 1 and 2, which reach each other, and voter 3, which is down: a request to it is under way until the
    /// node that sent it stops.
    /// </summary>
    /// <remarks>
    /// The election for term 1 is answered here as the two answer each other when their requests cross, which they do
    /// when both start within a round trip: the pre-vote granted, by a voter that is not a candidate yet, and the vote
    /// refused, by one that has voted for itself since. Either node gets there on its own, whenever its first election
    /// timeout runs out, and what one sends beyond term 1 reaches the other only once both have.
    /// </remarks>
    private sealed class TwoOfThree
    {
        private const int Down = 3;

        private readonly Dictionary<int, RaftNode> _nodes = [];
        private readonly TaskCompletionSource _voteSplit = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly object _lock = new();
        private readonly HashSet<int> _askedForTermOne = [];
        private readonly Dictionary<int, int> _preVotesOut = [];
        private readonly Dictionary<int, int> _voteRequestsOut = [];
        private int _answeredByTheThird;

        /// <summary>Completed once both nodes have asked for a vote in term 1, each having voted for itself.</summary>
        public Task VoteSplit => _voteSplit.Task;

        public int AnsweredByTheThird => Volatile.Read(ref _answeredByTheThird);

        /// <summary>The most pre-votes one node has sent to voter 3, none of which came back.</summary>
        public int MostPreVotesOutToTheThird { get; private set; }

        /// <summary>The most vote requests one node has sent to voter 3, none of which came back.</summary>
        public int MostVoteRequestsOutToTheThird { get; private set; }

        public void Add(RaftNode node) => _nodes[node.NodeId] = node;

        public async Task<PreVoteResponse> PreVoteAsync(int from, int to, PreVoteRequest request, CancellationToken ct)
        {
            if (to != Down)
            {
                if (request.ProposedTerm == 1)
                    return new PreVoteResponse(0, true);

                await _voteSplit.Task.WaitAsync(ct).ConfigureAwait(false);
                return await _nodes[to].HandlePreVoteAsync(request, ct).ConfigureAwait(false);
            }

            lock (_lock)
            {
                var asked = _preVotesOut[from] = _preVotesOut.GetValueOrDefault(from) + 1;
                MostPreVotesOutToTheThird = Math.Max(MostPreVotesOutToTheThird, asked);
            }
            await NeverAnsweredAsync(ct).ConfigureAwait(false);
            return new PreVoteResponse(request.ProposedTerm, false);
        }

        public async Task<RequestVoteResponse> RequestVoteAsync(int from, int to, RequestVoteRequest request, CancellationToken ct)
        {
            if (to == Down)
            {
                lock (_lock)
                {
                    var asked = _voteRequestsOut[from] = _voteRequestsOut.GetValueOrDefault(from) + 1;
                    MostVoteRequestsOutToTheThird = Math.Max(MostVoteRequestsOutToTheThird, asked);
                }
                await NeverAnsweredAsync(ct).ConfigureAwait(false);
                return new RequestVoteResponse(request.Term, false);
            }

            if (request.Term == 1)
            {
                lock (_lock)
                {
                    _askedForTermOne.Add(from);
                    if (_askedForTermOne.Count == 2)
                        _voteSplit.TrySetResult();
                }
                return new RequestVoteResponse(1, false);
            }

            await _voteSplit.Task.WaitAsync(ct).ConfigureAwait(false);
            return await _nodes[to].HandleRequestVoteAsync(request, ct).ConfigureAwait(false);
        }

        public async Task<AppendEntriesResponse> AppendEntriesAsync(int to, AppendEntriesRequest request, CancellationToken ct)
        {
            if (to != Down)
            {
                await _voteSplit.Task.WaitAsync(ct).ConfigureAwait(false);
                return await _nodes[to].HandleAppendEntriesAsync(request, ct).ConfigureAwait(false);
            }

            await NeverAnsweredAsync(ct).ConfigureAwait(false);
            return new AppendEntriesResponse(request.Term, false, 0);
        }

        // Returns only by throwing, when the asking node stops; counted if it ever returned otherwise.
        private async Task NeverAnsweredAsync(CancellationToken ct)
        {
            await new TaskCompletionSource().Task.WaitAsync(ct).ConfigureAwait(false);
            Interlocked.Increment(ref _answeredByTheThird);
        }
    }

    /// <summary>One node's transport into <see cref="TwoOfThree"/>.</summary>
    private sealed class Link(int from, TwoOfThree cluster) : IRaftTransport
    {
        public IReadOnlyList<int> GetPeerIds() => [.. new[] { 1, 2, 3 }.Where(id => id != from)];

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct) => Task.FromResult(peerId != 3);

        public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
            => cluster.PreVoteAsync(from, peerId, request, ct);

        public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
            => cluster.RequestVoteAsync(from, peerId, request, ct);

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
            => cluster.AppendEntriesAsync(peerId, request, ct);
    }

    private sealed class NoOpStateMachine : IRaftStateMachine
    {
        public void Apply(RaftLogEntry entry) { }

        public Task<byte[]> CreateSnapshotAsync(CancellationToken ct) => Task.FromResult(Array.Empty<byte>());

        public Task RestoreFromSnapshotAsync(byte[] snapshot, CancellationToken ct) => Task.CompletedTask;
    }
}
