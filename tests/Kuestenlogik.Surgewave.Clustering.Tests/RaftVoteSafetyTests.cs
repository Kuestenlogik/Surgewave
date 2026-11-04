using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// One vote per voter per term, and so at most one leader per term (#213).
/// </summary>
/// <remarks>
/// A node that became follower within its term — a candidate hearing from that term's leader, a leader stepping down —
/// forgot the vote it had cast in the term and could cast a second one, for a node that started its campaign for the
/// same term from the term before. Two nodes then led one term; the one that heard from the other stayed leader and
/// named the other as leader, the state a three-broker stack showed.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftVoteSafetyTests : IAsyncDisposable
{
    private readonly List<string> _directories = [];

    [Fact]
    public async Task ACandidateThatHearsFromItsTermsLeader_KeepsItsVoteForThatTerm()
    {
        var ct = TestContext.Current.CancellationToken;
        var transport = new ScriptedTransport { HoldVotes = true };
        await using var node = NewNode(transport);
        await node.StartAsync(ct);

        // A candidate for term 1, its vote cast for itself, the answers still out.
        await transport.VoteRequested.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);
        Assert.Equal(1, node.CurrentTerm);

        // Node 2 won term 1 and says so.
        Assert.True((await node.HandleAppendEntriesAsync(new AppendEntriesRequest(1, 2, 0, 0, [], 0), ct)).Success);
        Assert.False(node.IsLeader);

        // Node 3, campaigning for term 1 from term 0, asks for this node's vote: it was cast already.
        var vote = await node.HandleRequestVoteAsync(new RequestVoteRequest(1, 3, 100, 1), ct);
        Assert.False(vote.VoteGranted);
    }

    [Fact]
    public async Task ALeaderThatStepsDown_KeepsItsVoteForThatTerm()
    {
        var ct = TestContext.Current.CancellationToken;
        var transport = new ScriptedTransport();
        await using var node = NewNode(transport);
        await node.StartAsync(ct);
        Assert.True(await TestUtilities.WaitForCondition(() => node.IsLeader, TimeSpan.FromSeconds(30)), "the node never led");
        var term = node.CurrentTerm;

        await node.StepDownAsync();

        var vote = await node.HandleRequestVoteAsync(new RequestVoteRequest(term, 3, 100, term), ct);
        Assert.False(vote.VoteGranted);
    }

    [Fact]
    public async Task ALeaderThatHearsFromAnotherLeaderOfItsTerm_FollowsIt()
    {
        var ct = TestContext.Current.CancellationToken;
        var transport = new ScriptedTransport();
        await using var node = NewNode(transport);
        await node.StartAsync(ct);
        Assert.True(await TestUtilities.WaitForCondition(() => node.IsLeader, TimeSpan.FromSeconds(30)), "the node never led");
        var term = node.CurrentTerm;

        await node.HandleAppendEntriesAsync(new AppendEntriesRequest(term, 2, 0, 0, [], 0), ct);

        // Never "Leader, leader 2": a node that names another leader is not one.
        Assert.False(node.IsLeader);
        Assert.Equal(2, node.LeaderId);
    }

    private RaftNode NewNode(IRaftTransport transport)
    {
        var directory = Path.Combine(Path.GetTempPath(), "surgewave-raft-votes-" + Guid.NewGuid().ToString("N"));
        _directories.Add(directory);
        var config = new ClusteringConfig
        {
            BrokerId = 1,
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
            transport,
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

    /// <summary>Peers 2 and 3 grant every pre-vote and vote; with <see cref="HoldVotes"/> the votes never come back.</summary>
    private sealed class ScriptedTransport : IRaftTransport
    {
        public bool HoldVotes { get; init; }

        public TaskCompletionSource VoteRequested { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public IReadOnlyList<int> GetPeerIds() => [2, 3];

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct) => Task.FromResult(true);

        public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
            => Task.FromResult(new PreVoteResponse(request.ProposedTerm - 1, true));

        public async Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
        {
            VoteRequested.TrySetResult();
            if (HoldVotes)
                await new TaskCompletionSource().Task.WaitAsync(ct).ConfigureAwait(false); // answered never
            return new RequestVoteResponse(request.Term, true);
        }

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
            => Task.FromResult(new AppendEntriesResponse(request.Term, true, request.PrevLogIndex + request.Entries.Length));
    }

    private sealed class NoOpStateMachine : IRaftStateMachine
    {
        public void Apply(RaftLogEntry entry) { }

        public Task<byte[]> CreateSnapshotAsync(CancellationToken ct) => Task.FromResult(Array.Empty<byte>());

        public Task RestoreFromSnapshotAsync(byte[] snapshot, CancellationToken ct) => Task.CompletedTask;
    }
}
