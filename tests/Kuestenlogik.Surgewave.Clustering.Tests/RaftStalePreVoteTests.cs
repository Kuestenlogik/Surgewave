using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// A pre-vote whose answers arrive after a leader was elected does not start an election (#204).
/// </summary>
/// <remarks>
/// The answers can take seconds: a peer that is not up yet costs the connect retries. In a three-broker start the
/// node that asked first was by then the follower of the leader the others had elected, took the late "yes" for
/// the term it had proposed, and deposed that leader with a second election moments after the first — and the
/// controller operation under way on the deposed leader failed.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftStalePreVoteTests : IAsyncDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "surgewave-raft-prevote-" + Guid.NewGuid().ToString("N"));

    [Fact]
    public async Task APreVoteAnsweredAfterALeaderSpokeUp_StartsNoElection()
    {
        var ct = TestContext.Current.CancellationToken;
        var transport = new HeldPreVoteTransport();
        await using var node = NewNode(transport);
        await node.StartAsync(ct);

        // The first pre-vote is out, its answers held.
        await transport.FirstPreVoteSent.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);

        // Meanwhile node 2 won term 1 and its first heartbeat arrives.
        var heartbeat = await node.HandleAppendEntriesAsync(new AppendEntriesRequest(1, 2, 0, 0, [], 0), ct);
        Assert.True(heartbeat.Success);

        // Now the late answers: every peer grants it.
        transport.ReleaseFirstPreVote.TrySetResult();

        // The next pre-vote comes once the leader stays silent for an election timeout; by then a stale election
        // would long have asked for votes.
        await transport.SecondPreVoteSent.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);

        Assert.Equal(0, transport.VoteRequests);
        Assert.Equal(1, node.CurrentTerm);
    }

    private RaftNode NewNode(IRaftTransport transport)
    {
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 100,
            RaftElectionTimeoutMaxMs = 200,
            RaftHeartbeatIntervalMs = 25,
            RaftPeerDiscoveryTimeoutSeconds = 0,
            RaftDataDirectory = _directory,
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
        try { if (Directory.Exists(_directory)) Directory.Delete(_directory, recursive: true); }
        catch (IOException) { /* a test host still holding the log file is not a failure */ }
        return ValueTask.CompletedTask;
    }

    private sealed class FixedVoterSet(IReadOnlyList<int> voterIds) : IRaftVoterSet
    {
        public IReadOnlyList<int> VoterIds { get; } = voterIds;

        public int Majority => (VoterIds.Count / 2) + 1;
    }

    /// <summary>Holds the answers to the first pre-vote until the test releases them; later ones are refused.</summary>
    private sealed class HeldPreVoteTransport : IRaftTransport
    {
        private int _preVotes;
        private int _voteRequests;

        public TaskCompletionSource FirstPreVoteSent { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource ReleaseFirstPreVote { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource SecondPreVoteSent { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int VoteRequests => Volatile.Read(ref _voteRequests);

        public IReadOnlyList<int> GetPeerIds() => [2, 3];

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct) => Task.FromResult(true);

        public async Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
        {
            // One round asks both peers; the first round is the one whose answers are held.
            var round = (Interlocked.Increment(ref _preVotes) + 1) / 2;
            if (round == 1)
            {
                FirstPreVoteSent.TrySetResult();
                await ReleaseFirstPreVote.Task.WaitAsync(ct).ConfigureAwait(false);
                return new PreVoteResponse(request.ProposedTerm - 1, true);
            }

            SecondPreVoteSent.TrySetResult();
            return new PreVoteResponse(request.ProposedTerm - 1, false);
        }

        public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
        {
            Interlocked.Increment(ref _voteRequests);
            return Task.FromResult(new RequestVoteResponse(request.Term, false));
        }

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
            => Task.FromResult(new AppendEntriesResponse(request.Term, false, 0));
    }

    private sealed class NoOpStateMachine : IRaftStateMachine
    {
        public void Apply(RaftLogEntry entry) { }

        public Task<byte[]> CreateSnapshotAsync(CancellationToken ct) => Task.FromResult(Array.Empty<byte>());

        public Task RestoreFromSnapshotAsync(byte[] snapshot, CancellationToken ct) => Task.CompletedTask;
    }
}
