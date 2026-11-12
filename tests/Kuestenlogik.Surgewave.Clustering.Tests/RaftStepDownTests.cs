using System.Diagnostics;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// A leader that steps down waits out an election timeout before it campaigns, as any follower does (#275).
/// </summary>
/// <remarks>
/// The election timer was not restarted on the way down, and a leader's timer was last restarted by the campaign it
/// won — so it had long run out, and the node asked for pre-votes on its next 50 ms tick: of the peers it had just
/// found unreachable when it stepped down for the loss of its quorum.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftStepDownTests : IAsyncDisposable
{
    private const int ElectionTimeoutMinMs = 1000;
    private const int ElectionTimeoutMaxMs = 1100;
    private const int HeartbeatIntervalMs = 25;

    // Heartbeat rounds that take longer than the longest election timeout: a round follows the last one a heartbeat
    // interval later, but for the one a new leader sends at once for its first entry.
    private const int RoundsPastTheElectionTimeout = (ElectionTimeoutMaxMs / HeartbeatIntervalMs) + 10;

    private readonly string _directory = Path.Combine(Path.GetTempPath(), "surgewave-raft-stepdown-" + Guid.NewGuid().ToString("N"));

    [Fact]
    public async Task ALeaderThatStepsDown_CampaignsAnElectionTimeoutLater()
    {
        var ct = TestContext.Current.CancellationToken;
        var peers = new GrantingPeers();
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = ElectionTimeoutMinMs,
            RaftElectionTimeoutMaxMs = ElectionTimeoutMaxMs,
            RaftHeartbeatIntervalMs = HeartbeatIntervalMs,
            RaftPeerDiscoveryTimeoutSeconds = 0,
            RaftDataDirectory = _directory,
        };
        await using var node = new RaftNode(
            NullLogger<RaftNode>.Instance,
            config,
            new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            peers,
            new NoOpStateMachine(),
            new ConfiguredVoterSet([1, 2, 3]));

        var leads = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        node.LeadershipChanged += leadership =>
        {
            if (leadership.IsLeader)
                leads.TrySetResult();
        };
        await node.StartAsync(ct);
        await leads.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);

        // It leads for longer than an election timeout: the timer its own campaign restarted has run out since.
        await peers.HeartbeatRoundsAsync(RoundsPastTheElectionTimeout).WaitAsync(TimeSpan.FromSeconds(60), ct);

        var nextCampaign = peers.NextPreVoteAsync();
        var steppedDown = Stopwatch.StartNew();
        await node.StepDownAsync();
        await nextCampaign.WaitAsync(TimeSpan.FromSeconds(30), ct);
        var waited = steppedDown.Elapsed;

        Assert.True(waited.TotalMilliseconds >= ElectionTimeoutMinMs,
            $"the node asked for pre-votes {waited.TotalMilliseconds:F0} ms after it stepped down; its election timeout is at least {ElectionTimeoutMinMs} ms");
    }

    public ValueTask DisposeAsync()
    {
        try { if (Directory.Exists(_directory)) Directory.Delete(_directory, recursive: true); }
        catch (IOException) { /* a test host still holding the log file is not a failure */ }
        return ValueTask.CompletedTask;
    }

    /// <summary>Two voters that grant every pre-vote and vote and hold whatever the leader sends.</summary>
    private sealed class GrantingPeers : IRaftTransport
    {
        private TaskCompletionSource _preVote = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _roundsSeen = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _roundsAwaited = int.MaxValue;
        private int _rounds;

        /// <summary>Completed once the leader has sent <paramref name="rounds"/> heartbeat rounds, counted on voter 2.</summary>
        public Task HeartbeatRoundsAsync(int rounds)
        {
            Volatile.Write(ref _roundsAwaited, rounds);
            return _roundsSeen.Task;
        }

        /// <summary>Completed by the next pre-vote the node sends.</summary>
        public Task NextPreVoteAsync()
        {
            var next = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Volatile.Write(ref _preVote, next);
            return next.Task;
        }

        public IReadOnlyList<int> GetPeerIds() => [2, 3];

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct) => Task.FromResult(true);

        public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
        {
            Volatile.Read(ref _preVote).TrySetResult();
            return Task.FromResult(new PreVoteResponse(request.ProposedTerm - 1, true));
        }

        public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
            => Task.FromResult(new RequestVoteResponse(request.Term, true));

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
        {
            if (peerId == 2 && Interlocked.Increment(ref _rounds) >= Volatile.Read(ref _roundsAwaited))
                _roundsSeen.TrySetResult();
            return Task.FromResult(new AppendEntriesResponse(request.Term, true, request.PrevLogIndex + request.Entries.Length));
        }
    }

    private sealed class NoOpStateMachine : IRaftStateMachine
    {
        public void Apply(RaftLogEntry entry) { }

        public Task<byte[]> CreateSnapshotAsync(CancellationToken ct) => Task.FromResult(Array.Empty<byte>());

        public Task RestoreFromSnapshotAsync(byte[] snapshot, CancellationToken ct) => Task.CompletedTask;
    }
}
