using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// Committing is not delivering (#177): what a leader has to have done before it may walk away.
/// </summary>
/// <remarks>
/// The failure these pin is quiet and only bites at shutdown. A controller that is the whole
/// quorum commits a decision the instant it appends it — its own log is the majority — so every
/// "did it commit?" check says yes while no other node has heard anything. A departing controller
/// that hands a partition to its successor on that answer names a new leader, tells nobody, and
/// leaves: the partition has a leader that does not know it, and nobody left to say so.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftHandoverDeliveryTests : IAsyncDisposable
{
    private static readonly TimeSpan LeadershipTimeout = TimeSpan.FromSeconds(30);

    private readonly List<string> _dataDirectories = [];

    [Fact]
    public async Task ALoneVoterCommitsItsProposalWithoutWaitingForATick()
    {
        // The heartbeat interval is a minute here, so nothing can tick between the propose and
        // the assert: if the commit index moved, it moved in ProposeAsync itself. That is the
        // whole point — a commit that waits for the next tick is a commit the follower is told
        // about one round too late.
        var transport = new RecordingTransport(reachable: [2], voters: [1]);
        await using var leader = NewNode(nodeId: 1, voters: [1], transport: transport, heartbeatIntervalMs: 60_000);
        await leader.StartAsync(CancellationToken.None);

        Assert.True(
            await TestUtilities.WaitForCondition(() => leader.IsLeader, LeadershipTimeout),
            "the node under test has to reach leadership first");

        var index = await leader.ProposeAsync(MetadataCommandType.LeaderChanged, [1, 2, 3], CancellationToken.None);

        Assert.True(index > 0, "the proposal was not accepted");
        Assert.True(
            leader.CommitIndex >= index,
            $"a lone voter must commit its own proposal immediately, but commit index is {leader.CommitIndex} for entry {index}");
    }

    [Fact]
    public async Task TheFirstAppendCarryingAnEntryAlsoCarriesItsCommit()
    {
        // The receiving half, and the one that actually broke the failover: a follower applies
        // up to the leaderCommit it was SENT. If the request that carries the entry still says
        // "committed up to the one before it", the follower stores the decision and does not act
        // on it — and if the leader goes away before the following request, it never will.
        var transport = new RecordingTransport(reachable: [2], voters: [1]);
        await using var leader = NewNode(nodeId: 1, voters: [1], transport: transport);
        await leader.StartAsync(CancellationToken.None);

        Assert.True(
            await TestUtilities.WaitForCondition(() => leader.IsLeader, LeadershipTimeout),
            "the node under test has to reach leadership first");

        var index = await leader.ProposeAsync(MetadataCommandType.LeaderChanged, [1, 2, 3], CancellationToken.None);

        var carried = await TestUtilities.WaitForCondition(
            () => transport.FirstRequestCarrying(index) is not null,
            LeadershipTimeout);
        Assert.True(carried, $"entry {index} was never sent to the observer");

        var request = transport.FirstRequestCarrying(index)!;
        Assert.True(
            request.LeaderCommit >= index,
            $"the request carrying entry {index} announced commit {request.LeaderCommit}, so the receiver stores it without applying it");
    }

    [Fact]
    public async Task ReplicationIsConfirmedOnlyOnceTheNodeAcknowledges()
    {
        // What the departing controller asks before it leaves. Node 2 rejects everything, so the
        // answer has to stay "no" no matter how long the leader waits.
        var transport = new RecordingTransport(reachable: [2], voters: [1]);
        transport.AppendSucceedsFor = _ => false;

        await using var leader = NewNode(nodeId: 1, voters: [1], transport: transport);
        await leader.StartAsync(CancellationToken.None);

        Assert.True(
            await TestUtilities.WaitForCondition(() => leader.IsLeader, LeadershipTimeout),
            "the node under test has to reach leadership first");

        var index = await leader.ProposeAsync(MetadataCommandType.LeaderChanged, [1], CancellationToken.None);

        var delivered = await leader.WaitForReplicationAsync(
            index, [2], TimeSpan.FromMilliseconds(300), CancellationToken.None);

        Assert.False(delivered, "a node that never acknowledged must not count as having the entry");
    }

    [Fact]
    public async Task ReplicationIsConfirmedOnceTheNodeHoldsTheEntry()
    {
        // The contrast: with the node answering, the same wait says yes — so the check above
        // fails for the right reason and not because it can never succeed.
        var transport = new RecordingTransport(reachable: [2], voters: [1]);
        await using var leader = NewNode(nodeId: 1, voters: [1], transport: transport);
        await leader.StartAsync(CancellationToken.None);

        Assert.True(
            await TestUtilities.WaitForCondition(() => leader.IsLeader, LeadershipTimeout),
            "the node under test has to reach leadership first");

        var index = await leader.ProposeAsync(MetadataCommandType.LeaderChanged, [1], CancellationToken.None);

        var delivered = await leader.WaitForReplicationAsync(
            index, [2], LeadershipTimeout, CancellationToken.None);

        Assert.True(delivered, "the node acknowledged the entry but the wait did not see it");
    }

    [Fact]
    public async Task NothingToDeliverIsConfirmedImmediately()
    {
        // A shutdown with no handovers must not spend its budget waiting for nobody.
        var transport = new RecordingTransport(reachable: [], voters: [1]);
        await using var leader = NewNode(nodeId: 1, voters: [1], transport: transport);

        var delivered = await leader.WaitForReplicationAsync(
            index: 5, [], TimeSpan.Zero, CancellationToken.None);

        Assert.True(delivered);
    }

    private RaftNode NewNode(
        int nodeId,
        int[] voters,
        RecordingTransport transport,
        int heartbeatIntervalMs = 25)
    {
        var directory = Path.Combine(Path.GetTempPath(), "surgewave-raft-handover-" + Guid.NewGuid().ToString("N"));
        _dataDirectories.Add(directory);

        var config = new ClusteringConfig
        {
            BrokerId = nodeId,
            RaftElectionTimeoutMinMs = 100,
            RaftElectionTimeoutMaxMs = 200,
            RaftHeartbeatIntervalMs = heartbeatIntervalMs,
            RaftPeerDiscoveryTimeoutSeconds = 0,
            RaftDataDirectory = directory,
        };

        return new RaftNode(
            NullLogger<RaftNode>.Instance,
            config,
            new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            transport,
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

    private sealed class FixedVoterSet(IReadOnlyList<int> voterIds) : IRaftVoterSet
    {
        public IReadOnlyList<int> VoterIds { get; } = voterIds;

        public int Majority => (VoterIds.Count / 2) + 1;
    }

    /// <summary>
    /// Keeps every AppendEntries request it was handed, so a test can ask what the receiver was
    /// actually told rather than inferring it from the leader's own state.
    /// </summary>
    private sealed class RecordingTransport(IReadOnlyList<int> reachable, IReadOnlyList<int> voters) : IRaftTransport
    {
        private readonly ConcurrentQueue<AppendEntriesRequest> _requests = new();

        public Func<int, bool> AppendSucceedsFor { get; set; } = _ => true;

        /// <summary>The first request that carried the entry at <paramref name="index"/>, if any.</summary>
        public AppendEntriesRequest? FirstRequestCarrying(long index)
            => _requests.FirstOrDefault(r => r.Entries.Any(e => e.Index == index));

        public IReadOnlyList<int> GetPeerIds() => reachable;

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct)
            => Task.FromResult(reachable.Contains(peerId));

        public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
            => Task.FromResult(new PreVoteResponse(request.ProposedTerm - 1, voters.Contains(peerId)));

        public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
            => Task.FromResult(new RequestVoteResponse(request.Term, voters.Contains(peerId)));

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
        {
            _requests.Enqueue(request);

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
