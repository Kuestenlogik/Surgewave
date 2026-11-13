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
/// A Raft request that reaches a node before the node was started is answered from the term, the vote and the log the
/// node persisted, not from the term 0, no vote and empty log it is constructed with (#275).
/// </summary>
/// <remarks>
/// The node here voted for node 2 in term 5 and holds two entries, then restarted. Its replication port is open
/// before it has read that back — the embedded runtime started the port and the node side by side — and a request
/// that came in between found a node of term 0: it granted a second vote in term 5, which is two leaders of one term
/// if both candidates count it, granted the pre-vote of a candidate three terms behind, and took the entries of a
/// leader long deposed, appending them to the log file behind the ones it had not read yet.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftRequestBeforeStartTests : IAsyncDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "surgewave-raft-unloaded-" + Guid.NewGuid().ToString("N"));
    private readonly List<IAsyncDisposable> _disposables = [];

    [Fact]
    public async Task AVoteRequest_IsRefusedInTheTermTheNodeHasVotedIn()
    {
        var ct = TestContext.Current.CancellationToken;
        var node = await RestartedNodeAsync(ct);

        var response = await node.HandleRequestVoteAsync(new RequestVoteRequest(5, CandidateId: 3, LastLogIndex: 2, LastLogTerm: 5), ct);

        Assert.False(response.VoteGranted, "the node voted for node 2 in term 5 before it restarted, and for node 3 in term 5 after");
        Assert.Equal(5, response.Term);
        Assert.Equal(new RaftPersistentState(5, 2), await NewPersistence().LoadStateAsync(ct));
    }

    [Fact]
    public async Task APreVote_IsRefusedForATermTheNodeIsPast()
    {
        var ct = TestContext.Current.CancellationToken;
        var node = await RestartedNodeAsync(ct);

        var response = await node.HandlePreVoteAsync(new PreVoteRequest(3, CandidateId: 3, LastLogIndex: 0, LastLogTerm: 0), ct);

        Assert.False(response.VoteGranted, "a node of term 5 granted the pre-vote for term 3");
        Assert.Equal(5, response.Term);
    }

    [Fact]
    public async Task TheEntriesOfADeposedLeader_AreNotTaken()
    {
        var ct = TestContext.Current.CancellationToken;
        var node = await RestartedNodeAsync(ct);
        var stale = new RaftLogEntry { Term = 3, Index = 1, CommandType = MetadataCommandType.Noop, Data = [] };

        var response = await node.HandleAppendEntriesAsync(new AppendEntriesRequest(3, LeaderId: 3, 0, 0, [stale], LeaderCommit: 0), ct);

        Assert.False(response.Success, "a node of term 5 took the entries of the leader of term 3");
        Assert.Equal(5, response.Term);
        await node.DisposeAsync();
        Assert.Equal([4, 5], (await NewPersistence().LoadLogAsync(ct)).Select(e => e.Term));
    }

    [Fact]
    public async Task AVoteRequestOverTheReplicationPort_IsRefusedInTheTermTheNodeHasVotedIn()
    {
        var ct = TestContext.Current.CancellationToken;
        var node = await RestartedNodeAsync(ct);

        // The port is open and hands its Raft requests to the node, which nobody has started.
        var tcp = new TcpPeerTransport();
        var state = new ClusterState();
        var config = NewConfig();
        var logs = new LogManager(Path.Combine(_directory, "logs"), new MemoryLogSegmentFactory());
        var replicas = new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, logs, config, tcp);
        _disposables.Add(replicas);
        var server = new ReplicationServer(NullLogger<ReplicationServer>.Instance, state, logs, replicas, config, tcp);
        _disposables.Add(server);
        server.SetRaftNode(node);
        await server.StartAsync(ct);

        var candidateState = new ClusterState();
        candidateState.AddBroker(new BrokerNode { BrokerId = 1, Host = "127.0.0.1", Port = 9092, ReplicationPort = server.BoundEndPoint!.Port });
        var candidate = new RaftTransport(NullLogger<RaftTransport>.Instance, candidateState, new ClusteringConfig { BrokerId = 3 }, tcp);
        _disposables.Add(candidate);

        var response = await candidate.SendRequestVoteAsync(1, new RequestVoteRequest(5, CandidateId: 3, LastLogIndex: 2, LastLogTerm: 5), ct);

        Assert.False(response.VoteGranted, "the node voted for node 2 in term 5 before it restarted, and for node 3 in term 5 after");
        Assert.Equal(5, response.Term);
        logs.Dispose();
    }

    // A node that voted for node 2 in term 5 and holds two entries, constructed again on that directory: not started.
    private async Task<RaftNode> RestartedNodeAsync(CancellationToken ct)
    {
        var before = NewPersistence();
        await before.SaveStateAsync(5, 2, ct);
        await before.AppendEntriesAsync(
        [
            new RaftLogEntry { Term = 4, Index = 1, CommandType = MetadataCommandType.Noop, Data = [] },
            new RaftLogEntry { Term = 5, Index = 2, CommandType = MetadataCommandType.Noop, Data = [] },
        ]);

        var node = new RaftNode(
            NullLogger<RaftNode>.Instance,
            NewConfig(),
            NewPersistence(),
            new NobodyThere(),
            new NoOpStateMachine(),
            new ConfiguredVoterSet([1, 2, 3]));
        _disposables.Add(node);
        return node;
    }

    private RaftPersistence NewPersistence() => new(NullLogger<RaftPersistence>.Instance, NewConfig());

    private ClusteringConfig NewConfig() => new()
    {
        BrokerId = 1,
        Host = "127.0.0.1",
        Port = 9092,
        ReplicationPort = 0,
        ReplicationBindAddress = "127.0.0.1",
        RaftPeerDiscoveryTimeoutSeconds = 0,
        RaftDataDirectory = _directory,
    };

    public async ValueTask DisposeAsync()
    {
        foreach (var disposable in _disposables)
            await disposable.DisposeAsync();
        try { if (Directory.Exists(_directory)) Directory.Delete(_directory, recursive: true); }
        catch (IOException) { /* a test host still holding the log file is not a failure */ }
    }

    private sealed class NobodyThere : IRaftTransport
    {
        public IReadOnlyList<int> GetPeerIds() => [2, 3];

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct) => Task.FromResult(false);

        public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
            => Task.FromException<PreVoteResponse>(new RaftTransportException($"peer {peerId} is down"));

        public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
            => Task.FromException<RequestVoteResponse>(new RaftTransportException($"peer {peerId} is down"));

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
            => Task.FromException<AppendEntriesResponse>(new RaftTransportException($"peer {peerId} is down"));
    }

    private sealed class NoOpStateMachine : IRaftStateMachine
    {
        public void Apply(RaftLogEntry entry) { }

        public Task<byte[]> CreateSnapshotAsync(CancellationToken ct) => Task.FromResult(Array.Empty<byte>());

        public Task RestoreFromSnapshotAsync(byte[] snapshot, CancellationToken ct) => Task.CompletedTask;
    }
}
