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
/// The two steps a host starts its cluster controller in, with the replication port opened between them (#275, #270):
/// the load, after which the Raft node holds what it persisted and the brokers of the configuration are known, and
/// the start, which begins the elections and waits for no peer.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ClusterStartOrderTests : IAsyncDisposable
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private readonly string _root = Path.Combine(Path.GetTempPath(), "surgewave-start-order-" + Guid.NewGuid().ToString("N"));
    private readonly List<IAsyncDisposable> _disposables = [];
    private LogManager? _logs;

    [Fact]
    public async Task AfterTheLoad_TheNodeHoldsWhatItPersistedAndThePeersAreKnown()
    {
        var ct = TestContext.Current.CancellationToken;
        var config = NewConfig();
        var before = new RaftPersistence(NullLogger<RaftPersistence>.Instance, config);
        await before.SaveStateAsync(5, 2, ct);
        await before.AppendEntriesAsync([new RaftLogEntry { Term = 5, Index = 1, CommandType = MetadataCommandType.Noop, Data = [] }]);
        var state = new ClusterState();
        var (controller, node) = NewController(config, state);

        await controller.LoadAsync(ct);

        Assert.Equal(5, node.CurrentTerm);
        Assert.Equal(1, node.LastLogIndex);
        Assert.Equal([1, 2, 3], state.Brokers.Keys.Order());
        Assert.False(node.IsLeader);

        // What a peer asks through the port a host opens now is answered from that term and vote.
        var vote = await node.HandleRequestVoteAsync(new RequestVoteRequest(5, CandidateId: 3, LastLogIndex: 1, LastLogTerm: 5), ct);
        Assert.False(vote.VoteGranted);
    }

    [Fact]
    public async Task TheStartReturnsWhileNoPeerIsThere()
    {
        var ct = TestContext.Current.CancellationToken;
        var (controller, node) = NewController(NewConfig(), new ClusterState());

        await controller.LoadAsync(ct);
        try
        {
            await controller.StartAsync(ct).WaitAsync(Bound, ct);
        }
        catch (TimeoutException)
        {
            Assert.Fail("the controller's start waited for a peer");
        }

        Assert.False(node.IsLeader);
    }

    [Fact]
    public async Task ALogThatCannotBeRead_StopsTheLoad()
    {
        var ct = TestContext.Current.CancellationToken;
        var config = NewConfig();
        var before = new RaftPersistence(NullLogger<RaftPersistence>.Instance, config);
        await before.AppendEntriesAsync([new RaftLogEntry { Term = 1, Index = 1, CommandType = MetadataCommandType.Noop, Data = [] }]);
        var (controller, _) = NewController(config, new ClusterState());

        // Held as a writer holds it while it appends.
        await using (new FileStream(Path.Combine(config.RaftDataDirectory!, "raft-log.bin"), FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        {
            await Assert.ThrowsAnyAsync<IOException>(() => controller.LoadAsync(ct));
            await Assert.ThrowsAnyAsync<IOException>(() => controller.StartAsync(ct));
        }
    }

    private (ClusterController Controller, RaftNode Node) NewController(ClusteringConfig config, ClusterState state)
    {
        var node = new RaftNode(
            NullLogger<RaftNode>.Instance,
            config,
            new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            new NobodyThere(),
            new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, state, TestRaftNode.NewMembership(config, state)));
        _logs = new LogManager(Path.Combine(_root, "logs"), new MemoryLogSegmentFactory());
        var replicas = new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, _logs, config, new TcpPeerTransport());
        var controller = new ClusterController(NullLogger<ClusterController>.Instance, state, replicas, config);
        controller.SetRaftNode(node);
        _disposables.Add(controller);
        _disposables.Add(node);
        _disposables.Add(replicas);
        return (controller, node);
    }

    // Broker 1 of three controller voters, with a peer discovery timeout that no test here waits out.
    private ClusteringConfig NewConfig() => new()
    {
        BrokerId = 1,
        Host = "127.0.0.1",
        Port = 9092,
        ReplicationPort = 10092,
        ClusterNodes = "1:127.0.0.1:9092:10092,2:127.0.0.1:9093:10093,3:127.0.0.1:9094:10094",
        ControllerQuorumVoters = "1@127.0.0.1:10092,2@127.0.0.1:10093,3@127.0.0.1:10094",
        RaftPeerDiscoveryTimeoutSeconds = 300,
        RaftDataDirectory = Path.Combine(_root, "raft"),
    };

    public async ValueTask DisposeAsync()
    {
        foreach (var disposable in _disposables)
            await disposable.DisposeAsync();
        _logs?.Dispose();
        try { if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true); }
        catch (IOException) { /* a test host still holding the log file is not a failure */ }
    }

    /// <summary>No peer is up: an attempt to reach one is under way until the node gives it up.</summary>
    private sealed class NobodyThere : IRaftTransport
    {
        public IReadOnlyList<int> GetPeerIds() => [2, 3];

        public async Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct)
        {
            await Task.Delay(Timeout.Infinite, ct);
            return false;
        }

        public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
            => Task.FromException<PreVoteResponse>(new RaftTransportException($"peer {peerId} is down"));

        public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
            => Task.FromException<RequestVoteResponse>(new RaftTransportException($"peer {peerId} is down"));

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
            => Task.FromException<AppendEntriesResponse>(new RaftTransportException($"peer {peerId} is down"));
    }
}
