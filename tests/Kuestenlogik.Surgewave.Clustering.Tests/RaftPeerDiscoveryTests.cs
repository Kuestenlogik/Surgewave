using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
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
/// A node that expects peers starts without waiting for one, and holds its first election until a voter was reached
/// or heard from (#270, #267).
/// </summary>
/// <remarks>
/// The node's start used to wait for a peer, by connecting to every peer every 100 ms for up to 30 s, and the broker
/// host opened its replication port only after that start: brokers started together each waited the 30 s for a port
/// none of them had opened. The wait is the election loop's now and a handshake: one attempt to reach each peer, and
/// after it the word of whoever hears from one — the node itself for a Raft request, the replication server for a
/// broker heartbeat. The peer discovery timeout is five minutes here, so nothing below is the timeout running out.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftPeerDiscoveryTests : IAsyncDisposable
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private readonly string _directory = Path.Combine(Path.GetTempPath(), "surgewave-raft-discovery-" + Guid.NewGuid().ToString("N"));
    private readonly List<IAsyncDisposable> _disposables = [];

    [Fact]
    public async Task TheStartReturnsWhileNoPeerIsThere()
    {
        var ct = TestContext.Current.CancellationToken;
        var peers = new Peers();
        var node = NewNode(peers);

        await Within(node.StartAsync(ct), "the node's start waited for a peer", ct);

        Assert.Equal(0, peers.PreVotesSent);
    }

    [Fact]
    public async Task TheFirstElectionFollowsAPeersRequest()
    {
        var ct = TestContext.Current.CancellationToken;
        var peers = new Peers();
        var node = NewNode(peers);
        await Within(node.StartAsync(ct), "the node's start waited for a peer", ct);

        // Voter 2 is up and asks for a pre-vote; this node reached nobody itself.
        await node.HandlePreVoteAsync(new PreVoteRequest(1, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0), ct);

        await Within(peers.PreVoteSent, "the node heard from voter 2 and started no election", ct);
    }

    [Fact]
    public async Task TheFirstElectionFollowsAPeerThatWasReached()
    {
        var ct = TestContext.Current.CancellationToken;
        var peers = new Peers();
        var node = NewNode(peers);
        await Within(node.StartAsync(ct), "the node's start waited for a peer", ct);

        // The node's one attempt to reach voter 3 succeeds.
        peers.Reach(3);

        await Within(peers.PreVoteSent, "the node reached voter 3 and started no election", ct);
    }

    [Fact]
    public async Task TheFirstElectionFollowsAPeersHeartbeatOnTheReplicationPort()
    {
        var ct = TestContext.Current.CancellationToken;
        var peers = new Peers();
        var node = NewNode(peers);
        var port = await OpenReplicationPortAsync(node, ct);
        await Within(node.StartAsync(ct), "the node's start waited for a peer", ct);

        // Voter 2's heartbeat manager sends its heartbeat to this broker's replication port.
        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port, ct);
        await client.GetStream().WriteAsync(HeartbeatFrom(brokerId: 2), ct);

        await Within(peers.PreVoteSent, "voter 2's heartbeat reached the replication port and the node started no election", ct);
    }

    private static async Task Within(Task handshake, string otherwise, CancellationToken ct)
    {
        try
        {
            await handshake.WaitAsync(Bound, ct);
        }
        catch (TimeoutException)
        {
            Assert.Fail(otherwise);
        }
    }

    // The frame a broker's heartbeat manager writes: header, then broker id, broker epoch, timestamp, controller id
    // and controller epoch.
    private static byte[] HeartbeatFrom(int brokerId)
    {
        var frame = new byte[4 + 10 + 24];
        BinaryPrimitives.WriteInt32BigEndian(frame, frame.Length - 4);
        BinaryPrimitives.WriteInt16BigEndian(frame.AsSpan(4), 100);   // api key: heartbeat
        BinaryPrimitives.WriteInt16BigEndian(frame.AsSpan(6), 0);     // api version
        BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(8), 1);     // correlation id
        BinaryPrimitives.WriteInt16BigEndian(frame.AsSpan(12), -1);   // client id: none
        BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(14), brokerId);
        BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(30), -1);   // controller id: none known
        return frame;
    }

    private async Task<int> OpenReplicationPortAsync(RaftNode node, CancellationToken ct)
    {
        var tcp = new TcpPeerTransport();
        var state = new ClusterState();
        var config = NewConfig();
        var logs = new LogManager(Path.Combine(_directory, "logs"), new MemoryLogSegmentFactory());
        var replicas = new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, logs, config, tcp);
        var server = new ReplicationServer(NullLogger<ReplicationServer>.Instance, state, logs, replicas, config, tcp);
        _disposables.Add(server);
        _disposables.Add(replicas);
        _disposables.Add(new Disposal(logs));
        server.SetRaftNode(node);
        await server.StartAsync(ct);
        return server.BoundEndPoint!.Port;
    }

    private RaftNode NewNode(Peers peers)
    {
        var config = NewConfig();
        var node = new RaftNode(
            NullLogger<RaftNode>.Instance,
            config,
            new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            peers,
            new NoOpStateMachine(),
            new ConfiguredVoterSet([1, 2, 3]));
        _disposables.Insert(0, node);
        return node;
    }

    private ClusteringConfig NewConfig() => new()
    {
        BrokerId = 1,
        Host = "127.0.0.1",
        Port = 9092,
        ReplicationPort = 0,
        ClusterNodes = "1:127.0.0.1:9092:10092,2:127.0.0.1:9093:10093,3:127.0.0.1:9094:10094",
        RaftElectionTimeoutMinMs = 100,
        RaftElectionTimeoutMaxMs = 200,
        RaftPeerDiscoveryTimeoutSeconds = 300,
        RaftDataDirectory = Path.Combine(_directory, "raft"),
    };

    public async ValueTask DisposeAsync()
    {
        foreach (var disposable in _disposables)
            await disposable.DisposeAsync();
        try { if (Directory.Exists(_directory)) Directory.Delete(_directory, recursive: true); }
        catch (IOException) { /* a test host still holding the log file is not a failure */ }
    }

    private sealed class Disposal(IDisposable inner) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            inner.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// Voters 2 and 3 as this node's transport sees them: an attempt to reach one is under way until the test lets it
    /// succeed, and a pre-vote sent to one is noted and never answered.
    /// </summary>
    private sealed class Peers : IRaftTransport
    {
        private readonly Dictionary<int, TaskCompletionSource<bool>> _reached = new()
        {
            [2] = new(TaskCreationOptions.RunContinuationsAsynchronously),
            [3] = new(TaskCreationOptions.RunContinuationsAsynchronously),
        };
        private readonly TaskCompletionSource _preVoteSent = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _preVotesSent;

        /// <summary>Completed by the first pre-vote the node sends: its first election has begun.</summary>
        public Task PreVoteSent => _preVoteSent.Task;

        public int PreVotesSent => Volatile.Read(ref _preVotesSent);

        /// <summary>Lets the node's attempt to reach <paramref name="peerId"/> succeed.</summary>
        public void Reach(int peerId) => _reached[peerId].TrySetResult(true);

        public IReadOnlyList<int> GetPeerIds() => [2, 3];

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct) => _reached[peerId].Task.WaitAsync(ct);

        public async Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
        {
            Interlocked.Increment(ref _preVotesSent);
            _preVoteSent.TrySetResult();
            await Task.Delay(Timeout.Infinite, ct);
            throw new InvalidOperationException("unreachable");
        }

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
