using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// Three voters, one of them stopped (#225): the two left are a majority and must have a leader. A leader that counted
/// the other running voter unreachable too — it did not answer within the isolation timeout, under load — stepped
/// down as isolated, and the cluster stayed without a leader for minutes: each election waited for every voter's
/// answer, the stopped one's included, which came only once its connect attempts failed — so the two running voters
/// campaigned in step, each voting for itself, round after round.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftQuorumLossTests : IAsyncDisposable
{
    private const int IsolationTimeoutMs = 600;

    // A stopped broker refuses nothing: a call to it fails once the connect attempts gave up, which takes seconds.
    private static readonly TimeSpan StoppedFailsAfter = TimeSpan.FromSeconds(7);

    private readonly string _directory = Path.Combine(Path.GetTempPath(), "surgewave-raft-quorum-" + Guid.NewGuid().ToString("N"));
    private readonly Network _network = new() { StoppedFailsAfter = StoppedFailsAfter };

    [Fact]
    public async Task TwoOfThreeVotersElectALeaderAfterTheLeaderSteppedDownAsIsolated()
    {
        var ct = TestContext.Current.CancellationToken;
        await _network.StartAsync([1, 2, 3], NewNode, ct);
        var leader = await _network.WaitForLeaderAsync(ct);

        var stopped = _network.Nodes.Keys.First(id => id != leader.NodeId);
        var slow = _network.Nodes.Keys.First(id => id != leader.NodeId && id != stopped);
        _network.Stop(stopped);

        // The other voter does not answer for longer than the isolation timeout: the leader steps down.
        var steppedDown = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        leader.OnQuorumLost += (_, _) => steppedDown.TrySetResult();
        _network.Hold(slow);
        await steppedDown.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);

        // It answers again; the two voters are a majority.
        _network.Release(slow);
        var next = await _network.WaitForLeaderAsync(ct, except: stopped);

        Assert.NotEqual(stopped, next.NodeId);
    }

    private RaftNode NewNode(int id, IRaftTransport transport)
    {
        var config = new ClusteringConfig
        {
            BrokerId = id,
            RaftElectionTimeoutMinMs = 100,
            RaftElectionTimeoutMaxMs = 200,
            RaftHeartbeatIntervalMs = 25,
            RaftIsolationTimeoutMs = IsolationTimeoutMs,
            RaftPeerDiscoveryTimeoutSeconds = 0,
            RaftDataDirectory = Path.Combine(_directory, id.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        };
        return new RaftNode(
            NullLogger<RaftNode>.Instance,
            config,
            new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            transport,
            new NoOpStateMachine(),
            new FixedVoterSet([1, 2, 3]));
    }

    public async ValueTask DisposeAsync()
    {
        await _network.DisposeAsync();
        try { if (Directory.Exists(_directory)) Directory.Delete(_directory, recursive: true); }
        catch (IOException) { /* a node still holding its log file is not a failure */ }
    }

    private sealed class NoOpStateMachine : IRaftStateMachine
    {
        public void Apply(RaftLogEntry entry) { }

        public Task<byte[]> CreateSnapshotAsync(CancellationToken ct) => Task.FromResult(Array.Empty<byte>());

        public Task RestoreFromSnapshotAsync(byte[] snapshot, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class FixedVoterSet(IReadOnlyList<int> voterIds) : IRaftVoterSet
    {
        public IReadOnlyList<int> VoterIds { get; } = voterIds;

        public int Majority => (VoterIds.Count / 2) + 1;
    }

    /// <summary>
    /// Nodes that reach each other in memory. A stopped node's calls fail at once, as a refused connection does; a held
    /// node's calls, to it and from it, wait until it is released, as a node too busy to answer.
    /// </summary>
    private sealed class Network : IAsyncDisposable
    {
        private readonly ConcurrentDictionary<int, bool> _stopped = new();
        private readonly ConcurrentDictionary<int, TaskCompletionSource> _held = new();

        public ConcurrentDictionary<int, RaftNode> Nodes { get; } = new();

        // Signalled whenever a node may have become leader, so a wait for one is a handshake rather than a poll.
        private TaskCompletionSource _changed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TimeSpan StoppedFailsAfter { get; init; }

        public async Task StartAsync(int[] ids, Func<int, IRaftTransport, RaftNode> create, CancellationToken ct)
        {
            foreach (var id in ids)
                Nodes[id] = create(id, new Transport(this, id, [.. ids.Where(p => p != id)]));
            foreach (var node in Nodes.Values)
                await node.StartAsync(ct);
        }

        public void Stop(int id) => _stopped[id] = true;

        public void Hold(int id) => _held[id] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Release(int id)
        {
            if (_held.TryRemove(id, out var held))
                held.TrySetResult();
        }

        public async Task<RaftNode> WaitForLeaderAsync(CancellationToken ct, int? except = null)
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(TimeSpan.FromSeconds(20));
            while (true)
            {
                var changed = Volatile.Read(ref _changed);
                // A leader the other running nodes follow: one that was leader a moment ago may already be replaced.
                var running = Nodes.Values.Where(n => !_stopped.ContainsKey(n.NodeId)).ToList();
                var leader = running.FirstOrDefault(n => n.IsLeader && n.NodeId != except
                    && running.All(o => o.LeaderId == n.NodeId && o.CurrentTerm == n.CurrentTerm));
                if (leader is not null)
                    return leader;
                try
                {
                    await changed.Task.WaitAsync(timeout.Token);
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                {
                    throw new TimeoutException("no leader: " + string.Join("; ", Nodes.Values.Select(n =>
                        $"node {n.NodeId} term {n.CurrentTerm} leader {n.LeaderId?.ToString() ?? "-"}{(n.IsLeader ? " (leader)" : "")}{(_stopped.ContainsKey(n.NodeId) ? " (stopped)" : "")}")));
                }
            }
        }

        // Every answer may have changed who leads.
        private void Changed() => Interlocked.Exchange(ref _changed, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).TrySetResult();

        private async Task<T> CallAsync<T>(int to, Func<RaftNode, Task<T>> call, CancellationToken ct)
        {
            if (_stopped.ContainsKey(to))
            {
                await Task.Delay(StoppedFailsAfter, ct);
                throw new RaftTransportException($"peer {to} is stopped");
            }
            if (_held.TryGetValue(to, out var held))
                await held.Task.WaitAsync(ct);
            try
            {
                return await call(Nodes[to]);
            }
            finally
            {
                Changed();
            }
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var id in _held.Keys)
                Release(id);
            foreach (var node in Nodes.Values)
                await node.DisposeAsync();
        }

        private sealed class Transport(Network network, int self, int[] peers) : IRaftTransport
        {
            public IReadOnlyList<int> GetPeerIds() => peers;

            public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct) => Task.FromResult(!network._stopped.ContainsKey(peerId));

            public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct) =>
                FromSelf(() => network.CallAsync(peerId, n => n.HandlePreVoteAsync(request, ct), ct));

            public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct) =>
                FromSelf(() => network.CallAsync(peerId, n => n.HandleRequestVoteAsync(request, ct), ct));

            public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct) =>
                FromSelf(() => network.CallAsync(peerId, n => n.HandleAppendEntriesAsync(request, ct), ct));

            // A stopped node sends nothing either, and a held one only once it is released.
            private async Task<T> FromSelf<T>(Func<Task<T>> send)
            {
                if (network._stopped.ContainsKey(self))
                    throw new RaftTransportException($"node {self} is stopped");
                if (network._held.TryGetValue(self, out var held))
                    await held.Task;
                return await send();
            }
        }
    }
}
