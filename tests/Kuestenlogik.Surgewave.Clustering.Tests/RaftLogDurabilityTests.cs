using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// An entry counts and a request is answered once the write is flushed to disk, not once it is written (#260).
/// </summary>
/// <remarks>
/// The log and the state file were written and left in the operating system's cache: metadata a client had been
/// acknowledged for survived the process and not a power loss, and so did a vote. A power loss is not staged here.
/// The flush is: each test holds it where the write is in the file and not yet on disk, and looks at what the node
/// says in that time — which is what it would have said to a peer before the power went. Every wait is for a signal.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftLogDurabilityTests : IAsyncDisposable
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private readonly string _root = Path.Combine(Path.GetTempPath(), "surgewave-raft-durability-" + Guid.NewGuid().ToString("N"));
    private readonly HeldFlush _flush = new();
    private readonly List<RaftNode> _nodes = [];

    public async ValueTask DisposeAsync()
    {
        // First, whatever the test found: a node's stop waits for the write the test holds.
        _flush.Release();
        foreach (var node in _nodes)
        {
            await node.DisposeAsync();
        }

        try { Directory.Delete(_root, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private string LogFile => Path.Combine(_root, "raft-log.bin");

    private ClusteringConfig NewConfig() => new()
    {
        BrokerId = 1, Host = "localhost", Port = 9092, ReplicationPort = 10092,
        RaftElectionTimeoutMinMs = 50, RaftElectionTimeoutMaxMs = 100, RaftHeartbeatIntervalMs = 25,
        RaftPeerDiscoveryTimeoutSeconds = 0,
        RaftDataDirectory = _root,
    };

    private RaftPersistence NewPersistence() => new(NullLogger<RaftPersistence>.Instance, NewConfig(), _flush.Flush);

    private RaftNode NewNode(IRaftTransport transport, params int[] voters)
    {
        var node = new RaftNode(
            NullLogger<RaftNode>.Instance, NewConfig(), NewPersistence(), transport, new NoOpStateMachine(),
            new ConfiguredVoterSet(voters));
        _nodes.Add(node);
        return node;
    }

    private static RaftLogEntry Entry(long index, int term = 1) => new()
    {
        Term = term, Index = index, CommandType = MetadataCommandType.Noop, Data = [(byte)index],
    };

    [Fact]
    public async Task ALeaderNeitherSendsNorCommitsAnEntryItHasNotFlushed()
    {
        var ct = TestContext.Current.CancellationToken;
        var peer = new AcknowledgingPeer();
        var node = NewNode(peer, 1, 2);
        await node.StartAsync(ct);
        await node.WaitForCaughtUpAsync(ct).WaitAsync(Bound, ct);

        _flush.HoldNext("raft-log.bin");
        var proposal = node.ProposeAsync(MetadataCommandType.Noop, [42], ct);
        var index = node.LastLogIndex;
        await _flush.Held.WaitAsync(Bound, ct);

        // Two more rounds to the other voter: the second is sent once the first was answered and counted.
        await peer.AskedAsync(times: 2).WaitAsync(Bound, ct);

        // The leader's copy is in its memory and in the file's cache, and that is where it stays.
        Assert.True(peer.HighestIndexSeen < index);
        Assert.True(node.CommitIndex < index);
        Assert.False(proposal.IsCompleted);

        _flush.Release();

        Assert.Equal(index, await proposal.WaitAsync(Bound, ct));
        Assert.True(await node.WaitForCommitAsync(index, Bound, ct));
        Assert.Equal(index, peer.HighestIndexSeen);
    }

    [Fact]
    public async Task AFollowerAnswersTheLeaderOnceTheEntriesAreFlushed()
    {
        var ct = TestContext.Current.CancellationToken;
        // Never started, so it does not campaign: it knows what the leader's request tells it.
        var node = NewNode(new AcknowledgingPeer(), 1, 2);
        Assert.True((await node.HandleAppendEntriesAsync(
            new AppendEntriesRequest(2, 2, 0, 0, [Entry(1, term: 2)], LeaderCommit: 0), ct)).Success);

        _flush.HoldNext("raft-log.bin");
        var answer = node.HandleAppendEntriesAsync(
            new AppendEntriesRequest(2, 2, 1, 2, [Entry(2, term: 2)], LeaderCommit: 1), ct);
        await _flush.Held.WaitAsync(Bound, ct);

        // A heartbeat that brings nothing says how far the log matches too, and that is the entry being written.
        var heartbeat = node.HandleAppendEntriesAsync(new AppendEntriesRequest(2, 2, 2, 2, [], LeaderCommit: 1), ct);

        Assert.False(answer.IsCompleted);
        Assert.False(heartbeat.IsCompleted);

        _flush.Release();

        Assert.Equal(new AppendEntriesResponse(2, true, 2), await answer.WaitAsync(Bound, ct));
        Assert.Equal(new AppendEntriesResponse(2, true, 2), await heartbeat.WaitAsync(Bound, ct));
    }

    [Fact]
    public async Task AVoteIsAnsweredOnceItIsFlushed()
    {
        var ct = TestContext.Current.CancellationToken;
        var node = NewNode(new AcknowledgingPeer(), 1, 2);

        _flush.HoldNext("raft-state.json.tmp");
        // The state is saved on the caller's thread.
        var vote = Task.Run(() => node.HandleRequestVoteAsync(new RequestVoteRequest(3, 2, 0, 0), ct), ct);
        await _flush.Held.WaitAsync(Bound, ct);

        Assert.False(vote.IsCompleted);

        _flush.Release();

        Assert.True((await vote.WaitAsync(Bound, ct)).VoteGranted);
        Assert.Equal(new RaftPersistentState(3, 2), await NewPersistence().LoadStateAsync(ct));
    }

    [Fact]
    public async Task WritesThatWaitForAFlushGoToDiskWithOneFlush()
    {
        var ct = TestContext.Current.CancellationToken;
        var persistence = NewPersistence();

        _flush.HoldNext("raft-log.bin");
        var first = persistence.AppendEntriesAsync([Entry(1)]);
        await _flush.Held.WaitAsync(Bound, ct);

        var waiting = Enumerable.Range(2, 50).Select(i => persistence.AppendEntriesAsync([Entry(i)])).ToArray();
        Assert.All(waiting, write => Assert.False(write.IsCompleted));

        _flush.Release();
        await first.WaitAsync(Bound, ct);
        await Task.WhenAll(waiting).WaitAsync(Bound, ct);

        Assert.Equal(2, _flush.Count("raft-log.bin"));
        Assert.Equal(Enumerable.Range(1, 51).Select(i => (long)i), (await NewPersistence().LoadLogAsync(ct)).Select(e => e.Index));
    }

    [Fact]
    public async Task AWriteThatFailsIsTheLast()
    {
        var ct = TestContext.Current.CancellationToken;
        var persistence = NewPersistence();
        await persistence.AppendEntriesAsync([Entry(1)]);
        var before = await File.ReadAllBytesAsync(LogFile, ct);

        _flush.FailNext("raft-log.bin");
        var failed = await Assert.ThrowsAsync<IOException>(() => persistence.AppendEntriesAsync([Entry(2)]));

        // The entry with index 3 would stand behind one nobody knows to be on disk.
        Assert.Same(failed, await Assert.ThrowsAsync<IOException>(() => persistence.AppendEntriesAsync([Entry(3)])));
        Assert.Same(failed, await Assert.ThrowsAsync<IOException>(() => persistence.LogWritten));
        Assert.Equal(before.Length + (before.Length - 8), new FileInfo(LogFile).Length);
    }

    /// <summary>
    /// The flush to disk, which a test can hold or fail for the next write to one file.
    /// </summary>
    private sealed class HeldFlush
    {
        private readonly Lock _lock = new();
        private readonly ManualResetEventSlim _released = new(initialState: true);
        private readonly Dictionary<string, int> _counts = [];
        private TaskCompletionSource _held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private string? _hold;
        private string? _fail;

        /// <summary>Completed when the flush <see cref="HoldNext"/> asked for has been reached.</summary>
        public Task Held
        {
            get
            {
                lock (_lock)
                {
                    return _held.Task;
                }
            }
        }

        public void HoldNext(string fileName)
        {
            lock (_lock)
            {
                _held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _hold = fileName;
                _released.Reset();
            }
        }

        public void FailNext(string fileName)
        {
            lock (_lock)
            {
                _fail = fileName;
            }
        }

        public void Release() => _released.Set();

        public int Count(string fileName)
        {
            lock (_lock)
            {
                return _counts.GetValueOrDefault(fileName);
            }
        }

        public void Flush(FileStream file)
        {
            var name = Path.GetFileName(file.Name);
            var hold = false;

            lock (_lock)
            {
                _counts[name] = _counts.GetValueOrDefault(name) + 1;

                if (_fail == name)
                {
                    _fail = null;
                    throw new IOException("The disk did not take the flush.");
                }

                if (_hold == name)
                {
                    _hold = null;
                    hold = true;
                    _held.TrySetResult();
                }
            }

            if (hold)
                _released.Wait();

            file.Flush(flushToDisk: true);
        }
    }

    /// <summary>
    /// The other voter: grants its vote, takes every entry and says so.
    /// </summary>
    private sealed class AcknowledgingPeer : IRaftTransport
    {
        private readonly Lock _lock = new();
        private long _highestIndexSeen;
        private int _awaited;
        private TaskCompletionSource _asked = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The highest index of an entry the leader sent.</summary>
        public long HighestIndexSeen
        {
            get
            {
                lock (_lock)
                {
                    return _highestIndexSeen;
                }
            }
        }

        /// <summary>Completed by the <paramref name="times"/>-th request that reaches the peer from now on.</summary>
        public Task AskedAsync(int times)
        {
            lock (_lock)
            {
                _asked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _awaited = times;
                return _asked.Task;
            }
        }

        public IReadOnlyList<int> GetPeerIds() => [2];

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct) => Task.FromResult(true);

        public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
            => Task.FromResult(new PreVoteResponse(request.ProposedTerm - 1, true));

        public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
            => Task.FromResult(new RequestVoteResponse(request.Term, true));

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
        {
            var matchIndex = request.PrevLogIndex + request.Entries.Length;

            lock (_lock)
            {
                if (request.Entries.Length > 0)
                    _highestIndexSeen = Math.Max(_highestIndexSeen, request.Entries[^1].Index);

                if (_awaited > 0 && --_awaited == 0)
                    _asked.TrySetResult();
            }

            return Task.FromResult(new AppendEntriesResponse(request.Term, true, matchIndex));
        }
    }

    private sealed class NoOpStateMachine : IRaftStateMachine
    {
        public void Apply(RaftLogEntry entry) { }

        public Task<byte[]> CreateSnapshotAsync(CancellationToken ct) => Task.FromResult(Array.Empty<byte>());

        public Task RestoreFromSnapshotAsync(byte[] snapshot, CancellationToken ct) => Task.CompletedTask;
    }
}
