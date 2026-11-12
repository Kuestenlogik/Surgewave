using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// A Raft node that was disposed writes nothing to its log and state files, and a log that cannot be read is not an
/// empty one.
/// </summary>
/// <remarks>
/// A broker restarted on the same directory lost its cluster: the node of the run before was disposed and went on
/// appending what its controller still proposed — its registration, the internal topics — to <c>raft-log.bin</c>,
/// with the file opened for itself alone; the node started next could not open it, logged that and went on with no
/// entries, as a broker that never ran. It then led a new cluster, with a new cluster id ("Assert.Equal() Failure:
/// Strings differ" in <c>ClusterIdTests.TheFirstControllerWritesAndPersistsTheIdAndALaterOneFindsItInTheLog</c>, now
/// and then on a loaded machine), and wrote its entries behind the ones it had not read. Nothing here waits
/// for time to pass: the node is disposed and then asked, and the log is held open while it is loaded.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class MetadataLogAfterStopTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "surgewave-log-after-stop-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private string LogFile => Path.Combine(_root, "raft-log.bin");
    private string StateFile => Path.Combine(_root, "raft-state.json");

    private ClusteringConfig NewConfig() => new()
    {
        BrokerId = 1, Host = "localhost", Port = 9092, ReplicationPort = 10092,
        RaftElectionTimeoutMinMs = 50, RaftElectionTimeoutMaxMs = 100, RaftHeartbeatIntervalMs = 25,
        RaftDataDirectory = _root,
    };

    private static RaftNode NewNode(ClusteringConfig config)
    {
        var state = new ClusterState();
        return new RaftNode(
            NullLogger<RaftNode>.Instance, config, new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            new SingleNodeRaftTransport(),
            new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, state, TestRaftNode.NewMembership(config, state)));
    }

    private static RaftLogEntry Noop(long index, int term) => new()
    {
        Term = term, Index = index, CommandType = MetadataCommandType.Noop, Data = [],
    };

    [Fact]
    public async Task ADisposedLeaderProposesNothingAndLeavesItsLogAsItWas()
    {
        var ct = TestContext.Current.CancellationToken;
        var node = NewNode(NewConfig());
        await node.StartAsync(ct);
        await node.WaitForCaughtUpAsync(ct);
        Assert.True(await node.ProposeAsync(MetadataCommandType.Noop, [], ct) > 0);

        await node.DisposeAsync();
        var index = node.LastLogIndex;
        var log = await File.ReadAllBytesAsync(LogFile, ct);

        // What a controller still taking office does with the node it was given.
        Assert.Equal(-1, await node.ProposeAsync(MetadataCommandType.Noop, [], ct));

        Assert.Equal(index, node.LastLogIndex);
        Assert.Equal(log, await File.ReadAllBytesAsync(LogFile, ct));
    }

    [Fact]
    public async Task ADisposedFollowerTakesNoEntryAndGrantsNoVote()
    {
        var ct = TestContext.Current.CancellationToken;
        // Never started, so it does not campaign: it knows what the leader's request tells it.
        var node = NewNode(NewConfig());
        Assert.True((await node.HandleAppendEntriesAsync(
            new AppendEntriesRequest(2, 7, 0, 0, [Noop(index: 1, term: 2)], LeaderCommit: 0), ct)).Success);

        await node.DisposeAsync();
        var log = await File.ReadAllBytesAsync(LogFile, ct);
        var state = await File.ReadAllTextAsync(StateFile, ct);

        // What a replication server still running passes on to the node it was given.
        var appended = await node.HandleAppendEntriesAsync(
            new AppendEntriesRequest(2, 7, 1, 2, [Noop(index: 2, term: 2)], LeaderCommit: 1), ct);
        var vote = await node.HandleRequestVoteAsync(new RequestVoteRequest(3, 7, 1, 2), ct);

        Assert.False(appended.Success);
        Assert.False(vote.VoteGranted);
        Assert.Equal(1, node.LastLogIndex);
        Assert.Equal(2, node.CurrentTerm);
        Assert.Equal(log, await File.ReadAllBytesAsync(LogFile, ct));
        Assert.Equal(state, await File.ReadAllTextAsync(StateFile, ct));
    }

    [Fact]
    public async Task ALogThatCannotBeReadIsAnErrorAndNotAnEmptyLog()
    {
        var ct = TestContext.Current.CancellationToken;
        var config = NewConfig();
        var persistence = new RaftPersistence(NullLogger<RaftPersistence>.Instance, config);
        await persistence.AppendEntriesAsync([Noop(index: 1, term: 1), Noop(index: 2, term: 1)], ct);
        var node = NewNode(config);

        // Held as a writer holds it while it appends.
        await using (new FileStream(LogFile, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        {
            await Assert.ThrowsAnyAsync<IOException>(() => persistence.LoadLogAsync(ct));

            // The node does not start on a log it could not read: it would lead a cluster without it.
            await Assert.ThrowsAnyAsync<IOException>(() => node.StartAsync(ct));
            Assert.False(node.IsLeader);
        }

        await node.DisposeAsync();
        Assert.Equal([1L, 2L], (await persistence.LoadLogAsync(ct)).Select(e => e.Index));
    }

    [Fact]
    public async Task ClosingAWriteGateWaitsForTheWritesItAdmittedAndAdmitsNoOther()
    {
        var gate = new WriteGate();
        Assert.True(gate.TryEnter());
        Assert.True(gate.TryEnter());

        var closed = gate.CloseAsync();

        Assert.False(gate.TryEnter());
        Assert.False(closed.IsCompleted);
        gate.Exit();
        Assert.False(closed.IsCompleted);
        gate.Exit();
        await closed.WaitAsync(TestContext.Current.CancellationToken);
        Assert.Same(closed, gate.CloseAsync());
    }

    [Fact]
    public void AWriteGateWithNothingAdmittedIsClosedAtOnce()
    {
        var gate = new WriteGate();
        gate.TryEnter();
        gate.Exit();

        Assert.True(gate.CloseAsync().IsCompletedSuccessfully);
        Assert.False(gate.TryEnter());
    }
}
