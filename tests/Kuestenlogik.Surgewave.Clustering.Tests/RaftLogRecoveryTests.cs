using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// What a node finds on disk after a crash is either the log it wrote or a reason not to start (#261).
/// </summary>
/// <remarks>
/// An entry carried no checksum and a read error inside one meant "skip": a write the crash tore was loaded as an
/// entry with fewer bytes than it said, a flipped byte as the entry it then spelled, and a state file that could not
/// be read as term 0 with no vote — on which a node votes a second time in a term it had voted in. The files are
/// damaged here the way a crash or a disk damages them, by their length and their bytes; nothing waits.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftLogRecoveryTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "surgewave-raft-recovery-" + Guid.NewGuid().ToString("N"));

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
        RaftPeerDiscoveryTimeoutSeconds = 0,
        RaftDataDirectory = _root,
    };

    private RaftPersistence NewPersistence() => new(NullLogger<RaftPersistence>.Instance, NewConfig());

    private RaftNode NewNode()
    {
        var config = NewConfig();
        var state = new ClusterState();
        return new RaftNode(
            NullLogger<RaftNode>.Instance, config, new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            new SingleNodeRaftTransport(),
            new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, state, TestRaftNode.NewMembership(config, state)));
    }

    private static RaftLogEntry Entry(long index, int term = 1) => new()
    {
        Term = term, Index = index, CommandType = MetadataCommandType.Noop, Timestamp = 1_700_000_000_000 + index,
        Data = [1, 2, 3, 4, 5, 6, 7, (byte)index],
    };

    /// <summary>Three entries, each written by a call of its own; the file's length after each.</summary>
    private async Task<long[]> WriteThreeEntriesAsync()
    {
        var persistence = NewPersistence();
        var ends = new long[3];
        for (var i = 0; i < 3; i++)
        {
            await persistence.AppendEntriesAsync([Entry(i + 1)]);
            ends[i] = new FileInfo(LogFile).Length;
        }

        return ends;
    }

    [Fact]
    public async Task WhatWasWrittenIsWhatIsLoaded()
    {
        var ct = TestContext.Current.CancellationToken;
        await WriteThreeEntriesAsync();

        var loaded = await NewPersistence().LoadLogAsync(ct);

        Assert.Equal([1L, 2L, 3L], loaded.Select(e => e.Index));
        Assert.All(loaded, e =>
        {
            var written = Entry(e.Index);
            Assert.Equal(written.Term, e.Term);
            Assert.Equal(written.CommandType, e.CommandType);
            Assert.Equal(written.Timestamp, e.Timestamp);
            Assert.Equal(written.Data, e.Data);
        });
    }

    [Fact]
    public async Task ATornLastEntryIsCutOff_AndTheLogGoesOnBehindTheLastWholeOne()
    {
        var ct = TestContext.Current.CancellationToken;
        var ends = await WriteThreeEntriesAsync();

        // The crash came while the third entry was being written: half of it reached the file.
        await using (var file = new FileStream(LogFile, FileMode.Open, FileAccess.Write))
        {
            file.SetLength(ends[1] + ((ends[2] - ends[1]) / 2));
        }

        var persistence = NewPersistence();
        Assert.Equal([1L, 2L], (await persistence.LoadLogAsync(ct)).Select(e => e.Index));
        Assert.Equal(ends[1], new FileInfo(LogFile).Length);

        await persistence.AppendEntriesAsync([Entry(3, term: 2)]);

        var reloaded = await NewPersistence().LoadLogAsync(ct);
        Assert.Equal([1L, 2L, 3L], reloaded.Select(e => e.Index));
        Assert.Equal(2, reloaded[2].Term);
    }

    [Fact]
    public async Task ATailTheFileSystemLeftAsZerosIsCutOff()
    {
        var ct = TestContext.Current.CancellationToken;
        var ends = await WriteThreeEntriesAsync();

        // The file had grown for the next entries, and their bytes had not been written when the power went.
        await using (var file = new FileStream(LogFile, FileMode.Open, FileAccess.Write))
        {
            file.SetLength(ends[2] + 100);
        }

        Assert.Equal([1L, 2L, 3L], (await NewPersistence().LoadLogAsync(ct)).Select(e => e.Index));
        Assert.Equal(ends[2], new FileInfo(LogFile).Length);
    }

    [Fact]
    public async Task ADamagedEntryWithEntriesBehindItIsRefused_AndTheNodeDoesNotStart()
    {
        var ct = TestContext.Current.CancellationToken;
        var ends = await WriteThreeEntriesAsync();
        var before = await File.ReadAllBytesAsync(LogFile, ct);

        // One byte of the second entry's data, which is where an entry ends.
        var damaged = (byte[])before.Clone();
        damaged[ends[1] - 1] ^= 0xFF;
        await File.WriteAllBytesAsync(LogFile, damaged, ct);

        await Assert.ThrowsAsync<InvalidDataException>(() => NewPersistence().LoadLogAsync(ct));

        var node = NewNode();
        await Assert.ThrowsAsync<InvalidDataException>(() => node.StartAsync(ct));
        Assert.False(node.IsLeader);
        await node.DisposeAsync();

        // Refused, and left for whoever repairs it: nothing was cut off and nothing appended.
        Assert.Equal(damaged, await File.ReadAllBytesAsync(LogFile, ct));
    }

    [Fact]
    public async Task ANodeDoesNotWriteBehindALogItRefused()
    {
        var ct = TestContext.Current.CancellationToken;
        var ends = await WriteThreeEntriesAsync();
        var damaged = await File.ReadAllBytesAsync(LogFile, ct);
        damaged[ends[0] - 1] ^= 0xFF;
        await File.WriteAllBytesAsync(LogFile, damaged, ct);

        var persistence = NewPersistence();

        await Assert.ThrowsAsync<InvalidDataException>(() => persistence.AppendEntriesAsync([Entry(4)]));
        Assert.Equal(damaged, await File.ReadAllBytesAsync(LogFile, ct));
    }

    [Fact]
    public async Task AFileThatIsNotAMetadataLogIsRefused()
    {
        var ct = TestContext.Current.CancellationToken;
        Directory.CreateDirectory(_root);
        await File.WriteAllBytesAsync(LogFile, new byte[64].Select((_, i) => (byte)(i + 1)).ToArray(), ct);

        await Assert.ThrowsAsync<InvalidDataException>(() => NewPersistence().LoadLogAsync(ct));
    }

    [Theory]
    [InlineData("")]
    [InlineData("{\"CurrentTerm\":7,\"Vot")]
    [InlineData("null")]
    public async Task AStateFileThatCannotBeReadIsRefused_AndTheNodeDoesNotStart(string content)
    {
        var ct = TestContext.Current.CancellationToken;
        Directory.CreateDirectory(_root);
        await File.WriteAllTextAsync(StateFile, content, ct);

        await Assert.ThrowsAsync<InvalidDataException>(() => NewPersistence().LoadStateAsync(ct));

        var node = NewNode();
        await Assert.ThrowsAsync<InvalidDataException>(() => node.StartAsync(ct));
        await node.DisposeAsync();

        // Not replaced by a state of term 0: the node would vote again in a term it voted in.
        Assert.Equal(content, await File.ReadAllTextAsync(StateFile, ct));
    }

    [Fact]
    public async Task ANodeThatNeverRanStartsWithoutAStateFileOrALog()
    {
        var ct = TestContext.Current.CancellationToken;
        var persistence = NewPersistence();

        Assert.Equal(new RaftPersistentState(0, null), await persistence.LoadStateAsync(ct));
        Assert.Empty(await persistence.LoadLogAsync(ct));
    }

    [Fact]
    public async Task ASnapshotThatCannotBeReadIsAnErrorAndNotAMissingSnapshot()
    {
        var ct = TestContext.Current.CancellationToken;
        var persistence = NewPersistence();
        await persistence.SaveSnapshotAsync(new RaftSnapshot(EndOffset: 12, Epoch: 3, Data: [9, 8, 7, 6, 5, 4]), ct);
        var file = Assert.Single(Directory.GetFiles(Path.Combine(_root, "snapshots"), "snapshot-*.bin"));

        Assert.Equal([9, 8, 7, 6, 5, 4], (await persistence.LoadSnapshotAsync(12, 3, ct))!.Data);
        Assert.Null(await persistence.LoadSnapshotAsync(13, 3, ct));

        // Half of it is gone.
        await using (var stream = new FileStream(file, FileMode.Open, FileAccess.Write))
        {
            stream.SetLength(stream.Length - 3);
        }

        await Assert.ThrowsAsync<InvalidDataException>(() => persistence.LoadSnapshotAsync(12, 3, ct));
        Assert.Throws<InvalidDataException>(() => persistence.GetLatestSnapshotInfo());
    }
}
