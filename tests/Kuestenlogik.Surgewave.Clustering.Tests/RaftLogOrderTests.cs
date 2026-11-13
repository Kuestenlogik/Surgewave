using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// The log on disk holds its entries in the order of their indexes, however many callers propose at once (#262).
/// </summary>
/// <remarks>
/// A proposal took its index under the node's lock and was written after the lock was let go, so of two proposals
/// made at once the later index could reach <c>raft-log.bin</c> first. The node that loaded that file held a log
/// whose positions and indexes disagreed.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftLogOrderTests : IDisposable
{
    private const int Proposals = 2000;

    private readonly string _root = Path.Combine(Path.GetTempPath(), "surgewave-raft-order-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private ClusteringConfig NewConfig() => new()
    {
        BrokerId = 1, Host = "localhost", Port = 9092, ReplicationPort = 10092,
        RaftElectionTimeoutMinMs = 50, RaftElectionTimeoutMaxMs = 100, RaftHeartbeatIntervalMs = 25,
        RaftPeerDiscoveryTimeoutSeconds = 0,
        RaftDataDirectory = _root,
    };

    [Fact]
    public async Task ProposalsMadeAtOnceAreOnDiskInTheOrderOfTheirIndexes()
    {
        var ct = TestContext.Current.CancellationToken;
        var config = NewConfig();
        var state = new ClusterState();
        var node = new RaftNode(
            NullLogger<RaftNode>.Instance, config, new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            new SingleNodeRaftTransport(),
            new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, state, TestRaftNode.NewMembership(config, state)));
        await node.StartAsync(ct);
        await node.WaitForCaughtUpAsync(ct);

        var indexes = await Task.WhenAll(Enumerable.Range(0, Proposals).Select(i =>
            Task.Run(() => node.ProposeAsync(MetadataCommandType.Noop, BitConverter.GetBytes(i), ct), ct)));
        var last = indexes.Max();
        Assert.True(await node.WaitForCommitAsync(last, TimeSpan.FromSeconds(30), ct));
        await node.DisposeAsync();

        var onDisk = await new RaftPersistence(NullLogger<RaftPersistence>.Instance, config).LoadLogAsync(ct);

        Assert.Equal(Enumerable.Range(1, (int)last).Select(i => (long)i), onDisk.Select(e => e.Index));
        // Each proposal is the entry at the index it was told.
        Assert.All(Enumerable.Range(0, Proposals), i => Assert.Equal(BitConverter.GetBytes(i), onDisk[(int)indexes[i] - 1].Data));
    }
}
