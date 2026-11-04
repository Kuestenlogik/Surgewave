using Kuestenlogik.Surgewave.Broker.Coordination;
using Kuestenlogik.Surgewave.Broker.GroupOffsets;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Group offsets in <c>__consumer_offsets</c> (#205): what a store writes is what the next store over the same log
/// reads back, a store serves only the partitions it leads, and the records are Kafka's.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class OffsetStoreTests : IDisposable
{
    private const int Partitions = 4;

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-offsets-" + Guid.NewGuid().ToString("N"));
    private readonly LogManager _log;
    private readonly RecordBatchSerializer _serializer = new(NullLogger<RecordBatchSerializer>.Instance);
    private readonly CancellationToken _ct = TestContext.Current.CancellationToken;

    public OffsetStoreTests()
    {
        _log = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
    }

    public void Dispose()
    {
        _log.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task ACommitIsReadBackByTheNextStoreOverTheLog()
    {
        var first = await LocalStoreAsync();
        Assert.Equal(StateWriteStatus.Ok, await first.CommitOffsetsAsync("orders-app",
            [new CommittedOffset("orders", 0, 42), new CommittedOffset("orders", 1, 7)], _ct));
        Assert.Equal(StateWriteStatus.Ok, await first.CommitOffsetAsync("orders-app", "orders", 0, 43, _ct));

        // A restart, or another broker taking the partition over: everything comes from the log.
        var next = await LocalStoreAsync();

        Assert.Equal(43, next.GetCommittedOffset("orders-app", "orders", 0));
        Assert.Equal(7, next.GetCommittedOffset("orders-app", "orders", 1));
        Assert.Equal(-1, next.GetCommittedOffset("orders-app", "orders", 2));
        Assert.Contains("orders-app", next.GetGroupIds());
    }

    [Fact]
    public async Task DeletedOffsetsAndGroupsStayDeleted()
    {
        var first = await LocalStoreAsync();
        await first.CommitOffsetsAsync("keeps", [new CommittedOffset("orders", 0, 1), new CommittedOffset("orders", 1, 2)], _ct);
        await first.CommitOffsetAsync("goes", "orders", 0, 3, _ct);

        Assert.Equal(StateWriteStatus.Ok, await first.DeleteOffsetAsync("keeps", "orders", 1, _ct));
        Assert.Equal(StateWriteStatus.Ok, await first.DeleteGroupAsync("goes", _ct));
        Assert.Equal(-1, first.GetCommittedOffset("goes", "orders", 0));

        var next = await LocalStoreAsync();
        Assert.Equal(1, next.GetCommittedOffset("keeps", "orders", 0));
        Assert.Equal(-1, next.GetCommittedOffset("keeps", "orders", 1));
        Assert.Equal(-1, next.GetCommittedOffset("goes", "orders", 0));
        Assert.DoesNotContain("goes", next.GetGroupIds());
    }

    [Fact]
    public async Task TheGenerationAGroupReachedIsReadBack()
    {
        var first = await LocalStoreAsync();
        Assert.Equal(StateWriteStatus.Ok, await first.RecordGenerationAsync("orders-app", "consumer", 5, "range", "member-1", _ct));

        var next = await LocalStoreAsync();
        Assert.Equal(5, next.GetGeneration("orders-app"));
        Assert.Equal(0, next.GetGeneration("never-joined"));
    }

    [Fact]
    public async Task AStoreThatLosesThePartitionNeitherServesNorWritesItsGroups()
    {
        var store = await LocalStoreAsync();
        await store.CommitOffsetAsync("orders-app", "orders", 0, 42, _ct);

        store.OnLeadershipChanged(PartitionOf("orders-app"), isLeader: false);

        Assert.Equal(CoordinatorOwnership.NotOwned, store.OwnershipOf("orders-app"));
        Assert.Equal(-1, store.GetCommittedOffset("orders-app", "orders", 0));
        Assert.Equal(StateWriteStatus.NotCoordinator, await store.CommitOffsetAsync("orders-app", "orders", 0, 43, _ct));
    }

    [Fact]
    public async Task ABrokerThatTakesThePartitionOverServesWhatThePreviousLeaderCommitted()
    {
        var previous = await LocalStoreAsync();
        await previous.CommitOffsetAsync("orders-app", "orders", 0, 42, _ct);

        // A store that leads nothing yet, then is told it leads the group's partition.
        var takeover = new OffsetStore(_log, _serializer, Partitions, NullLogger<OffsetStore>.Instance);
        Assert.Equal(CoordinatorOwnership.NotOwned, takeover.OwnershipOf("orders-app"));

        takeover.OnLeadershipChanged(PartitionOf("orders-app"), isLeader: true);
        await takeover.WhenLoadedAsync("orders-app").WaitAsync(TimeSpan.FromSeconds(30), _ct);

        Assert.Equal(CoordinatorOwnership.Owned, takeover.OwnershipOf("orders-app"));
        Assert.Equal(42, takeover.GetCommittedOffset("orders-app", "orders", 0));
    }

    [Fact]
    public async Task TheRecordsAreKafkasOffsetCommitAndGroupMetadataShapes()
    {
        var store = await LocalStoreAsync();
        await store.CommitOffsetAsync("g", "t", 3, 99, _ct);
        await store.RecordGenerationAsync("g", "consumer", 2, "range", "m", _ct);

        var tp = new Kuestenlogik.Surgewave.Core.Models.TopicPartition { Topic = CoordinatorPartitioner.GroupTopic, Partition = PartitionOf("g") };
        var messages = (await _log.ReadBatchesAsync(tp, 0, cancellationToken: _ct)).SelectMany(_serializer.ParseRecordBatch).ToList();
        Assert.Equal(2, messages.Count);

        // OffsetCommitKey v1: version, group, topic, partition.
        Assert.Equal(new byte[] { 0, 1, 0, 1, (byte)'g', 0, 1, (byte)'t', 0, 0, 0, 3 }, messages[0].Key.ToArray());
        Assert.Equal(new GroupRecord.OffsetCommitted("g", "t", 3, 99), GroupRecords.Read(messages[0].Key.Span, messages[0].Value.Span));

        // GroupMetadataKey v2: version, group.
        Assert.Equal(new byte[] { 0, 2, 0, 1, (byte)'g' }, messages[1].Key.ToArray());
        Assert.Equal(new GroupRecord.GroupMetadata("g", 2, "consumer"), GroupRecords.Read(messages[1].Key.Span, messages[1].Value.Span));
    }

    /// <summary>
    /// A broker that takes a group over from the log describes and lists it — Empty, with the protocol type of its
    /// metadata record — before any member joined it (#224). It answered INVALID_GROUP_ID, so the Kafka tools could
    /// not describe a group after a change of coordinator.
    /// </summary>
    [Fact]
    public async Task AGroupTakenOverFromTheLogIsDescribedAndListedAsEmpty()
    {
        var first = await LocalStoreAsync();
        await first.CommitOffsetAsync("orders-app", "orders", 0, 42, _ct);
        await first.RecordGenerationAsync("orders-app", "consumer", 3, "range", "m", _ct);

        var takeover = new ConsumerGroupCoordinator(NullLogger<ConsumerGroupCoordinator>.Instance, await LocalStoreAsync());

        var described = Assert.Single(takeover.DescribeGroups(["orders-app"]));
        Assert.Equal((Kuestenlogik.Surgewave.Coordination.Consumer.ConsumerGroupErrorStatus.None, "Empty", "consumer"),
            (described.Status, described.GroupState, described.ProtocolType));
        var listed = Assert.Single(takeover.ListGroups(null), g => g.GroupId == "orders-app");
        Assert.Equal("Empty", listed.GroupState);
        Assert.Equal(Kuestenlogik.Surgewave.Coordination.Consumer.ConsumerGroupErrorStatus.InvalidGroupId,
            Assert.Single(takeover.DescribeGroups(["never-seen"])).Status);
    }

    private async Task<OffsetStore> LocalStoreAsync()
    {
        var store = new OffsetStore(_log, _serializer, Partitions, NullLogger<OffsetStore>.Instance);
        await store.StartLocalAsync(_ct);
        return store;
    }

    private static int PartitionOf(string group) => CoordinatorPartitioner.PartitionFor(group, Partitions);
}
