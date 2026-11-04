using Kuestenlogik.Surgewave.Broker.Coordination;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// A transactional offset commit is a batch of the producer's transaction in the group's partition of
/// <c>__consumer_offsets</c>, as in Kafka (#197): pending until the transaction's marker lands there, committed by a
/// commit marker, dropped by an abort marker — and a broker that takes the partition over finds both in the log. The
/// offsets used to be staged with the transaction's coordinator and written by it into its own store, which in a
/// cluster was mostly not the group's coordinator.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TransactionalOffsetCommitTests : IDisposable
{
    private const int Partitions = 4;
    private const string Group = "billing";
    private const long ProducerId = 9;

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-txn-offsets-" + Guid.NewGuid().ToString("N"));
    private readonly LogManager _log;
    private readonly RecordBatchSerializer _serializer = new(NullLogger<RecordBatchSerializer>.Instance);
    private readonly CancellationToken _ct = TestContext.Current.CancellationToken;

    public TransactionalOffsetCommitTests()
    {
        _log = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
    }

    public void Dispose()
    {
        _log.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static int GroupPartition => CoordinatorPartitioner.PartitionFor(Group, Partitions);

    [Fact]
    public async Task TheOffsetsCountOnceTheCommitMarkerLands()
    {
        var store = await LocalStoreAsync();

        Assert.Equal(StateWriteStatus.Ok, await store.CommitTransactionalOffsetsAsync(Group, ProducerId, 0, [new CommittedOffset("orders", 0, 42)], _ct));
        Assert.Equal(-1, store.GetCommittedOffset(Group, "orders", 0));
        Assert.True(store.HasPendingOffsets(Group));

        await MarkAsync(store, commit: true);

        Assert.Equal(42, store.GetCommittedOffset(Group, "orders", 0));
        Assert.False(store.HasPendingOffsets(Group));
    }

    [Fact]
    public async Task AnAbortDropsThemAndKeepsWhatWasCommittedBefore()
    {
        var store = await LocalStoreAsync();
        await store.CommitOffsetAsync(Group, "orders", 0, 10, _ct);

        await store.CommitTransactionalOffsetsAsync(Group, ProducerId, 0, [new CommittedOffset("orders", 0, 42)], _ct);
        await MarkAsync(store, commit: false);

        Assert.Equal(10, store.GetCommittedOffset(Group, "orders", 0));
        Assert.False(store.HasPendingOffsets(Group));
    }

    [Fact]
    public async Task ABrokerThatTakesThePartitionOverReadsPendingOffsetsAndMarkersFromTheLog()
    {
        var first = await LocalStoreAsync();
        await first.CommitTransactionalOffsetsAsync(Group, ProducerId, 0, [new CommittedOffset("orders", 0, 42)], _ct);
        await MarkAsync(first, commit: true);
        await first.CommitTransactionalOffsetsAsync(Group, ProducerId + 1, 0, [new CommittedOffset("orders", 1, 7)], _ct);

        // Committed through its marker in the log; the second transaction is still open there.
        var next = await LocalStoreAsync();
        Assert.Equal(42, next.GetCommittedOffset(Group, "orders", 0));
        Assert.Equal(-1, next.GetCommittedOffset(Group, "orders", 1));
        Assert.True(next.HasPendingOffsets(Group));

        await MarkAsync(next, commit: true, ProducerId + 1);
        Assert.Equal(7, next.GetCommittedOffset(Group, "orders", 1));
    }

    // The transaction's marker as its partition's leader writes it: appended, then the store told.
    private async Task MarkAsync(OffsetStore store, bool commit, long producerId = ProducerId)
    {
        var partition = new TopicPartition { Topic = CoordinatorPartitioner.GroupTopic, Partition = GroupPartition };
        var marker = ControlBatchBuilder.BuildTransactionMarker(producerId, 0,
            commit ? KafkaConstants.ControlRecordType.Commit : KafkaConstants.ControlRecordType.Abort);
        await _log.AppendBatchAsync(partition, marker, _ct);
        store.CompleteTransaction(GroupPartition, producerId, commit);
    }

    private async Task<OffsetStore> LocalStoreAsync()
    {
        var store = new OffsetStore(_log, _serializer, Partitions, NullLogger<OffsetStore>.Instance);
        await store.StartLocalAsync(_ct);
        return store;
    }
}
