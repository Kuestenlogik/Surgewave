using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests.Transactions;

/// <summary>
/// An aborted transaction hides its own batches from a read-committed consumer, from its first batch on the partition
/// to its abort marker — not the producer's batches after it. The aborted range had no end, so a producer that
/// committed after an abort stayed invisible to every read-committed consumer of the partition.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TransactionIndexAbortRangeTests
{
    private const long ProducerId = 4711;
    private static readonly TopicPartition Orders = new() { Topic = "orders", Partition = 0 };

    [Fact]
    public void ABatchCommittedAfterAnAbortOfTheSameProducerIsRead()
    {
        var index = new TransactionIndex();

        index.RecordTransactionalBatch(Orders, ProducerId, baseOffset: 0);
        index.AbortTransaction(ProducerId, [Orders], abortOffset: 1);
        index.RecordTransactionalBatch(Orders, ProducerId, baseOffset: 2);
        index.CommitTransaction(ProducerId, [Orders], commitOffset: 3);

        var read = index.FilterForReadCommitted(Orders, [Batch(0), Batch(2)], highWatermark: 4);

        Assert.Equal([2L], read.Select(b => BinaryPrimitives.ReadInt64BigEndian(b)));
    }

    [Fact]
    public void AnAbortOfAProducerThatWroteNothingHereHidesNothing()
    {
        var index = new TransactionIndex();

        index.AbortTransaction(ProducerId, [Orders], abortOffset: 5);

        Assert.False(index.IsBatchAborted(Orders, ProducerId, batchBaseOffset: 2));
    }

    // A record batch header of the producer, transactional, at the given base offset.
    private static byte[] Batch(long baseOffset)
    {
        var batch = new byte[KafkaConstants.RecordBatch.HeaderSize];
        BinaryPrimitives.WriteInt64BigEndian(batch, baseOffset);
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(KafkaConstants.RecordBatch.AttributesOffset), KafkaConstants.Attributes.IsTransactionalBit);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(KafkaConstants.RecordBatch.ProducerIdOffset), ProducerId);
        return batch;
    }
}
