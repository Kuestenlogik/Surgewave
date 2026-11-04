using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Storage;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests.Storage;

/// <summary>
/// #60 Inc7 — the transaction-marker control-batch builder extracted from the Kafka-wire handler and
/// shared by both inter-broker wires. Verifies the batch is well-formed (control + transactional
/// attributes, single record), deterministic for fixed inputs, and distinguishes commit from abort.
/// </summary>
public class ControlBatchBuilderTests
{
    private const long FixedTs = 1_700_000_000_000;

    [Fact]
    public void BuildTransactionMarker_IsDeterministicForFixedInputs()
    {
        var a = ControlBatchBuilder.BuildTransactionMarker(555, 3, KafkaConstants.ControlRecordType.Commit, FixedTs);
        var b = ControlBatchBuilder.BuildTransactionMarker(555, 3, KafkaConstants.ControlRecordType.Commit, FixedTs);
        Assert.Equal(a, b);
    }

    [Fact]
    public void BuildTransactionMarker_CommitAndAbortDiffer()
    {
        var commit = ControlBatchBuilder.BuildTransactionMarker(555, 3, KafkaConstants.ControlRecordType.Commit, FixedTs);
        var abort = ControlBatchBuilder.BuildTransactionMarker(555, 3, KafkaConstants.ControlRecordType.Abort, FixedTs);
        Assert.NotEqual(commit, abort);
        Assert.Equal(commit.Length, abort.Length); // same shape, only the control-type byte differs
    }

    [Fact]
    public void BuildTransactionMarker_SetsControlAndTransactionalAttributes()
    {
        var batch = ControlBatchBuilder.BuildTransactionMarker(555, 3, KafkaConstants.ControlRecordType.Commit, FixedTs);

        // RecordBatch v2 layout: baseOffset(8) batchLength(4) leaderEpoch(4) magic(1) crc(4) attributes(2)...
        // The attributes int16 is at offset 8+4+4+1+4 = 21.
        var attributes = (short)((batch[21] << 8) | batch[22]);
        Assert.Equal(KafkaConstants.Attributes.IsControlBatchBit, (short)(attributes & KafkaConstants.Attributes.IsControlBatchBit));
        Assert.Equal(KafkaConstants.Attributes.IsTransactionalBit, (short)(attributes & KafkaConstants.Attributes.IsTransactionalBit));

        // Magic byte is v2.
        Assert.Equal(KafkaConstants.Magic.V2, batch[16]);
    }

    /// <summary>
    /// Kafka's control record (#226): key {version 0, type}, value {version 0, coordinator epoch} — what Kafka's
    /// <c>ControlRecordType.parse</c> and <c>EndTransactionMarker.deserialize</c> read. The type was in the value.
    /// </summary>
    [Theory]
    [InlineData(KafkaConstants.ControlRecordType.Commit)]
    [InlineData(KafkaConstants.ControlRecordType.Abort)]
    public void BuildTransactionMarker_TheControlRecordIsKafkas(short type)
    {
        var batch = ControlBatchBuilder.BuildTransactionMarker(555, 3, type, FixedTs, coordinatorEpoch: 0x01020304);

        var record = Assert.Single(new RecordBatchSerializer(Microsoft.Extensions.Logging.Abstractions.NullLogger<RecordBatchSerializer>.Instance).ParseRecordBatch(batch));
        Assert.Equal(new byte[] { 0, 0, 0, (byte)type }, record.Key.ToArray());
        Assert.Equal(new byte[] { 0, 0, 1, 2, 3, 4 }, record.Value.ToArray());
        Assert.Equal(type == KafkaConstants.ControlRecordType.Commit, TransactionalBatches.IsCommitMarker(record.Key.Span));
    }

    [Fact]
    public void BuildTransactionMarker_EncodesProducerId()
    {
        var batch = ControlBatchBuilder.BuildTransactionMarker(producerId: 0x0102030405060708, producerEpoch: 7, KafkaConstants.ControlRecordType.Commit, FixedTs);

        // producerId int64 sits after attributes(2) lastOffsetDelta(4) firstTs(8) maxTs(8) => offset 21+2+4+8+8 = 43.
        var producerId = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(batch.AsSpan(43, 8));
        Assert.Equal(0x0102030405060708, producerId);
    }
}
