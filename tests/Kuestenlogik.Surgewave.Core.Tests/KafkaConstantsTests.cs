using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Tests for KafkaConstants - verifying critical constants are correct per Kafka protocol specification.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class KafkaConstantsTests
{
    #region RecordBatch Header Layout Tests

    [Fact]
    public void RecordBatch_HeaderSize_Is61Bytes()
    {
        Assert.Equal(61, KafkaConstants.RecordBatch.HeaderSize);
    }

    [Fact]
    public void RecordBatch_FieldOffsets_AreCorrect()
    {
        Assert.Equal(0, KafkaConstants.RecordBatch.BaseOffsetOffset);
        Assert.Equal(8, KafkaConstants.RecordBatch.LengthOffset);
        Assert.Equal(12, KafkaConstants.RecordBatch.PartitionLeaderEpochOffset);
        Assert.Equal(16, KafkaConstants.RecordBatch.MagicOffset);
        Assert.Equal(17, KafkaConstants.RecordBatch.CrcOffset);
        Assert.Equal(21, KafkaConstants.RecordBatch.AttributesOffset);
        Assert.Equal(23, KafkaConstants.RecordBatch.LastOffsetDeltaOffset);
        Assert.Equal(27, KafkaConstants.RecordBatch.BaseTimestampOffset);
        Assert.Equal(35, KafkaConstants.RecordBatch.MaxTimestampOffset);
        Assert.Equal(43, KafkaConstants.RecordBatch.ProducerIdOffset);
        Assert.Equal(51, KafkaConstants.RecordBatch.ProducerEpochOffset);
        Assert.Equal(53, KafkaConstants.RecordBatch.BaseSequenceOffset);
        Assert.Equal(57, KafkaConstants.RecordBatch.RecordsCountOffset);
    }

    [Fact]
    public void RecordBatch_CrcStartOffset_Is21()
    {
        Assert.Equal(21, KafkaConstants.RecordBatch.CrcStartOffset);
    }

    #endregion

    #region Compression Constants Tests

    [Fact]
    public void Compression_ValuesMatchKafkaSpec()
    {
        Assert.Equal(0, KafkaConstants.Compression.None);
        Assert.Equal(1, KafkaConstants.Compression.Gzip);
        Assert.Equal(2, KafkaConstants.Compression.Snappy);
        Assert.Equal(3, KafkaConstants.Compression.Lz4);
        Assert.Equal(4, KafkaConstants.Compression.Zstd);
        Assert.Equal(0x07, KafkaConstants.Compression.Mask);
    }

    #endregion

    #region Attributes Tests

    [Fact]
    public void Attributes_BitFlags_AreCorrect()
    {
        Assert.Equal(0x08, KafkaConstants.Attributes.TimestampTypeBit);
        Assert.Equal(0x10, KafkaConstants.Attributes.IsTransactionalBit);
        Assert.Equal(0x20, KafkaConstants.Attributes.IsControlBatchBit);
    }

    [Fact]
    public void Attributes_IsTransactional_ChecksCorrectBit()
    {
        Assert.False(KafkaConstants.Attributes.IsTransactional(0));
        Assert.True(KafkaConstants.Attributes.IsTransactional(0x10));
        Assert.True(KafkaConstants.Attributes.IsTransactional(0x30)); // transactional + control
    }

    [Fact]
    public void Attributes_IsControlBatch_ChecksCorrectBit()
    {
        Assert.False(KafkaConstants.Attributes.IsControlBatch(0));
        Assert.True(KafkaConstants.Attributes.IsControlBatch(0x20));
    }

    [Fact]
    public void Attributes_IsLogAppendTime_ChecksCorrectBit()
    {
        Assert.False(KafkaConstants.Attributes.IsLogAppendTime(0));
        Assert.True(KafkaConstants.Attributes.IsLogAppendTime(0x08));
    }

    #endregion

    #region Magic Version Tests

    [Fact]
    public void Magic_V2_Is2()
    {
        Assert.Equal(2, KafkaConstants.Magic.V2);
    }

    #endregion

    #region Port Constants Tests

    [Fact]
    public void Ports_DefaultValues_AreCorrect()
    {
        Assert.Equal(9092, KafkaConstants.Ports.Kafka);
        Assert.Equal(9093, KafkaConstants.Ports.Grpc);
        Assert.Equal(10092, KafkaConstants.Ports.Replication);
    }

    #endregion

    #region Producer Constants Tests

    [Fact]
    public void Producer_NoProducerId_IsMinusOne()
    {
        Assert.Equal(-1, KafkaConstants.Producer.NoProducerId);
        Assert.Equal(-1, KafkaConstants.Producer.NoProducerEpoch);
        Assert.Equal(-1, KafkaConstants.Producer.NoSequence);
    }

    #endregion

    #region TransactionState Tests

    [Fact]
    public void TransactionState_AllValues_Exist()
    {
        var values = Enum.GetValues<KafkaConstants.TransactionState>();
        Assert.Equal(7, values.Length);
        Assert.Contains(KafkaConstants.TransactionState.Empty, values);
        Assert.Contains(KafkaConstants.TransactionState.Ongoing, values);
        Assert.Contains(KafkaConstants.TransactionState.PrepareCommit, values);
        Assert.Contains(KafkaConstants.TransactionState.PrepareAbort, values);
        Assert.Contains(KafkaConstants.TransactionState.CompleteCommit, values);
        Assert.Contains(KafkaConstants.TransactionState.CompleteAbort, values);
        Assert.Contains(KafkaConstants.TransactionState.Dead, values);
    }

    #endregion
}
