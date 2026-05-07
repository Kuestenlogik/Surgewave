using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Exceptions;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Unit tests for data integrity validation.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class DataIntegrityTests
{
    private readonly ITestOutputHelper _output;

    public DataIntegrityTests(ITestOutputHelper output)
    {
        _output = output;
    }

    /// <summary>
    /// Creates a minimal valid Kafka RecordBatch with correct CRC.
    /// </summary>
    private static byte[] CreateValidBatch(long baseOffset = 0, int recordCount = 1)
    {
        // Kafka RecordBatch header is 61 bytes minimum
        var batch = new byte[100];

        // BaseOffset (0-7)
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), baseOffset);

        // BatchLength (8-11) - length after first 12 bytes
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);

        // PartitionLeaderEpoch (12-15)
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(12, 4), 0);

        // Magic (16) = 2 for Kafka v2
        batch[16] = 2;

        // Attributes (21-22)
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(21, 2), 0);

        // LastOffsetDelta (23-26)
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(23, 4), recordCount - 1);

        // BaseTimestamp (27-34)
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(27, 8), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        // MaxTimestamp (35-42)
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(35, 8), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        // ProducerId (43-50)
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(43, 8), -1);

        // ProducerEpoch (51-52)
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(51, 2), -1);

        // BaseSequence (53-56)
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(53, 4), -1);

        // RecordCount (57-60)
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), recordCount);

        // Compute and write CRC (17-20) over bytes 21+
        var crc = Crc32C.Compute(batch.AsSpan(21));
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), crc);

        return batch;
    }

    [Fact]
    public void ValidateCrc_ValidBatch_ReturnsTrue()
    {
        // Arrange
        var batch = CreateValidBatch();

        // Act
        var isValid = RecordBatchValidator.ValidateCrc(batch, out var expected, out var actual);

        // Assert
        Assert.True(isValid);
        Assert.Equal(expected, actual);
        _output.WriteLine($"CRC validated: 0x{expected:X8}");
    }

    [Fact]
    public void ValidateCrc_CorruptedBatch_ReturnsFalse()
    {
        // Arrange
        var batch = CreateValidBatch();
        // Corrupt the data after CRC
        batch[50] ^= 0xFF;

        // Act
        var isValid = RecordBatchValidator.ValidateCrc(batch, out var expected, out var actual);

        // Assert
        Assert.False(isValid);
        Assert.NotEqual(expected, actual);
        _output.WriteLine($"Expected CRC: 0x{expected:X8}, Actual: 0x{actual:X8}");
    }

    [Fact]
    public void ValidateCrc_TooSmallBatch_ReturnsFalse()
    {
        // Arrange - batch smaller than minimum header size
        var batch = new byte[50];

        // Act
        var isValid = RecordBatchValidator.ValidateCrc(batch, out var expected, out var actual);

        // Assert
        Assert.False(isValid);
        Assert.Equal(0u, expected);
        Assert.Equal(0u, actual);
    }

    [Fact]
    public void ValidateCrc_EmptyBatch_ReturnsFalse()
    {
        // Act
        var isValid = RecordBatchValidator.ValidateCrc(Array.Empty<byte>());

        // Assert
        Assert.False(isValid);
    }

    [Fact]
    public void ValidateCrc_Overload_ReturnsCorrectValue()
    {
        // Arrange
        var batch = CreateValidBatch();

        // Act - use the simple overload
        var isValid = RecordBatchValidator.ValidateCrc(batch);

        // Assert
        Assert.True(isValid);
    }

    [Fact]
    public void GetBaseOffset_ValidBatch_ReturnsCorrectOffset()
    {
        // Arrange
        var batch = CreateValidBatch(baseOffset: 12345);

        // Act
        var offset = RecordBatchValidator.GetBaseOffset(batch);

        // Assert
        Assert.Equal(12345L, offset);
    }

    [Fact]
    public void GetBaseOffset_TooSmallBatch_ReturnsMinusOne()
    {
        // Arrange
        var batch = new byte[4];

        // Act
        var offset = RecordBatchValidator.GetBaseOffset(batch);

        // Assert
        Assert.Equal(-1L, offset);
    }

    [Fact]
    public void GetBatchLength_ValidBatch_ReturnsCorrectLength()
    {
        // Arrange
        var batch = CreateValidBatch();

        // Act
        var length = RecordBatchValidator.GetBatchLength(batch);

        // Assert
        Assert.Equal(batch.Length - 12, length);
    }

    [Fact]
    public void GetBatchLength_TooSmallBatch_ReturnsMinusOne()
    {
        // Arrange
        var batch = new byte[8];

        // Act
        var length = RecordBatchValidator.GetBatchLength(batch);

        // Assert
        Assert.Equal(-1, length);
    }

    [Fact]
    public void DataCorruptionException_Constructor_SetsPropertiesCorrectly()
    {
        // Arrange
        var topic = "test-topic";
        var partition = 1;
        var baseOffset = 100L;
        var expectedCrc = 0x12345678u;
        var actualCrc = 0xDEADBEEFu;

        // Act
        var ex = new DataCorruptionException(topic, partition, baseOffset, expectedCrc, actualCrc);

        // Assert
        Assert.Equal(topic, ex.Topic);
        Assert.Equal(partition, ex.Partition);
        Assert.Equal(baseOffset, ex.BaseOffset);
        Assert.Equal(expectedCrc, ex.ExpectedCrc);
        Assert.Equal(actualCrc, ex.ActualCrc);
        Assert.Contains("test-topic-1", ex.Message);
        Assert.Contains("offset 100", ex.Message);
        Assert.Contains("0x12345678", ex.Message);
        Assert.Contains("0xDEADBEEF", ex.Message);
        _output.WriteLine($"Exception message: {ex.Message}");
    }

    [Fact]
    public void DataCorruptionException_FromCorruptedBatchInfo_SetsPropertiesCorrectly()
    {
        // Arrange
        var info = new CorruptedBatchInfo(
            Topic: "test-topic",
            Partition: 2,
            BaseOffset: 200L,
            ExpectedCrc: 0xABCDEF00u,
            ActualCrc: 0x11111111u,
            BatchLength: 1024);

        // Act
        var ex = new DataCorruptionException(info);

        // Assert
        Assert.Equal("test-topic", ex.Topic);
        Assert.Equal(2, ex.Partition);
        Assert.Equal(200L, ex.BaseOffset);
        Assert.Equal(0xABCDEF00u, ex.ExpectedCrc);
        Assert.Equal(0x11111111u, ex.ActualCrc);
    }

    [Fact]
    public void CorruptedBatchInfo_RecordStruct_HasCorrectValues()
    {
        // Arrange & Act
        var info = new CorruptedBatchInfo(
            Topic: "my-topic",
            Partition: 5,
            BaseOffset: 500L,
            ExpectedCrc: 0x11223344u,
            ActualCrc: 0x55667788u,
            BatchLength: 2048);

        // Assert
        Assert.Equal("my-topic", info.Topic);
        Assert.Equal(5, info.Partition);
        Assert.Equal(500L, info.BaseOffset);
        Assert.Equal(0x11223344u, info.ExpectedCrc);
        Assert.Equal(0x55667788u, info.ActualCrc);
        Assert.Equal(2048, info.BatchLength);
    }

    [Fact]
    public void CorruptionRecoveryMode_HasExpectedValues()
    {
        // Assert
        Assert.Equal(0, (int)CorruptionRecoveryMode.SkipAndContinue);
        Assert.Equal(1, (int)CorruptionRecoveryMode.FailFast);
    }

    [Fact]
    public void ValidateCrc_MultipleValidBatches_AllReturnTrue()
    {
        // Arrange & Act & Assert
        for (int i = 0; i < 10; i++)
        {
            var batch = CreateValidBatch(baseOffset: i * 100, recordCount: i + 1);
            Assert.True(RecordBatchValidator.ValidateCrc(batch),
                $"Batch {i} with offset {i * 100} should be valid");
        }
    }

    [Fact]
    public void ValidateCrc_CorruptedAtDifferentPositions_AllReturnFalse()
    {
        // Test corruption at various positions after CRC region
        int[] corruptPositions = [21, 30, 50, 60, 80, 99];

        foreach (var pos in corruptPositions)
        {
            var batch = CreateValidBatch();
            batch[pos] ^= 0xFF; // Flip all bits at position

            var isValid = RecordBatchValidator.ValidateCrc(batch);
            Assert.False(isValid, $"Corruption at position {pos} should be detected");
        }
    }

    [Fact]
    public void ValidateCrc_CorruptedCrcField_DetectedByMismatch()
    {
        // Arrange
        var batch = CreateValidBatch();
        // Corrupt the CRC field itself (bytes 17-20)
        batch[17] ^= 0xFF;

        // Act
        var isValid = RecordBatchValidator.ValidateCrc(batch, out var expected, out var actual);

        // Assert
        Assert.False(isValid);
        // The expected CRC (from corrupted header) will not match actual computed CRC
        Assert.NotEqual(expected, actual);
    }
}
