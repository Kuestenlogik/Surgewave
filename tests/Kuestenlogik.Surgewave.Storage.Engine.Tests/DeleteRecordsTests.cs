using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// Tests for delete records functionality in the storage layer.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class DeleteRecordsTests : IDisposable
{
    private readonly string _testDirectory;

    public DeleteRecordsTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"kl-streaming-delete-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDirectory);
    }

    [Fact]
    public async Task PartitionLog_DeleteRecordsToOffset_UpdatesLogStartOffset()
    {
        // Arrange
        var topicPartition = new TopicPartition { Topic = "test-topic", Partition = 0 };
        var log = new PartitionLog(_testDirectory, topicPartition, FileLogSegmentFactory.Create());

        // Write some batches
        for (int i = 0; i < 5; i++)
        {
            var batch = CreateSimpleRecordBatch(baseOffset: i * 2, recordCount: 2);
            await log.AppendBatchAsync(batch);
        }

        // Verify initial state
        Assert.Equal(0, log.LogStartOffset);
        Assert.Equal(10, log.NextOffset);

        // Act - Delete records up to offset 4
        var newLogStart = log.DeleteRecordsToOffset(4);

        // Assert
        Assert.Equal(4, newLogStart);
        Assert.Equal(4, log.LogStartOffset);
        Assert.Equal(10, log.NextOffset); // High watermark unchanged

        log.Dispose();
    }

    [Fact]
    public async Task PartitionLog_DeleteRecordsToOffset_PreventsReadsBelowLogStart()
    {
        // Arrange
        var topicPartition = new TopicPartition { Topic = "test-topic", Partition = 0 };
        var log = new PartitionLog(_testDirectory, topicPartition, FileLogSegmentFactory.Create());

        // Write some batches
        for (int i = 0; i < 5; i++)
        {
            var batch = CreateSimpleRecordBatch(baseOffset: i * 2, recordCount: 2);
            await log.AppendBatchAsync(batch);
        }

        // Delete records up to offset 6
        log.DeleteRecordsToOffset(6);

        // Act - Try to read from deleted offset
        var batches = await log.ReadBatchesAsync(0, maxBytes: 1024 * 1024);

        // Assert - Should return empty since offset 0 < LogStartOffset
        Assert.Empty(batches);

        log.Dispose();
    }

    [Fact]
    public async Task PartitionLog_DeleteRecordsToOffset_MinusOneMeansDeleteAll()
    {
        // Arrange
        var topicPartition = new TopicPartition { Topic = "test-topic", Partition = 0 };
        var log = new PartitionLog(_testDirectory, topicPartition, FileLogSegmentFactory.Create());

        // Write some batches
        for (int i = 0; i < 3; i++)
        {
            var batch = CreateSimpleRecordBatch(baseOffset: i * 2, recordCount: 2);
            await log.AppendBatchAsync(batch);
        }

        Assert.Equal(6, log.NextOffset);

        // Act - Delete all records (offset = -1 means delete up to high watermark)
        var newLogStart = log.DeleteRecordsToOffset(-1);

        // Assert
        Assert.Equal(6, newLogStart);
        Assert.Equal(6, log.LogStartOffset);

        log.Dispose();
    }

    [Fact]
    public async Task PartitionLog_DeleteRecordsToOffset_CannotDecreaseBelowCurrentLogStart()
    {
        // Arrange
        var topicPartition = new TopicPartition { Topic = "test-topic", Partition = 0 };
        var log = new PartitionLog(_testDirectory, topicPartition, FileLogSegmentFactory.Create());

        for (int i = 0; i < 3; i++)
        {
            var batch = CreateSimpleRecordBatch(baseOffset: i * 2, recordCount: 2);
            await log.AppendBatchAsync(batch);
        }

        // First deletion to offset 4
        log.DeleteRecordsToOffset(4);
        Assert.Equal(4, log.LogStartOffset);

        // Act - Try to delete to offset 2 (lower than current LogStartOffset)
        var newLogStart = log.DeleteRecordsToOffset(2);

        // Assert - Should remain at 4, can't go backwards
        Assert.Equal(4, newLogStart);
        Assert.Equal(4, log.LogStartOffset);

        log.Dispose();
    }

    [Fact]
    public async Task PartitionLog_DeleteRecordsToOffset_TruncatesToNextOffset()
    {
        // Arrange
        var topicPartition = new TopicPartition { Topic = "test-topic", Partition = 0 };
        var log = new PartitionLog(_testDirectory, topicPartition, FileLogSegmentFactory.Create());

        for (int i = 0; i < 2; i++)
        {
            var batch = CreateSimpleRecordBatch(baseOffset: i * 2, recordCount: 2);
            await log.AppendBatchAsync(batch);
        }

        Assert.Equal(4, log.NextOffset);

        // Act - Try to delete beyond what exists (offset 100)
        var newLogStart = log.DeleteRecordsToOffset(100);

        // Assert - Should truncate to NextOffset
        Assert.Equal(4, newLogStart);
        Assert.Equal(4, log.LogStartOffset);

        log.Dispose();
    }

    [Fact]
    public async Task LogManager_DeleteRecords_DelegatesCorrectly()
    {
        // Arrange
        var logManager = new LogManager(_testDirectory, FileLogSegmentFactory.Create());
        await logManager.CreateTopicAsync("test-topic", partitionCount: 1);

        var topicPartition = new TopicPartition { Topic = "test-topic", Partition = 0 };

        // Write batches
        for (int i = 0; i < 3; i++)
        {
            var batch = CreateSimpleRecordBatch(baseOffset: i * 2, recordCount: 2);
            await logManager.AppendBatchAsync(topicPartition, batch);
        }

        // Act
        var newLogStart = logManager.DeleteRecords(topicPartition, 4);

        // Assert
        Assert.NotNull(newLogStart);
        Assert.Equal(4, newLogStart.Value);

        logManager.Dispose();
    }

    [Fact]
    public void LogManager_DeleteRecords_ReturnsNullForNonExistentPartition()
    {
        // Arrange
        var logManager = new LogManager(_testDirectory, FileLogSegmentFactory.Create());
        var topicPartition = new TopicPartition { Topic = "non-existent", Partition = 0 };

        // Act
        var result = logManager.DeleteRecords(topicPartition, 0);

        // Assert
        Assert.Null(result);

        logManager.Dispose();
    }

    [Fact]
    public async Task PartitionLog_DeleteRecords_CanStillReadFromValidOffsets()
    {
        // Arrange
        var topicPartition = new TopicPartition { Topic = "test-topic", Partition = 0 };
        var log = new PartitionLog(_testDirectory, topicPartition, FileLogSegmentFactory.Create());

        // Write 10 batches (20 records total)
        for (int i = 0; i < 10; i++)
        {
            var batch = CreateSimpleRecordBatch(baseOffset: i * 2, recordCount: 2);
            await log.AppendBatchAsync(batch);
        }

        // Delete first 4 offsets
        log.DeleteRecordsToOffset(4);

        // Act - Read from valid offset (>= LogStartOffset)
        var batches = await log.ReadBatchesAsync(4, maxBytes: 1024 * 1024);

        // Assert - Should still be able to read remaining records
        Assert.True(batches.Count > 0);

        log.Dispose();
    }

    private static byte[] CreateSimpleRecordBatch(long baseOffset, int recordCount)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        // Record batch header (61 bytes minimum)
        // BaseOffset (8 bytes)
        writer.Write(System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(baseOffset));

        // BatchLength placeholder - will be filled later (4 bytes)
        var batchLengthPosition = ms.Position;
        writer.Write(0); // placeholder

        // PartitionLeaderEpoch (4 bytes)
        writer.Write(System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(0));

        // Magic (1 byte) - version 2
        writer.Write((byte)2);

        // CRC placeholder (4 bytes) - will be filled later
        var crcPosition = ms.Position;
        writer.Write(0); // placeholder

        // Attributes (2 bytes)
        writer.Write((short)0);

        // LastOffsetDelta (4 bytes)
        writer.Write(System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(recordCount - 1));

        // BaseTimestamp (8 bytes)
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        writer.Write(System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(timestamp));

        // MaxTimestamp (8 bytes)
        writer.Write(System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(timestamp));

        // ProducerId (8 bytes)
        writer.Write(System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(-1L));

        // ProducerEpoch (2 bytes)
        writer.Write(System.Buffers.Binary.BinaryPrimitives.ReverseEndianness((short)-1));

        // BaseSequence (4 bytes)
        writer.Write(System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(-1));

        // RecordCount (4 bytes)
        writer.Write(System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(recordCount));

        // Write simple records
        for (int i = 0; i < recordCount; i++)
        {
            var value = System.Text.Encoding.UTF8.GetBytes($"test-message-{i}");

            // Record: length (varint), attributes, timestampDelta, offsetDelta, keyLength, key, valueLength, value, headersCount
            var recordBytes = new List<byte>();

            // Attributes (1 byte)
            recordBytes.Add(0);

            // TimestampDelta (varint)
            WriteVarInt(recordBytes, 0);

            // OffsetDelta (varint)
            WriteVarInt(recordBytes, i);

            // KeyLength (varint) - -1 for null
            WriteVarInt(recordBytes, -1);

            // ValueLength (varint)
            WriteVarInt(recordBytes, value.Length);

            // Value
            recordBytes.AddRange(value);

            // HeadersCount (varint)
            WriteVarInt(recordBytes, 0);

            // Write record length then record
            WriteVarInt(ms, recordBytes.Count);
            ms.Write(recordBytes.ToArray(), 0, recordBytes.Count);
        }

        // Calculate and write batch length
        var batchLength = (int)(ms.Length - batchLengthPosition - 4);
        ms.Position = batchLengthPosition;
        writer.Write(System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(batchLength));

        // Write dummy CRC (storage doesn't validate CRC for unit tests)
        ms.Position = crcPosition;
        writer.Write(System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(0x12345678));

        return ms.ToArray();
    }

    private static void WriteVarInt(List<byte> bytes, int value)
    {
        // ZigZag encode
        var encoded = (uint)((value << 1) ^ (value >> 31));
        while (encoded >= 0x80)
        {
            bytes.Add((byte)(encoded | 0x80));
            encoded >>= 7;
        }
        bytes.Add((byte)encoded);
    }

    private static void WriteVarInt(Stream stream, int value)
    {
        var bytes = new List<byte>();
        WriteVarInt(bytes, value);
        stream.Write(bytes.ToArray(), 0, bytes.Count);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDirectory))
        {
            try
            {
                Directory.Delete(_testDirectory, recursive: true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }
}
