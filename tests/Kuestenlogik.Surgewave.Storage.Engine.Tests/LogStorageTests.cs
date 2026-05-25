using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage;
using Kuestenlogik.Surgewave.Storage.Engine;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// Tests for storage engine and log storage operations.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class LogStorageTests : IDisposable
{
    private readonly string _testDirectory;

    public LogStorageTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"kl-streaming-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDirectory);
    }

    [Fact]
    public async Task FileStorageEngine_CanAppendAndRead()
    {
        // Arrange
        using var engine = new FileStorageEngine(_testDirectory, 0, createNew: true);
        using var segment = new StorageEngineSegmentAdapter(engine);
        var recordBatch = CreateSimpleRecordBatch(baseOffset: 0, recordCount: 2);

        // Act
        var (baseOffset, recordCount) = await segment.AppendBatchAsync(recordBatch);
        var batches = await segment.ReadBatchesAsync(0, maxBytes: 1024 * 1024);

        // Assert
        Assert.Equal(0, baseOffset);
        Assert.Equal(2, recordCount);
        Assert.Single(batches);
        Assert.True(batches[0].Length > 0);
    }

    [Fact]
    public async Task PartitionLog_HandlesMultipleBatches()
    {
        // Arrange
        var topicPartition = new TopicPartition { Topic = "test-topic", Partition = 0 };
        var log = new PartitionLog(_testDirectory, topicPartition, FileLogSegmentFactory.Create());

        // Act - Write multiple batches
        for (int i = 0; i < 10; i++)
        {
            var batch = CreateSimpleRecordBatch(baseOffset: i * 10, recordCount: 10);
            await log.AppendBatchAsync(batch);
        }

        // Assert
        var batches = await log.ReadBatchesAsync(0, maxBytes: 1024 * 1024);
        Assert.True(batches.Count > 0);

        log.Dispose();
    }

    [Fact]
    public async Task LogManager_CanCreateAndManageTopics()
    {
        // Arrange
        var logManager = new LogManager(_testDirectory, FileLogSegmentFactory.Create());

        // Act
        var metadata = await logManager.CreateTopicAsync("test-topic", partitionCount: 3);

        // Assert
        Assert.Equal("test-topic", metadata.Name);
        Assert.Equal(3, metadata.PartitionCount);

        var retrievedMetadata = logManager.GetTopicMetadata("test-topic");
        Assert.NotNull(retrievedMetadata);
        Assert.Equal("test-topic", retrievedMetadata.Name);

        logManager.Dispose();
    }

    [Fact]
    public async Task LogManager_CanAppendAndReadBatchesFromPartitions()
    {
        // Arrange
        var logManager = new LogManager(_testDirectory, FileLogSegmentFactory.Create());
        await logManager.CreateTopicAsync("test-topic", partitionCount: 1);

        var topicPartition = new TopicPartition { Topic = "test-topic", Partition = 0 };
        var recordBatch = CreateSimpleRecordBatch(baseOffset: 0, recordCount: 1);

        // Act
        var offset = await logManager.AppendBatchAsync(topicPartition, recordBatch);
        var batches = await logManager.ReadBatchesAsync(topicPartition, 0, maxBytes: 1024 * 1024);

        // Assert
        Assert.Equal(0, offset);
        Assert.Single(batches);

        logManager.Dispose();
    }

    [Fact]
    public async Task PartitionLog_MaintainsOffsetOrdering()
    {
        // Arrange
        var topicPartition = new TopicPartition { Topic = "test-topic", Partition = 0 };
        var log = new PartitionLog(_testDirectory, topicPartition, FileLogSegmentFactory.Create());

        // Act - Append batches with known offsets
        for (int i = 0; i < 5; i++)
        {
            var batch = CreateSimpleRecordBatch(baseOffset: i * 2, recordCount: 2);
            await log.AppendBatchAsync(batch);
        }

        // Assert - Read from different offsets
        var allBatches = await log.ReadBatchesAsync(0, maxBytes: 1024 * 1024);
        Assert.True(allBatches.Count >= 5);

        // Read from middle offset
        var laterBatches = await log.ReadBatchesAsync(4, maxBytes: 1024 * 1024);
        Assert.True(laterBatches.Count <= allBatches.Count);

        log.Dispose();
    }

    /// <summary>
    /// Creates a minimal valid Kafka record batch for testing.
    /// This is a simplified batch format for unit testing storage operations.
    /// </summary>
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
