using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage;
using Kuestenlogik.Surgewave.Storage.Engine;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// Tests for StorageEngineSegmentAdapter - ensures the adapter correctly bridges
/// ISurgewaveStorageEngine to ILogSegment for backward compatibility.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class StorageEngineAdapterTests : IDisposable
{
    private readonly string _tempDir;

    public StorageEngineAdapterTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-adapter-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch { }
        GC.SuppressFinalize(this);
    }

    [Fact]
    public async Task Adapter_AppendsAndReads_MemoryEngine()
    {
        // Arrange
        var engine = new MemoryStorageEngine(baseOffset: 0);
        using var adapter = new StorageEngineSegmentAdapter(engine);

        var batch = CreateTestBatch(baseOffset: 0, recordCount: 5);

        // Act
        var (baseOffset, recordCount) = await adapter.AppendBatchAsync(batch);

        // Assert
        Assert.Equal(0, baseOffset);
        Assert.Equal(5, recordCount);
        Assert.Equal(5, adapter.CurrentOffset);
    }

    [Fact]
    public async Task Adapter_ReadsBackAppendedData()
    {
        // Arrange
        var engine = new MemoryStorageEngine(baseOffset: 0);
        using var adapter = new StorageEngineSegmentAdapter(engine);

        var batch1 = CreateTestBatch(baseOffset: 0, recordCount: 3);
        var batch2 = CreateTestBatch(baseOffset: 3, recordCount: 2);

        await adapter.AppendBatchAsync(batch1);
        await adapter.AppendBatchAsync(batch2);

        // Act
        var batches = await adapter.ReadBatchesAsync(0, maxBytes: 1024 * 1024);

        // Assert
        Assert.Equal(2, batches.Count);
    }

    [Fact]
    public async Task Adapter_ReadBatchesContiguous_ReturnsValidData()
    {
        // Arrange
        var engine = new MemoryStorageEngine(baseOffset: 0);
        using var adapter = new StorageEngineSegmentAdapter(engine);

        var batch = CreateTestBatch(baseOffset: 0, recordCount: 5);
        await adapter.AppendBatchAsync(batch);

        // Act
        var (data, batchOffsets) = await adapter.ReadBatchesContiguousAsync(0, maxBytes: 1024 * 1024);

        // Assert
        Assert.True(data.Length > 0);
        Assert.Single(batchOffsets);
        Assert.Equal(0, batchOffsets[0]);
    }

    [Fact]
    public async Task AdapterFactory_CreatesWorkingSegments()
    {
        // Arrange
        var engineFactory = new MemoryStorageEngineFactory();
        var segmentFactory = new StorageEngineSegmentFactory(engineFactory, isPersistent: false);

        // Act
        using var segment = segmentFactory.CreateSegment(_tempDir, baseOffset: 0, createNew: true, maxSegmentSize: 1024 * 1024);

        var batch = CreateTestBatch(baseOffset: 0, recordCount: 3);
        var (baseOffset, recordCount) = await segment.AppendBatchAsync(batch);

        // Assert
        Assert.Equal(0, baseOffset);
        Assert.Equal(3, recordCount);
        Assert.Equal(3, segment.CurrentOffset);
    }

    [Fact]
    public async Task Adapter_WorksWithPartitionLog()
    {
        // Arrange
        var engineFactory = new MemoryStorageEngineFactory();
        var segmentFactory = new StorageEngineSegmentFactory(engineFactory, isPersistent: false);
        var topicPartition = new Kuestenlogik.Surgewave.Core.Models.TopicPartition { Topic = "test-topic", Partition = 0 };

        using var partitionLog = new PartitionLog(_tempDir, topicPartition, segmentFactory);

        // Act
        var batch = CreateTestBatch(baseOffset: 0, recordCount: 5);
        var appendedOffset = await partitionLog.AppendBatchAsync(batch);

        // Assert
        Assert.Equal(0, appendedOffset);
        Assert.Equal(5, partitionLog.NextOffset);
        Assert.Equal(5, partitionLog.HighWatermark);
    }

    [Fact]
    public async Task Adapter_Properties_ReflectEngineState()
    {
        // Arrange
        var engine = new MemoryStorageEngine(baseOffset: 100, maxSize: 1024 * 1024);
        using var adapter = new StorageEngineSegmentAdapter(engine);

        // Assert initial state
        Assert.Equal(100, adapter.BaseOffset);
        Assert.Equal(100, adapter.CurrentOffset);
        Assert.Equal(0, adapter.Size);
        Assert.False(adapter.IsFull);

        // Act
        var batch = CreateTestBatch(baseOffset: 100, recordCount: 10);
        await adapter.AppendBatchAsync(batch);

        // Assert after append
        Assert.Equal(110, adapter.CurrentOffset);
        Assert.True(adapter.Size > 0);
    }

    [Fact]
    public void Adapter_GetFirstMessageOffset_ReturnsCorrectValue()
    {
        // Arrange
        var engine = new MemoryStorageEngine(baseOffset: 50);
        using var adapter = new StorageEngineSegmentAdapter(engine);

        // Initially null (no data)
        Assert.Null(adapter.GetFirstMessageOffset());
    }

    // ==================== FileStorageEngine Tests ====================

    [Fact]
    public async Task FileEngine_AppendsAndReads_FileBased()
    {
        // Arrange
        var fileDir = Path.Combine(_tempDir, "file-test");
        Directory.CreateDirectory(fileDir);

        using var engine = new FileStorageEngine(fileDir, baseOffset: 0, createNew: true);
        var batch = CreateTestBatch(baseOffset: 0, recordCount: 5);

        // Act
        var (baseOffset, recordCount) = await engine.AppendAsync(batch.AsSpan());
        await engine.FlushAsync();

        // Assert
        Assert.Equal(0, baseOffset);
        Assert.Equal(5, recordCount);
        Assert.Equal(5, engine.CurrentOffset);
        Assert.True(engine.Size > 0);

        // Verify file was created
        var logFile = Path.Combine(fileDir, "00000000000000000000.log");
        Assert.True(File.Exists(logFile));
    }

    [Fact]
    public async Task FileEngine_ReadsBackData()
    {
        // Arrange
        var fileDir = Path.Combine(_tempDir, "file-read-test");
        Directory.CreateDirectory(fileDir);

        using var engine = new FileStorageEngine(fileDir, baseOffset: 0, createNew: true);

        var batch1 = CreateTestBatch(baseOffset: 0, recordCount: 3);
        var batch2 = CreateTestBatch(baseOffset: 3, recordCount: 2);

        await engine.AppendAsync(batch1.AsSpan());
        await engine.AppendAsync(batch2.AsSpan());
        await engine.FlushAsync();

        // Act
        using var lease = await engine.ReadAsync(0, maxBytes: 1024 * 1024);

        // Assert
        Assert.False(lease.IsEmpty);
        Assert.Equal(2, lease.BatchCount);
    }

    [Fact]
    public async Task FileEngine_WorksWithPartitionLog()
    {
        // Arrange
        var fileDir = Path.Combine(_tempDir, "file-partition-test");
        var engineFactory = new FileStorageEngineFactory(useMmap: false); // Disable mmap for fresh files
        var segmentFactory = new StorageEngineSegmentFactory(engineFactory, isPersistent: true);
        var topicPartition = new Kuestenlogik.Surgewave.Core.Models.TopicPartition { Topic = "file-test-topic", Partition = 0 };

        using var partitionLog = new PartitionLog(fileDir, topicPartition, segmentFactory);

        // Act
        var batch1 = CreateTestBatch(baseOffset: 0, recordCount: 5);
        var batch2 = CreateTestBatch(baseOffset: 5, recordCount: 3);

        var offset1 = await partitionLog.AppendBatchAsync(batch1);
        var offset2 = await partitionLog.AppendBatchAsync(batch2);

        // Assert
        Assert.Equal(0, offset1);
        Assert.Equal(5, offset2);
        Assert.Equal(8, partitionLog.NextOffset);
        Assert.Equal(8, partitionLog.HighWatermark);
    }

    [Fact]
    public async Task FileEngine_PersistsAndRecovers()
    {
        // Arrange
        var fileDir = Path.Combine(_tempDir, "file-recovery-test");
        var engineFactory = new FileStorageEngineFactory(useMmap: false);
        var segmentFactory = new StorageEngineSegmentFactory(engineFactory, isPersistent: true);
        var topicPartition = new Kuestenlogik.Surgewave.Core.Models.TopicPartition { Topic = "recovery-topic", Partition = 0 };

        // Write data
        {
            using var partitionLog = new PartitionLog(fileDir, topicPartition, segmentFactory);
            var batch = CreateTestBatch(baseOffset: 0, recordCount: 10);
            await partitionLog.AppendBatchAsync(batch);
        }

        // Recover and verify
        {
            using var partitionLog = new PartitionLog(fileDir, topicPartition, segmentFactory);

            // Should recover the written data
            Assert.Equal(10, partitionLog.NextOffset);

            // Should be able to read it back
            var batches = await partitionLog.ReadBatchesAsync(0, maxBytes: 1024 * 1024);
            Assert.Single(batches);
        }
    }

    [Fact]
    public async Task FileEngine_MultipleSegments()
    {
        // Arrange - use small segment size to force rollover
        var fileDir = Path.Combine(_tempDir, "file-multi-segment-test");
        var engineFactory = new FileStorageEngineFactory(useMmap: false, defaultMaxSize: 1024); // 1KB segments
        var segmentFactory = new StorageEngineSegmentFactory(engineFactory, isPersistent: true);
        var topicPartition = new Kuestenlogik.Surgewave.Core.Models.TopicPartition { Topic = "multi-seg-topic", Partition = 0 };

        using var partitionLog = new PartitionLog(fileDir, topicPartition, segmentFactory, maxSegmentBytes: 1024);

        // Act - write enough data to create multiple segments
        long offset = 0;
        for (int i = 0; i < 20; i++)
        {
            var batch = CreateTestBatch(baseOffset: offset, recordCount: 1);
            await partitionLog.AppendBatchAsync(batch);
            offset++;
        }

        // Assert
        Assert.Equal(20, partitionLog.NextOffset);
        Assert.True(partitionLog.Segments.Count > 1, $"Expected multiple segments, got {partitionLog.Segments.Count}");
    }

    // ==================== Mmap Tests ====================

    [Fact]
    public async Task FileEngine_WithMmap_ReadsAfterReopen()
    {
        // Arrange - write data and close
        var fileDir = Path.Combine(_tempDir, "file-mmap-test");
        Directory.CreateDirectory(fileDir);

        // Write data
        {
            using var engine = new FileStorageEngine(fileDir, baseOffset: 0, createNew: true, useMmap: false);
            for (int i = 0; i < 10; i++)
            {
                var batch = CreateTestBatch(baseOffset: i * 5, recordCount: 5);
                await engine.AppendAsync(batch.AsSpan());
            }
            await engine.FlushAsync();
        }

        // Reopen with mmap enabled and verify reads work
        {
            using var engine = new FileStorageEngine(fileDir, baseOffset: 0, createNew: false, useMmap: true);

            // Assert engine recovered state
            Assert.Equal(50, engine.CurrentOffset);

            // Read should use mmap
            using var lease = await engine.ReadAsync(0, maxBytes: 1024 * 1024);
            Assert.False(lease.IsEmpty);
            Assert.Equal(10, lease.BatchCount);

            // Verify batch content
            using var batch0 = lease.GetBatch(0);
            Assert.True(batch0.Length > 0);
        }
    }

    // ==================== StorageBackend Factory Tests ====================

    [Theory]
    [InlineData(StorageBackend.ZeroCopyMemory)]
    [InlineData(StorageBackend.ZeroCopyWal)]
    public async Task LogSegmentFactories_CreateWorkingFactories(StorageBackend backend)
    {
        // Arrange
        var factory = CreateFactory(backend);
        var topicPartition = new Kuestenlogik.Surgewave.Core.Models.TopicPartition { Topic = $"factory-test-{backend}", Partition = 0 };

        using var partitionLog = new PartitionLog(_tempDir, topicPartition, factory);

        // Act
        var batch = CreateTestBatch(baseOffset: 0, recordCount: 5);
        var offset = await partitionLog.AppendBatchAsync(batch);

        // Assert
        Assert.Equal(0, offset);
        Assert.Equal(5, partitionLog.NextOffset);
    }

    [Fact]
    public async Task LogManager_WithZeroCopyFile_WorksCorrectly()
    {
        // Arrange
        var factory = FileLogSegmentFactory.Create(useMmap: false);
        using var logManager = new LogManager(_tempDir, factory);

        await logManager.CreateTopicAsync("zero-copy-test", partitionCount: 1);
        var tp = new Kuestenlogik.Surgewave.Core.Models.TopicPartition { Topic = "zero-copy-test", Partition = 0 };

        // Act
        var batch = CreateTestBatch(baseOffset: 0, recordCount: 10);
        var offset = await logManager.AppendBatchAsync(tp, batch);

        // Assert
        Assert.Equal(0, offset);

        var log = logManager.GetLog(tp);
        Assert.NotNull(log);
        Assert.Equal(10, log.NextOffset);
    }

    [Fact]
    public async Task LogManager_WithZeroCopyMemory_WorksCorrectly()
    {
        // Arrange
        var factory = ZeroCopyMemoryLogSegmentFactory.Create();
        using var logManager = new LogManager(_tempDir, factory);

        await logManager.CreateTopicAsync("zero-copy-mem-test", partitionCount: 2);
        var tp0 = new Kuestenlogik.Surgewave.Core.Models.TopicPartition { Topic = "zero-copy-mem-test", Partition = 0 };
        var tp1 = new Kuestenlogik.Surgewave.Core.Models.TopicPartition { Topic = "zero-copy-mem-test", Partition = 1 };

        // Act
        var batch1 = CreateTestBatch(baseOffset: 0, recordCount: 5);
        var batch2 = CreateTestBatch(baseOffset: 0, recordCount: 3);

        await logManager.AppendBatchAsync(tp0, batch1);
        await logManager.AppendBatchAsync(tp1, batch2);

        // Assert
        var log0 = logManager.GetLog(tp0);
        var log1 = logManager.GetLog(tp1);

        Assert.NotNull(log0);
        Assert.NotNull(log1);
        Assert.Equal(5, log0.NextOffset);
        Assert.Equal(3, log1.NextOffset);
    }

    /// <summary>
    /// Create a minimal valid Kafka RecordBatch for testing.
    /// </summary>
    private static byte[] CreateTestBatch(long baseOffset, int recordCount)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var valueData = new byte[100];
        System.Security.Cryptography.RandomNumberGenerator.Fill(valueData);

        // Base Offset (8 bytes, big-endian)
        WriteBigEndian(writer, baseOffset);

        // Placeholder for batch length
        var batchLengthPos = stream.Position;
        WriteBigEndian(writer, 0);

        var batchDataStart = stream.Position;

        // Partition Leader Epoch
        WriteBigEndian(writer, 0);
        // Magic
        writer.Write((byte)2);
        // CRC placeholder
        WriteBigEndian(writer, 0u);
        // Attributes
        WriteBigEndian(writer, (short)0);
        // Last Offset Delta
        WriteBigEndian(writer, recordCount - 1);
        // Base Timestamp
        WriteBigEndian(writer, timestamp);
        // Max Timestamp
        WriteBigEndian(writer, timestamp);
        // Producer ID
        WriteBigEndian(writer, -1L);
        // Producer Epoch
        WriteBigEndian(writer, (short)-1);
        // Base Sequence
        WriteBigEndian(writer, -1);
        // Record Count
        WriteBigEndian(writer, recordCount);

        // Write records
        for (int i = 0; i < recordCount; i++)
        {
            WriteRecord(writer, valueData, i);
        }

        // Update batch length
        var batchLength = (int)(stream.Position - batchDataStart);
        var endPos = stream.Position;
        stream.Position = batchLengthPos;
        WriteBigEndian(writer, batchLength);
        stream.Position = endPos;

        return stream.ToArray();
    }

    private static void WriteRecord(BinaryWriter writer, byte[] value, int offsetDelta)
    {
        using var recordStream = new MemoryStream();
        using var recordWriter = new BinaryWriter(recordStream);

        recordWriter.Write((byte)0);           // Attributes
        WriteVarInt(recordWriter, 0);          // Timestamp delta
        WriteVarInt(recordWriter, offsetDelta); // Offset delta
        WriteVarInt(recordWriter, -1);         // Key length (null)
        WriteVarInt(recordWriter, value.Length);
        recordWriter.Write(value);
        WriteVarInt(recordWriter, 0);          // Headers count

        var recordBytes = recordStream.ToArray();
        WriteVarInt(writer, recordBytes.Length);
        writer.Write(recordBytes);
    }

    private static void WriteBigEndian(BinaryWriter writer, short value)
    {
        writer.Write((byte)(value >> 8));
        writer.Write((byte)value);
    }

    private static void WriteBigEndian(BinaryWriter writer, int value)
    {
        writer.Write((byte)(value >> 24));
        writer.Write((byte)(value >> 16));
        writer.Write((byte)(value >> 8));
        writer.Write((byte)value);
    }

    private static void WriteBigEndian(BinaryWriter writer, long value)
    {
        writer.Write((byte)(value >> 56));
        writer.Write((byte)(value >> 48));
        writer.Write((byte)(value >> 40));
        writer.Write((byte)(value >> 32));
        writer.Write((byte)(value >> 24));
        writer.Write((byte)(value >> 16));
        writer.Write((byte)(value >> 8));
        writer.Write((byte)value);
    }

    private static void WriteBigEndian(BinaryWriter writer, uint value)
    {
        writer.Write((byte)(value >> 24));
        writer.Write((byte)(value >> 16));
        writer.Write((byte)(value >> 8));
        writer.Write((byte)value);
    }

    private static void WriteVarInt(BinaryWriter writer, int value)
    {
        var v = (uint)((value << 1) ^ (value >> 31));
        while ((v & ~0x7F) != 0)
        {
            writer.Write((byte)((v & 0x7F) | 0x80));
            v >>= 7;
        }
        writer.Write((byte)v);
    }

    private static ILogSegmentFactory CreateFactory(StorageBackend backend)
    {
        return backend switch
        {
            StorageBackend.File => FileLogSegmentFactory.Create(useMmap: false),
            StorageBackend.Memory => new MemoryLogSegmentFactory(),
            StorageBackend.ZeroCopyWal => FileLogSegmentFactory.Create(useMmap: true),
            StorageBackend.ZeroCopyMemory => ZeroCopyMemoryLogSegmentFactory.Create(),
            _ => throw new ArgumentOutOfRangeException(nameof(backend), backend, null)
        };
    }
}
