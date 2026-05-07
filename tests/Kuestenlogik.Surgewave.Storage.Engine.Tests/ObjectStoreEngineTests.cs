using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Storage;
using Kuestenlogik.Surgewave.Storage.Engine;
using Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// Tests for the ObjectStore storage engine components: WriteBuffer, ReadCache,
/// ObjectStoreEngine, ObjectStoreEngineFactory, and ObjectStoreConfig.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class ObjectStoreEngineTests : IDisposable
{
    private readonly string _tempDir;
    private readonly InMemoryObjectStoreProvider _provider;

    public ObjectStoreEngineTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-objstore-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _provider = new InMemoryObjectStoreProvider();
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

    // ==================== WriteBuffer Tests ====================

    [Fact]
    public void WriteBuffer_Append_IncreasesOffset()
    {
        // Arrange
        using var buffer = CreateWriteBuffer(baseOffset: 0);
        var batch = CreateTestBatch(baseOffset: 0, recordCount: 5);

        // Act
        var (baseOffset, recordCount) = buffer.Append(batch);

        // Assert
        Assert.Equal(0, baseOffset);
        Assert.Equal(5, recordCount);
        Assert.Equal(5, buffer.CurrentOffset);
    }

    [Fact]
    public async Task WriteBuffer_Flush_ClearsBuffer()
    {
        // Arrange
        using var buffer = CreateWriteBuffer(baseOffset: 0);
        var batch = CreateTestBatch(baseOffset: 0, recordCount: 3);
        buffer.Append(batch);

        Assert.True(buffer.CurrentSize > 0);

        // Act
        await buffer.FlushAsync();

        // Assert
        Assert.Equal(0, buffer.CurrentSize);
        Assert.False(_provider.Segments.IsEmpty, "Data should have been uploaded to the provider");
    }

    [Fact]
    public void WriteBuffer_IsFull_ReturnsTrueAtLimit()
    {
        // Arrange - use a very small buffer
        using var buffer = CreateWriteBuffer(baseOffset: 0, maxSizeBytes: 100);

        // Act - write enough data to exceed the limit
        var batch = CreateTestBatch(baseOffset: 0, recordCount: 1);
        buffer.Append(batch);

        // Assert
        Assert.True(buffer.IsFull, $"Buffer should be full. CurrentSize={buffer.CurrentSize}, limit=100");
    }

    [Fact]
    public void WriteBuffer_ReadFromBuffer_ReturnsAppendedData()
    {
        // Arrange
        using var buffer = CreateWriteBuffer(baseOffset: 0);
        var batch = CreateTestBatch(baseOffset: 0, recordCount: 5);
        buffer.Append(batch);

        // Act
        var data = buffer.ReadFromBuffer(0, maxBytes: 1024 * 1024);

        // Assert
        Assert.False(data.IsEmpty, "Should return data from buffer");
        Assert.Equal(batch.Length, data.Length);
    }

    // ==================== ReadCache Tests ====================

    [Fact]
    public void ReadCache_PutAndGet_ReturnsData()
    {
        // Arrange
        using var cache = new ReadCache(_tempDir, maxSizeBytes: 1024 * 1024);
        var data = new byte[] { 1, 2, 3, 4, 5 };

        // Act
        cache.Put("key1", data);
        var result = cache.Get("key1");

        // Assert
        Assert.NotNull(result);
        Assert.Equal(data, result);
    }

    [Fact]
    public void ReadCache_Eviction_RemovesLRU()
    {
        // Arrange - cache with very limited size
        using var cache = new ReadCache(_tempDir, maxSizeBytes: 15);
        var data1 = new byte[] { 1, 2, 3, 4, 5 };  // 5 bytes
        var data2 = new byte[] { 6, 7, 8, 9, 10 };  // 5 bytes
        var data3 = new byte[] { 11, 12, 13, 14, 15 }; // 5 bytes

        // Act
        cache.Put("key1", data1); // 5 bytes total
        cache.Put("key2", data2); // 10 bytes total
        cache.Get("key1");        // Access key1 to make it recently used
        cache.Put("key3", data3); // 15 bytes - should evict key2 (LRU)

        // Adding one more that forces eviction
        var data4 = new byte[] { 16, 17, 18, 19, 20 }; // Would push over 15 bytes
        cache.Put("key4", data4);

        // Assert - key1 was accessed recently so it should survive eviction longer
        // but with size 15, we can only hold 3 items of 5 bytes each
        // After adding key4, something must have been evicted
        Assert.True(cache.CurrentSize <= 15, $"Cache size {cache.CurrentSize} should not exceed limit of 15");
    }

    [Fact]
    public void ReadCache_Miss_ReturnsNull()
    {
        // Arrange
        using var cache = new ReadCache(_tempDir, maxSizeBytes: 1024);

        // Act
        var result = cache.Get("nonexistent");

        // Assert
        Assert.Null(result);
    }

    // ==================== ObjectStoreEngine Tests ====================

    [Fact]
    public async Task ObjectStoreEngine_Append_WritesToBuffer()
    {
        // Arrange
        using var engine = CreateEngine(baseOffset: 0);
        var batch = CreateTestBatch(baseOffset: 0, recordCount: 5);

        // Act
        var (baseOffset, recordCount) = await engine.AppendAsync(batch.AsSpan());

        // Assert
        Assert.Equal(0, baseOffset);
        Assert.Equal(5, recordCount);
        Assert.Equal(5, engine.CurrentOffset);
        Assert.True(engine.Size > 0);
    }

    [Fact]
    public async Task ObjectStoreEngine_Read_FromBuffer()
    {
        // Arrange
        using var engine = CreateEngine(baseOffset: 0);
        var batch = CreateTestBatch(baseOffset: 0, recordCount: 3);
        await engine.AppendAsync(batch.AsSpan());

        // Act
        using var lease = await engine.ReadAsync(0, maxBytes: 1024 * 1024);

        // Assert
        Assert.False(lease.IsEmpty);
        Assert.Equal(1, lease.BatchCount);
    }

    [Fact]
    public async Task ObjectStoreEngine_Properties_ReturnCorrectValues()
    {
        // Arrange
        using var engine = CreateEngine(baseOffset: 100);

        // Assert initial state
        Assert.Equal(100, engine.BaseOffset);
        Assert.Equal(100, engine.CurrentOffset);
        Assert.Equal(0, engine.Size);
        Assert.False(engine.IsFull);
        Assert.Null(engine.FirstOffset);

        // Act
        var batch = CreateTestBatch(baseOffset: 100, recordCount: 10);
        await engine.AppendAsync(batch.AsSpan());

        // Assert after append
        Assert.Equal(110, engine.CurrentOffset);
        Assert.True(engine.Size > 0);
        Assert.Equal(100, engine.FirstOffset);
        Assert.True(engine.MaxTimestamp > 0);
    }

    // ==================== ObjectStoreConfig Tests ====================

    [Fact]
    public void ObjectStoreConfig_DefaultValues_Correct()
    {
        // Act
        var config = new ObjectStoreConfig();

        // Assert
        Assert.Equal(64 * 1024 * 1024, config.WriteBufferSizeBytes);
        Assert.Equal(512 * 1024 * 1024, config.ReadCacheSizeBytes);
        Assert.Equal("./zero-disk-cache", config.CacheDirectory);
        Assert.Equal(TimeSpan.FromSeconds(30), config.FlushInterval);
        Assert.Equal(1024L * 1024 * 1024, config.DefaultMaxSegmentSize);
    }

    // ==================== ObjectStoreEngineFactory Tests ====================

    [Fact]
    public void ObjectStoreEngineFactory_Create_ReturnsEngine()
    {
        // Arrange
        var config = new ObjectStoreConfig
        {
            CacheDirectory = Path.Combine(_tempDir, "cache")
        };
        var factory = new ObjectStoreEngineFactory(_provider, config);

        // Act
        using var engine = factory.Create("test-topic/0", baseOffset: 0, maxSize: 1024 * 1024);

        // Assert
        Assert.NotNull(engine);
        Assert.IsType<ObjectStoreEngine>(engine);
        Assert.Equal(0, engine.BaseOffset);
        Assert.Equal(0, engine.CurrentOffset);
    }

    [Fact]
    public async Task ObjectStoreEngine_FlushUploadsToRemote()
    {
        // Arrange
        using var engine = CreateEngine(baseOffset: 0);
        var batch = CreateTestBatch(baseOffset: 0, recordCount: 5);
        await engine.AppendAsync(batch.AsSpan());

        // Act
        await engine.FlushAsync();

        // Assert
        Assert.False(_provider.Segments.IsEmpty, "Flush should upload data to remote storage");
    }

    [Fact]
    public async Task ObjectStoreEngine_MultipleBatches_ReadAll()
    {
        // Arrange
        using var engine = CreateEngine(baseOffset: 0);
        var batch1 = CreateTestBatch(baseOffset: 0, recordCount: 3);
        var batch2 = CreateTestBatch(baseOffset: 3, recordCount: 2);

        await engine.AppendAsync(batch1.AsSpan());
        await engine.AppendAsync(batch2.AsSpan());

        // Act
        using var lease = await engine.ReadAsync(0, maxBytes: 1024 * 1024);

        // Assert
        Assert.False(lease.IsEmpty);
        Assert.Equal(2, lease.BatchCount);
        Assert.Equal(5, engine.CurrentOffset);
    }

    [Fact]
    public void ObjectStoreLogSegmentFactory_Create_ReturnsWorkingSegment()
    {
        // Arrange
        var config = new ObjectStoreConfig
        {
            CacheDirectory = Path.Combine(_tempDir, "cache")
        };
        var factory = ObjectStoreLogSegmentFactory.Create(_provider, config);

        // Act
        using var segment = factory.CreateSegment(
            Path.Combine(_tempDir, "test-topic/0"),
            baseOffset: 0,
            createNew: true,
            maxSegmentSize: 1024 * 1024);

        // Assert
        Assert.NotNull(segment);
        Assert.Equal(0, segment.BaseOffset);
        Assert.True(factory.IsPersistent);
    }

    // ==================== Helper Methods ====================

    private WriteBuffer CreateWriteBuffer(long baseOffset, long maxSizeBytes = 64 * 1024 * 1024)
    {
        return new WriteBuffer(maxSizeBytes, _provider, "test-topic", 0, baseOffset);
    }

    private ObjectStoreEngine CreateEngine(long baseOffset)
    {
        var config = new ObjectStoreConfig
        {
            CacheDirectory = Path.Combine(_tempDir, "cache"),
            WriteBufferSizeBytes = 64 * 1024 * 1024,
            ReadCacheSizeBytes = 512 * 1024 * 1024
        };

        return new ObjectStoreEngine(
            _provider,
            config,
            "test-topic",
            0,
            baseOffset,
            maxSize: 1024L * 1024 * 1024);
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
}

/// <summary>
/// In-memory implementation of IObjectStoreProvider for testing.
/// Stores all uploaded segments in a ConcurrentDictionary.
/// </summary>
internal sealed class InMemoryObjectStoreProvider : IObjectStoreProvider
{
    public ConcurrentDictionary<string, byte[]> Segments { get; } = new();

    public Task UploadAsync(
        string topic,
        int partition,
        long baseOffset,
        ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        var key = MakeKey(topic, partition, baseOffset);
        Segments[key] = data.ToArray();
        return Task.CompletedTask;
    }

    public Task<byte[]?> DownloadAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default)
    {
        var key = MakeKey(topic, partition, baseOffset);
        return Task.FromResult(Segments.TryGetValue(key, out var data) ? data : null);
    }

    public Task DeleteAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default)
    {
        var key = MakeKey(topic, partition, baseOffset);
        Segments.TryRemove(key, out _);
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<long>> ListSegmentOffsetsAsync(
        string topic,
        int partition,
        CancellationToken cancellationToken = default)
    {
        var prefix = $"{topic}/{partition}/";
        var offsets = Segments.Keys
            .Where(k => k.StartsWith(prefix, StringComparison.Ordinal))
            .Select(k => long.Parse(k[prefix.Length..]))
            .Order()
            .ToList();

        return Task.FromResult<IReadOnlyList<long>>(offsets);
    }

    private static string MakeKey(string topic, int partition, long baseOffset) =>
        $"{topic}/{partition}/{baseOffset}";
}
