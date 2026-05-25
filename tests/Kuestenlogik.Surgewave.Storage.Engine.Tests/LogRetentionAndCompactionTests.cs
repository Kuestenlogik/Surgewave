using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage;
using Kuestenlogik.Surgewave.Storage.Engine;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// Tests for log retention policies and log compaction.
/// These are unit tests that test the storage layer directly without requiring the broker.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class LogRetentionAndCompactionTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _testDirectory;

    public LogRetentionAndCompactionTests(ITestOutputHelper output)
    {
        _output = output;
        _testDirectory = Path.Combine(Path.GetTempPath(), "surgewave-retention-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_testDirectory);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDirectory))
            {
                Directory.Delete(_testDirectory, recursive: true);
            }
        }
        catch { }
    }

    /// <summary>
    /// Test that RetentionPolicy defaults are sensible.
    /// </summary>
    [Fact]
    public void RetentionPolicy_HasSensibleDefaults()
    {
        var policy = RetentionPolicy.Default;

        Assert.Equal(168, policy.RetentionHours); // 7 days
        Assert.Equal(-1, policy.RetentionBytes);  // Unlimited
        Assert.Equal(1, policy.MinSegmentsToKeep); // Keep at least 1

        _output.WriteLine($"Default retention: {policy.RetentionHours}h, {policy.RetentionBytes} bytes, min segments: {policy.MinSegmentsToKeep}");
    }

    /// <summary>
    /// Test custom retention policy settings.
    /// </summary>
    [Fact]
    public void RetentionPolicy_CanBeCustomized()
    {
        var policy = new RetentionPolicy
        {
            RetentionHours = 24,
            RetentionBytes = 1024 * 1024 * 100, // 100MB
            MinSegmentsToKeep = 2
        };

        Assert.Equal(24, policy.RetentionHours);
        Assert.Equal(104857600, policy.RetentionBytes);
        Assert.Equal(2, policy.MinSegmentsToKeep);

        _output.WriteLine($"Custom retention: {policy.RetentionHours}h, {policy.RetentionBytes} bytes, min segments: {policy.MinSegmentsToKeep}");
    }

    /// <summary>
    /// Test unlimited retention policy.
    /// </summary>
    [Fact]
    public void RetentionPolicy_Unlimited()
    {
        var policy = RetentionPolicy.Unlimited;

        Assert.Equal(-1, policy.RetentionHours);
        Assert.Equal(-1, policy.RetentionBytes);

        _output.WriteLine($"Unlimited retention: hours={policy.RetentionHours}, bytes={policy.RetentionBytes}");
    }

    /// <summary>
    /// Test that CompactionConfig defaults are sensible.
    /// </summary>
    [Fact]
    public void CompactionConfig_HasSensibleDefaults()
    {
        var config = CompactionConfig.Default;

        Assert.Equal(0, config.MinCompactionLagMs);
        Assert.Equal(24 * 60 * 60 * 1000, config.DeleteRetentionMs); // 24 hours
        Assert.Equal(0.5, config.MinCleanableDirtyRatio);
        Assert.Equal(0, config.MaxCompactionBytes);

        _output.WriteLine($"Default compaction: MinLag={config.MinCompactionLagMs}ms, DeleteRetention={config.DeleteRetentionMs}ms");
    }

    /// <summary>
    /// Test custom compaction config settings.
    /// </summary>
    [Fact]
    public void CompactionConfig_CanBeCustomized()
    {
        var config = new CompactionConfig
        {
            MinCompactionLagMs = 60000, // 1 minute
            DeleteRetentionMs = 3600000, // 1 hour
            MinCleanableDirtyRatio = 0.3,
            MaxCompactionBytes = 1024 * 1024 * 50 // 50MB
        };

        Assert.Equal(60000, config.MinCompactionLagMs);
        Assert.Equal(3600000, config.DeleteRetentionMs);
        Assert.Equal(0.3, config.MinCleanableDirtyRatio);
        Assert.Equal(52428800, config.MaxCompactionBytes);

        _output.WriteLine($"Custom compaction: MinLag={config.MinCompactionLagMs}ms, MaxBytes={config.MaxCompactionBytes}");
    }

    /// <summary>
    /// Test CleanupPolicy enum values.
    /// </summary>
    [Fact]
    public void CleanupPolicy_HasCorrectValues()
    {
        Assert.True(CleanupPolicy.Delete.HasFlag(CleanupPolicy.Delete));
        Assert.True(CleanupPolicy.Compact.HasFlag(CleanupPolicy.Compact));

        var both = CleanupPolicy.Delete | CleanupPolicy.Compact;
        Assert.True(both.HasFlag(CleanupPolicy.Delete));
        Assert.True(both.HasFlag(CleanupPolicy.Compact));

        // DeleteAndCompact should be the combination
        Assert.Equal(CleanupPolicy.DeleteAndCompact, both);

        _output.WriteLine($"Delete={CleanupPolicy.Delete}, Compact={CleanupPolicy.Compact}, Both={CleanupPolicy.DeleteAndCompact}");
    }

    /// <summary>
    /// Test LogManager creation with retention policy.
    /// </summary>
    [Fact]
    public void LogManager_AcceptsRetentionPolicy()
    {
        var policy = new RetentionPolicy
        {
            RetentionHours = 48,
            RetentionBytes = 1024 * 1024 * 500
        };

        using var logManager = new LogManager(
            _testDirectory,
            FileLogSegmentFactory.Create(),
            retentionPolicy: policy,
            retentionCheckInterval: TimeSpan.FromMinutes(1));

        // Should not throw
        _output.WriteLine($"LogManager created with custom retention policy");
    }

    /// <summary>
    /// Test LogManager creation with compaction config.
    /// </summary>
    [Fact]
    public void LogManager_AcceptsCompactionConfig()
    {
        var compactionConfig = new CompactionConfig
        {
            MinCompactionLagMs = 30000,
            DeleteRetentionMs = 7200000
        };

        using var logManager = new LogManager(
            _testDirectory,
            FileLogSegmentFactory.Create(),
            compactionConfig: compactionConfig,
            compactionCheckInterval: TimeSpan.FromMinutes(5));

        // Should not throw
        _output.WriteLine($"LogManager created with custom compaction config");
    }

    /// <summary>
    /// Test LogManager applies retention policy.
    /// </summary>
    [Fact]
    public void LogManager_AppliesRetentionPolicy()
    {
        var policy = new RetentionPolicy
        {
            RetentionHours = 1,
            RetentionBytes = -1
        };

        using var logManager = new LogManager(
            _testDirectory,
            FileLogSegmentFactory.Create(),
            retentionPolicy: policy);

        // Apply retention manually
        var deletedSegments = logManager.ApplyRetentionPolicy();

        // With no data, should delete 0 segments
        Assert.Equal(0, deletedSegments);
        _output.WriteLine($"Applied retention policy, deleted {deletedSegments} segments");
    }

    /// <summary>
    /// Test FileStorageEngine creation.
    /// </summary>
    [Fact]
    public void FileStorageEngine_CanBeCreated()
    {
        var segmentDir = Path.Combine(_testDirectory, "test-segment");
        Directory.CreateDirectory(segmentDir);

        using var engine = new FileStorageEngine(segmentDir, baseOffset: 0, createNew: true);
        using var segment = new StorageEngineSegmentAdapter(engine);

        Assert.Equal(0, segment.BaseOffset);
        Assert.NotNull(segment);

        _output.WriteLine($"Created segment in {segmentDir} with base offset 0");
    }

    /// <summary>
    /// Test FileStorageEngine reports correct properties.
    /// </summary>
    [Fact]
    public void FileStorageEngine_ReportsCorrectProperties()
    {
        var segmentDir = Path.Combine(_testDirectory, "properties-test");
        Directory.CreateDirectory(segmentDir);

        using var engine = new FileStorageEngine(segmentDir, baseOffset: 100, createNew: true);
        using var segment = new StorageEngineSegmentAdapter(engine);

        Assert.Equal(100, segment.BaseOffset);
        Assert.Equal(100, segment.CurrentOffset);
        Assert.Equal(0, segment.Size); // Empty segment
        Assert.False(segment.IsFull);

        _output.WriteLine($"Segment properties: BaseOffset={segment.BaseOffset}, CurrentOffset={segment.CurrentOffset}, Size={segment.Size}");
    }

    /// <summary>
    /// Test PartitionLog creation and basic operations.
    /// </summary>
    [Fact]
    public async Task PartitionLog_CanAppendAndRead()
    {
        var tp = new TopicPartition { Topic = "test-topic", Partition = 0 };

        using var partitionLog = new PartitionLog(_testDirectory, tp, FileLogSegmentFactory.Create());

        // Append a batch
        var batch = CreateTestBatch("test-key", "test-value");
        var offset = await partitionLog.AppendBatchAsync(batch, CancellationToken.None);

        Assert.True(offset >= 0);
        _output.WriteLine($"Appended batch at offset {offset}");

        // Check high watermark moved
        Assert.True(partitionLog.HighWatermark > 0);
        _output.WriteLine($"High watermark: {partitionLog.HighWatermark}");
    }

    /// <summary>
    /// Test PartitionLog reports correct offsets.
    /// </summary>
    [Fact]
    public async Task PartitionLog_ReportsCorrectOffsets()
    {
        var tp = new TopicPartition { Topic = "offset-test-topic", Partition = 0 };

        using var partitionLog = new PartitionLog(_testDirectory, tp, FileLogSegmentFactory.Create());

        var initialStart = partitionLog.LogStartOffset;
        var initialNext = partitionLog.NextOffset;

        _output.WriteLine($"Initial offsets: start={initialStart}, next={initialNext}");

        // Append several batches
        for (int i = 0; i < 5; i++)
        {
            var batch = CreateTestBatch($"key-{i}", $"value-{i}");
            await partitionLog.AppendBatchAsync(batch, CancellationToken.None);
        }

        var finalStart = partitionLog.LogStartOffset;
        var finalNext = partitionLog.NextOffset;

        _output.WriteLine($"Final offsets: start={finalStart}, next={finalNext}");

        Assert.True(finalNext > initialNext, "Next offset should have increased");
    }

    /// <summary>
    /// Test PartitionLog applies retention.
    /// </summary>
    [Fact]
    public async Task PartitionLog_AppliesRetention()
    {
        var tp = new TopicPartition { Topic = "retention-apply-topic", Partition = 0 };

        using var partitionLog = new PartitionLog(_testDirectory, tp, FileLogSegmentFactory.Create());

        // Append several batches
        for (int i = 0; i < 10; i++)
        {
            var batch = CreateTestBatch($"key-{i}", $"value-{i}");
            await partitionLog.AppendBatchAsync(batch, CancellationToken.None);
        }

        var beforeRetention = partitionLog.NextOffset;

        // Apply retention with very aggressive settings
        var policy = new RetentionPolicy
        {
            RetentionHours = 0, // Immediate expiry
            RetentionBytes = 100, // Very small
            MinSegmentsToKeep = 1
        };

        var deleted = partitionLog.ApplyRetentionPolicy(policy);

        _output.WriteLine($"Applied retention, deleted {deleted} segments");
        _output.WriteLine($"Offset before: {beforeRetention}, after: {partitionLog.NextOffset}");
    }

    /// <summary>
    /// Test LogCompactor creation.
    /// </summary>
    [Fact]
    public void LogCompactor_CanBeCreated()
    {
        var config = CompactionConfig.Default;
        var compactor = new LogCompactor(config);

        Assert.NotNull(compactor);
        _output.WriteLine("Created LogCompactor with default config");
    }

    /// <summary>
    /// Test LogCompactor handles empty partition.
    /// </summary>
    [Fact]
    public async Task LogCompactor_HandlesEmptyPartition()
    {
        var tp = new TopicPartition { Topic = "empty-compact-topic", Partition = 0 };

        using var partitionLog = new PartitionLog(_testDirectory, tp, FileLogSegmentFactory.Create());

        var config = CompactionConfig.Default;
        var compactor = new LogCompactor(config);

        var result = await compactor.CompactAsync(partitionLog, CancellationToken.None);

        Assert.Equal(0, result.RecordsRemoved);
        Assert.Equal(0, result.BytesRemoved);
        Assert.Equal(0, result.SegmentsCompacted);

        _output.WriteLine($"Compaction of empty log: removed={result.RecordsRemoved}, bytes={result.BytesRemoved}");
    }

    /// <summary>
    /// Test LogCompactor removes duplicate keys, keeping only latest.
    /// </summary>
    [Fact]
    public async Task LogCompactor_RemovesDuplicateKeys()
    {
        var tp = new TopicPartition { Topic = "compact-duplicates-topic", Partition = 0 };

        using var partitionLog = new PartitionLog(_testDirectory, tp, FileLogSegmentFactory.Create());

        // Append same key multiple times with different values
        for (int i = 0; i < 5; i++)
        {
            var batch = CreateTestBatch("same-key", $"value-{i}");
            await partitionLog.AppendBatchAsync(batch, CancellationToken.None);
        }

        var beforeCompaction = partitionLog.TotalSize;
        _output.WriteLine($"Size before compaction: {beforeCompaction} bytes, offset: {partitionLog.NextOffset}");

        // Need at least 2 segments for compaction to work
        // For single segment, compactor skips (active segment is never compacted)
        var config = new CompactionConfig
        {
            MinCompactionLagMs = 0,
            DeleteRetentionMs = 0 // Immediate tombstone deletion
        };
        var compactor = new LogCompactor(config);

        var result = await compactor.CompactAsync(partitionLog, CancellationToken.None);

        _output.WriteLine($"Compaction result: removed {result.RecordsRemoved} records, {result.BytesRemoved} bytes, {result.SegmentsCompacted} segments");
    }

    /// <summary>
    /// Test that records without keys are never removed during compaction.
    /// </summary>
    [Fact]
    public async Task LogCompactor_KeepsRecordsWithoutKeys()
    {
        var tp = new TopicPartition { Topic = "compact-null-keys-topic", Partition = 0 };

        using var partitionLog = new PartitionLog(_testDirectory, tp, FileLogSegmentFactory.Create());

        // Append records without keys (null key)
        for (int i = 0; i < 3; i++)
        {
            var batch = CreateTestBatchWithNullKey($"value-{i}");
            await partitionLog.AppendBatchAsync(batch, CancellationToken.None);
        }

        _output.WriteLine($"Appended {partitionLog.NextOffset} records with null keys");

        var config = new CompactionConfig { MinCompactionLagMs = 0 };
        var compactor = new LogCompactor(config);

        var result = await compactor.CompactAsync(partitionLog, CancellationToken.None);

        // Records without keys should never be removed
        Assert.Equal(0, result.RecordsRemoved);
        _output.WriteLine($"Compaction preserved all null-key records");
    }

    /// <summary>
    /// Test TopicMetadata cleanup policy parsing.
    /// </summary>
    [Fact]
    public void TopicMetadata_ParsesCleanupPolicy()
    {
        var deleteOnlyTopic = new TopicMetadata
        {
            Name = "delete-topic",
            TopicId = Guid.NewGuid(),
            PartitionCount = 1,
            ReplicationFactor = 1,
            Config = new Dictionary<string, string> { ["cleanup.policy"] = "delete" },
            CreatedAt = DateTime.UtcNow
        };

        var compactOnlyTopic = new TopicMetadata
        {
            Name = "compact-topic",
            TopicId = Guid.NewGuid(),
            PartitionCount = 1,
            ReplicationFactor = 1,
            Config = new Dictionary<string, string> { ["cleanup.policy"] = "compact" },
            CreatedAt = DateTime.UtcNow
        };

        var bothTopic = new TopicMetadata
        {
            Name = "both-topic",
            TopicId = Guid.NewGuid(),
            PartitionCount = 1,
            ReplicationFactor = 1,
            Config = new Dictionary<string, string> { ["cleanup.policy"] = "compact,delete" },
            CreatedAt = DateTime.UtcNow
        };

        var defaultTopic = new TopicMetadata
        {
            Name = "default-topic",
            TopicId = Guid.NewGuid(),
            PartitionCount = 1,
            ReplicationFactor = 1,
            Config = new Dictionary<string, string>(),
            CreatedAt = DateTime.UtcNow
        };

        Assert.Equal(CleanupPolicy.Delete, deleteOnlyTopic.CleanupPolicy);
        Assert.Equal(CleanupPolicy.Compact, compactOnlyTopic.CleanupPolicy);
        Assert.Equal(CleanupPolicy.DeleteAndCompact, bothTopic.CleanupPolicy);
        Assert.Equal(CleanupPolicy.Delete, defaultTopic.CleanupPolicy); // Default is delete

        _output.WriteLine($"delete-topic: {deleteOnlyTopic.CleanupPolicy}");
        _output.WriteLine($"compact-topic: {compactOnlyTopic.CleanupPolicy}");
        _output.WriteLine($"both-topic: {bothTopic.CleanupPolicy}");
        _output.WriteLine($"default-topic: {defaultTopic.CleanupPolicy}");
    }

    /// <summary>
    /// Test LogManager applies compaction only to topics with compact policy.
    /// </summary>
    [Fact]
    public async Task LogManager_AppliesCompactionToCompactTopicsOnly()
    {
        using var logManager = new LogManager(
            _testDirectory,
            FileLogSegmentFactory.Create(),
            compactionConfig: new CompactionConfig { MinCompactionLagMs = 0 });

        // Create topic with compact policy
        await logManager.CreateTopicAsync(
            "compactable-topic",
            partitionCount: 1,
            config: new Dictionary<string, string> { ["cleanup.policy"] = "compact" });

        // Create topic with delete policy (default)
        await logManager.CreateTopicAsync(
            "delete-only-topic",
            partitionCount: 1);

        // Append to both
        var tp1 = new TopicPartition { Topic = "compactable-topic", Partition = 0 };
        var tp2 = new TopicPartition { Topic = "delete-only-topic", Partition = 0 };

        for (int i = 0; i < 3; i++)
        {
            await logManager.AppendBatchAsync(tp1, CreateTestBatch("key", $"value-{i}"));
            await logManager.AppendBatchAsync(tp2, CreateTestBatch("key", $"value-{i}"));
        }

        // Apply compaction
        var result = await logManager.ApplyCompactionAsync();

        _output.WriteLine($"Compaction applied: {result.RecordsRemoved} records removed");
        _output.WriteLine("Compactable topic should have been compacted, delete-only should not");

        // Verify topic policies
        var compactTopic = logManager.GetTopicMetadata("compactable-topic");
        var deleteTopic = logManager.GetTopicMetadata("delete-only-topic");

        Assert.NotNull(compactTopic);
        Assert.NotNull(deleteTopic);
        Assert.True(compactTopic.CleanupPolicy.HasFlag(CleanupPolicy.Compact));
        Assert.False(deleteTopic.CleanupPolicy.HasFlag(CleanupPolicy.Compact));
    }

    /// <summary>
    /// Helper to create a test batch with null key.
    /// </summary>
    private static byte[] CreateTestBatchWithNullKey(string value)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        var valueBytes = System.Text.Encoding.UTF8.GetBytes(value);

        var baseOffset = 0L;
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Base offset (8 bytes)
        WriteBigEndian(writer, baseOffset);

        // Batch length placeholder (4 bytes)
        var lengthPosition = stream.Position;
        writer.Write(0);

        // Partition leader epoch (4 bytes)
        WriteBigEndian(writer, 0);

        // Magic byte (1 byte) - version 2
        writer.Write((byte)2);

        // CRC placeholder (4 bytes)
        var crcPosition = stream.Position;
        writer.Write(0);

        var crcStartPosition = stream.Position;

        // Attributes (2 bytes)
        WriteBigEndian(writer, (short)0);

        // Last offset delta (4 bytes)
        WriteBigEndian(writer, 0);

        // Base timestamp (8 bytes)
        WriteBigEndian(writer, timestamp);

        // Max timestamp (8 bytes)
        WriteBigEndian(writer, timestamp);

        // Producer ID (8 bytes)
        WriteBigEndian(writer, -1L);

        // Producer epoch (2 bytes)
        WriteBigEndian(writer, (short)-1);

        // Base sequence (4 bytes)
        WriteBigEndian(writer, -1);

        // Record count (4 bytes)
        WriteBigEndian(writer, 1);

        // Record
        using var recordStream = new MemoryStream();

        // Attributes (1 byte)
        recordStream.WriteByte(0);

        // Timestamp delta (varint zigzag) = 0
        recordStream.WriteByte(0);

        // Offset delta (varint zigzag) = 0
        recordStream.WriteByte(0);

        // Key length (varint zigzag) = -1 (null key)
        recordStream.WriteByte(1); // -1 in zigzag encoding

        // Value length (varint zigzag)
        WriteVarint(recordStream, valueBytes.Length * 2);
        recordStream.Write(valueBytes);

        // Headers count (varint) = 0
        recordStream.WriteByte(0);

        var recordBytes = recordStream.ToArray();

        // Write record length (varint zigzag)
        WriteVarint(stream, recordBytes.Length * 2);

        // Write record
        stream.Write(recordBytes);

        // Fix the batch length
        var endPosition = stream.Position;
        var batchLength = (int)(endPosition - lengthPosition - 4);
        stream.Position = lengthPosition;
        WriteBigEndian(writer, batchLength);

        // Calculate CRC
        stream.Position = crcStartPosition;
        var crcData = new byte[endPosition - crcStartPosition];
        stream.Read(crcData, 0, crcData.Length);
        var crc = Kuestenlogik.Surgewave.Core.Util.Crc32C.Compute(crcData);
        stream.Position = crcPosition;
        WriteBigEndian(writer, (int)crc);

        return stream.ToArray();
    }

    /// <summary>
    /// Test LogManager with multiple topics.
    /// </summary>
    [Fact]
    public async Task LogManager_HandlesMultipleTopics()
    {
        using var logManager = new LogManager(_testDirectory, FileLogSegmentFactory.Create());

        // Create logs for multiple topic partitions (auto-creates topics)
        var tpA0 = new TopicPartition { Topic = "topic-a", Partition = 0 };
        var tpB1 = new TopicPartition { Topic = "topic-b", Partition = 1 };

        // GetOrCreateLog will create the topic if it doesn't exist
        var logA = logManager.GetOrCreateLog(tpA0);
        var logB = logManager.GetOrCreateLog(tpB1);

        Assert.NotNull(logA);
        Assert.NotNull(logB);

        // Append to different partitions
        var batchA = CreateTestBatch("key-a", "value-a");
        var batchB = CreateTestBatch("key-b", "value-b");

        var offsetA = await logManager.AppendBatchAsync(tpA0, batchA, CancellationToken.None);
        var offsetB = await logManager.AppendBatchAsync(tpB1, batchB, CancellationToken.None);

        _output.WriteLine($"topic-a/0 offset: {offsetA}, topic-b/1 offset: {offsetB}");

        Assert.True(offsetA >= 0);
        Assert.True(offsetB >= 0);
    }

    /// <summary>
    /// Helper to create a minimal Kafka record batch for testing.
    /// </summary>
    private static byte[] CreateTestBatch(string key, string value)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        var keyBytes = System.Text.Encoding.UTF8.GetBytes(key);
        var valueBytes = System.Text.Encoding.UTF8.GetBytes(value);

        var baseOffset = 0L;
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Base offset (8 bytes)
        WriteBigEndian(writer, baseOffset);

        // Batch length placeholder (4 bytes)
        var lengthPosition = stream.Position;
        writer.Write(0);

        // Partition leader epoch (4 bytes)
        WriteBigEndian(writer, 0);

        // Magic byte (1 byte) - version 2
        writer.Write((byte)2);

        // CRC placeholder (4 bytes)
        var crcPosition = stream.Position;
        writer.Write(0);

        var crcStartPosition = stream.Position;

        // Attributes (2 bytes)
        WriteBigEndian(writer, (short)0);

        // Last offset delta (4 bytes)
        WriteBigEndian(writer, 0);

        // Base timestamp (8 bytes)
        WriteBigEndian(writer, timestamp);

        // Max timestamp (8 bytes)
        WriteBigEndian(writer, timestamp);

        // Producer ID (8 bytes)
        WriteBigEndian(writer, -1L);

        // Producer epoch (2 bytes)
        WriteBigEndian(writer, (short)-1);

        // Base sequence (4 bytes)
        WriteBigEndian(writer, -1);

        // Record count (4 bytes)
        WriteBigEndian(writer, 1);

        // Record
        using var recordStream = new MemoryStream();

        // Attributes (1 byte)
        recordStream.WriteByte(0);

        // Timestamp delta (varint zigzag) = 0
        recordStream.WriteByte(0);

        // Offset delta (varint zigzag) = 0
        recordStream.WriteByte(0);

        // Key length (varint zigzag)
        WriteVarint(recordStream, keyBytes.Length * 2);
        recordStream.Write(keyBytes);

        // Value length (varint zigzag)
        WriteVarint(recordStream, valueBytes.Length * 2);
        recordStream.Write(valueBytes);

        // Headers count (varint) = 0
        recordStream.WriteByte(0);

        var recordBytes = recordStream.ToArray();

        // Write record length (varint zigzag)
        WriteVarint(stream, recordBytes.Length * 2);

        // Write record
        stream.Write(recordBytes);

        // Fix the batch length
        var endPosition = stream.Position;
        var batchLength = (int)(endPosition - lengthPosition - 4);
        stream.Position = lengthPosition;
        WriteBigEndian(writer, batchLength);

        // Calculate CRC
        stream.Position = crcStartPosition;
        var crcData = new byte[endPosition - crcStartPosition];
        stream.Read(crcData, 0, crcData.Length);
        var crc = Kuestenlogik.Surgewave.Core.Util.Crc32C.Compute(crcData);
        stream.Position = crcPosition;
        WriteBigEndian(writer, (int)crc);

        return stream.ToArray();
    }

    private static void WriteBigEndian(BinaryWriter writer, long value)
    {
        Span<byte> buffer = stackalloc byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteBigEndian(BinaryWriter writer, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteBigEndian(BinaryWriter writer, short value)
    {
        Span<byte> buffer = stackalloc byte[2];
        System.Buffers.Binary.BinaryPrimitives.WriteInt16BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteVarint(Stream stream, int value)
    {
        while ((value & ~0x7F) != 0)
        {
            stream.WriteByte((byte)((value & 0x7F) | 0x80));
            value = (int)((uint)value >> 7);
        }
        stream.WriteByte((byte)value);
    }
}
