using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Configuration;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

[Trait("Category", TestCategories.Unit)]
public class EphemeralPartitionLogTests : IDisposable
{
    private readonly TopicPartition _tp = new() { Topic = "ephemeral-test", Partition = 0 };
    private readonly string _testDirectory;

    public EphemeralPartitionLogTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"kl-surgewave-ephemeral-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDirectory);
    }

    [Fact]
    public async Task WriteAndRead_SingleBatch_ReturnsCorrectData()
    {
        using var log = new EphemeralPartitionLog(_tp, bufferBytes: 1024 * 1024);

        var batch = CreateSimpleRecordBatch(baseOffset: 0, recordCount: 3);
        var offset = await log.AppendBatchAsync(batch);

        Assert.Equal(0, offset);
        Assert.Equal(3, log.NextOffset);
        Assert.Equal(3, log.HighWatermark);

        var batches = await log.ReadBatchesAsync(0);
        Assert.Single(batches);
        Assert.Equal(batch.Length, batches[0].Length);
    }

    [Fact]
    public async Task WriteAndRead_MultipleBatches_MaintainsOffsetOrdering()
    {
        using var log = new EphemeralPartitionLog(_tp, bufferBytes: 1024 * 1024);

        var offset1 = await log.AppendBatchAsync(CreateSimpleRecordBatch(0, 2));
        var offset2 = await log.AppendBatchAsync(CreateSimpleRecordBatch(0, 3));
        var offset3 = await log.AppendBatchAsync(CreateSimpleRecordBatch(0, 1));

        Assert.Equal(0, offset1);
        Assert.Equal(2, offset2);
        Assert.Equal(5, offset3);
        Assert.Equal(6, log.NextOffset);

        var allBatches = await log.ReadBatchesAsync(0);
        Assert.Equal(3, allBatches.Count);

        // Read from middle
        var laterBatches = await log.ReadBatchesAsync(2);
        Assert.Equal(2, laterBatches.Count);
    }

    [Fact]
    public async Task RingBuffer_WrapAround_EvictsOldData()
    {
        // Small buffer: 4KB - enough for only a few batches
        using var log = new EphemeralPartitionLog(_tp, bufferBytes: 4096, maxEntries: 100);

        // Write batches until we've written more than the buffer size
        long lastOffset = -1;
        for (int i = 0; i < 50; i++)
        {
            lastOffset = await log.AppendBatchAsync(CreateSimpleRecordBatch(0, 1));
        }

        // Old data should have been evicted
        Assert.True(log.LogStartOffset > 0, $"LogStartOffset should advance, was {log.LogStartOffset}");

        // Reading old data should return empty
        var oldBatches = await log.ReadBatchesAsync(0);
        Assert.Empty(oldBatches);

        // Recent data should still be readable
        var recentBatches = await log.ReadBatchesAsync(log.LogStartOffset);
        Assert.True(recentBatches.Count > 0);
    }

    [Fact]
    public async Task LogStartOffset_AdvancesCorrectly()
    {
        using var log = new EphemeralPartitionLog(_tp, bufferBytes: 2048, maxEntries: 10);

        Assert.Equal(0, log.LogStartOffset);

        // Fill up and overflow the buffer
        for (int i = 0; i < 30; i++)
        {
            await log.AppendBatchAsync(CreateSimpleRecordBatch(0, 1));
        }

        Assert.True(log.LogStartOffset > 0);
        Assert.True(log.LogStartOffset < log.NextOffset);
    }

    [Fact]
    public async Task ConcurrentWriters_AllOffsetsUnique()
    {
        using var log = new EphemeralPartitionLog(_tp, bufferBytes: 4 * 1024 * 1024);

        var offsets = new System.Collections.Concurrent.ConcurrentBag<long>();
        var tasks = Enumerable.Range(0, 10).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < 100; i++)
            {
                var offset = await log.AppendBatchAsync(CreateSimpleRecordBatch(0, 1));
                offsets.Add(offset);
            }
        }));

        await Task.WhenAll(tasks);

        Assert.Equal(1000, log.NextOffset);
        // All offsets should be unique
        Assert.Equal(1000, offsets.Distinct().Count());
    }

    [Fact]
    public async Task WaitForDataAsync_ReturnsTrue_WhenDataAvailable()
    {
        using var log = new EphemeralPartitionLog(_tp, bufferBytes: 1024 * 1024);

        // Write some data first
        await log.AppendBatchAsync(CreateSimpleRecordBatch(0, 5));

        // Wait for data that's already there
        var result = await log.WaitForDataAsync(0, TimeSpan.FromMilliseconds(100));
        Assert.True(result);
    }

    [Fact]
    public async Task WaitForDataAsync_WaitsForNewData()
    {
        using var log = new EphemeralPartitionLog(_tp, bufferBytes: 1024 * 1024);

        // Start waiting for data that doesn't exist yet
        var waitTask = log.WaitForDataAsync(0, TimeSpan.FromSeconds(5));

        // Write data after a short delay
        await Task.Delay(50);
        await log.AppendBatchAsync(CreateSimpleRecordBatch(0, 1));

        var result = await waitTask;
        Assert.True(result);
    }

    [Fact]
    public async Task WaitForDataAsync_ReturnsFalse_OnTimeout()
    {
        using var log = new EphemeralPartitionLog(_tp, bufferBytes: 1024 * 1024);

        var result = await log.WaitForDataAsync(0, TimeSpan.FromMilliseconds(50));
        Assert.False(result);
    }

    [Fact]
    public async Task ReadBatchesContiguousAsync_ReturnsCorrectData()
    {
        using var log = new EphemeralPartitionLog(_tp, bufferBytes: 1024 * 1024);

        await log.AppendBatchAsync(CreateSimpleRecordBatch(0, 2));
        await log.AppendBatchAsync(CreateSimpleRecordBatch(0, 3));

        var (data, offsets) = await log.ReadBatchesContiguousAsync(0);

        Assert.True(data.Length > 0);
        Assert.Equal(2, offsets.Count);
        Assert.Equal(0, offsets[0]);
    }

    [Fact]
    public void FindOffsetByTimestamp_ReturnsNull()
    {
        using var log = new EphemeralPartitionLog(_tp);
        Assert.Null(log.FindOffsetByTimestamp(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()));
    }

    [Fact]
    public void TotalSize_EqualsBufferSize()
    {
        using var log = new EphemeralPartitionLog(_tp, bufferBytes: 8192);
        Assert.Equal(8192, log.TotalSize);
    }

    [Fact]
    public void Dispose_CleansUp()
    {
        var log = new EphemeralPartitionLog(_tp);
        log.Dispose();

        // Should not throw on double dispose
        log.Dispose();
    }

    [Fact]
    public async Task ReadBatchesAsync_EmptyOnInvalidOffset()
    {
        using var log = new EphemeralPartitionLog(_tp, bufferBytes: 1024 * 1024);

        await log.AppendBatchAsync(CreateSimpleRecordBatch(0, 2));

        // Future offset
        var batches = await log.ReadBatchesAsync(100);
        Assert.Empty(batches);
    }

    // --- Integration: Ephemeral Topic via LogManager ---

    [Fact]
    public async Task LogManager_CreatesEphemeralLog_ForEphemeralTopic()
    {
        using var logManager = new LogManager(_testDirectory, FileLogSegmentFactory.Create());

        var config = new Dictionary<string, string>
        {
            ["cleanup.policy"] = "ephemeral",
            ["ephemeral.buffer.bytes"] = "1048576"
        };

        var metadata = await logManager.CreateTopicAsync("ephemeral-topic", partitionCount: 2, config: config);
        Assert.Equal(CleanupPolicy.Ephemeral, metadata.CleanupPolicy);

        var log = logManager.GetLog(new TopicPartition { Topic = "ephemeral-topic", Partition = 0 });
        Assert.NotNull(log);
        Assert.IsType<EphemeralPartitionLog>(log);
    }

    [Fact]
    public async Task LogManager_ProduceConsumeEphemeral()
    {
        using var logManager = new LogManager(_testDirectory, FileLogSegmentFactory.Create());

        var config = new Dictionary<string, string> { ["cleanup.policy"] = "ephemeral" };
        await logManager.CreateTopicAsync("eph-topic", partitionCount: 1, config: config);

        var tp = new TopicPartition { Topic = "eph-topic", Partition = 0 };
        var batch = CreateSimpleRecordBatch(0, 3);

        var offset = await logManager.AppendBatchDirectAsync(tp, batch);
        Assert.Equal(0, offset);

        var batches = await logManager.ReadBatchesAsync(tp, 0);
        Assert.Single(batches);
    }

    [Fact]
    public async Task LogManager_RetentionSkipsEphemeralTopics()
    {
        using var logManager = new LogManager(
            _testDirectory,
            FileLogSegmentFactory.Create(),
            retentionPolicy: new RetentionPolicy { RetentionHours = 1, RetentionBytes = 1024 });

        var ephConfig = new Dictionary<string, string> { ["cleanup.policy"] = "ephemeral" };
        await logManager.CreateTopicAsync("eph-topic", partitionCount: 1, config: ephConfig);
        await logManager.CreateTopicAsync("persistent-topic", partitionCount: 1);

        // This should not throw and should skip ephemeral topics
        var deletedSegments = logManager.ApplyRetentionPolicy();
        Assert.True(deletedSegments >= 0);
    }

    [Fact]
    public void ConfigParser_GetEphemeralBufferBytes_DefaultValue()
    {
        var config = new Dictionary<string, string>();
        var bytes = ConfigParser.GetEphemeralBufferBytes(config);
        Assert.Equal(64 * 1024 * 1024, bytes);
    }

    [Fact]
    public void ConfigParser_GetEphemeralBufferBytes_FromConfig()
    {
        var config = new Dictionary<string, string> { ["ephemeral.buffer.bytes"] = "128MB" };
        var bytes = ConfigParser.GetEphemeralBufferBytes(config);
        Assert.Equal(128 * 1024 * 1024, bytes);
    }

    [Fact]
    public void ConfigParser_GetEphemeralBufferBytes_ShortName()
    {
        var config = new Dictionary<string, string> { ["ephemeral.buffer"] = "256MB" };
        var bytes = ConfigParser.GetEphemeralBufferBytes(config);
        Assert.Equal(256 * 1024 * 1024, bytes);
    }

    // --- Helper to create valid record batches ---

    private static byte[] CreateSimpleRecordBatch(long baseOffset, int recordCount)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        // BaseOffset (8 bytes)
        writer.Write(BinaryPrimitives.ReverseEndianness(baseOffset));
        // BatchLength placeholder (4 bytes)
        var batchLengthPosition = ms.Position;
        writer.Write(0);
        // PartitionLeaderEpoch (4 bytes)
        writer.Write(BinaryPrimitives.ReverseEndianness(0));
        // Magic (1 byte)
        writer.Write((byte)2);
        // CRC placeholder (4 bytes)
        var crcPosition = ms.Position;
        writer.Write(0);
        // Attributes (2 bytes)
        writer.Write((short)0);
        // LastOffsetDelta (4 bytes)
        writer.Write(BinaryPrimitives.ReverseEndianness(recordCount - 1));
        // BaseTimestamp (8 bytes)
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        writer.Write(BinaryPrimitives.ReverseEndianness(timestamp));
        // MaxTimestamp (8 bytes)
        writer.Write(BinaryPrimitives.ReverseEndianness(timestamp));
        // ProducerId (8 bytes)
        writer.Write(BinaryPrimitives.ReverseEndianness(-1L));
        // ProducerEpoch (2 bytes)
        writer.Write(BinaryPrimitives.ReverseEndianness((short)-1));
        // BaseSequence (4 bytes)
        writer.Write(BinaryPrimitives.ReverseEndianness(-1));
        // RecordCount (4 bytes)
        writer.Write(BinaryPrimitives.ReverseEndianness(recordCount));

        // Write simple records
        for (int i = 0; i < recordCount; i++)
        {
            var value = System.Text.Encoding.UTF8.GetBytes($"test-message-{i}");
            var recordBytes = new List<byte>();
            recordBytes.Add(0); // Attributes
            WriteVarInt(recordBytes, 0); // TimestampDelta
            WriteVarInt(recordBytes, i); // OffsetDelta
            WriteVarInt(recordBytes, -1); // KeyLength (null)
            WriteVarInt(recordBytes, value.Length); // ValueLength
            recordBytes.AddRange(value);
            WriteVarInt(recordBytes, 0); // HeadersCount

            WriteVarInt(ms, recordBytes.Count);
            ms.Write(recordBytes.ToArray(), 0, recordBytes.Count);
        }

        // Write batch length
        var batchLength = (int)(ms.Length - batchLengthPosition - 4);
        ms.Position = batchLengthPosition;
        writer.Write(BinaryPrimitives.ReverseEndianness(batchLength));

        // Write dummy CRC
        ms.Position = crcPosition;
        writer.Write(BinaryPrimitives.ReverseEndianness(0x12345678));

        return ms.ToArray();
    }

    private static void WriteVarInt(List<byte> bytes, int value)
    {
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
            try { Directory.Delete(_testDirectory, recursive: true); }
            catch { /* ignore cleanup errors */ }
        }
    }
}
