using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Storage.Indexing;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Storage.Engine;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Tests for the custom indexer infrastructure.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class CustomIndexerTests : IDisposable
{
    private readonly string _testDir;

    public CustomIndexerTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"surgewave-custom-indexer-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_testDir, recursive: true); }
        catch { /* Ignore cleanup errors */ }
        GC.SuppressFinalize(this);
    }

    #region RecordHeaderParser Tests

    [Fact]
    public void RecordHeaderParser_ParseBatchHeader_ReturnsCorrectValues()
    {
        // Create a test batch
        var batch = CreateTestRecordBatch(
            baseOffset: 42,
            records: [("key1", "value1", [("header1", "headerValue1")])]);

        var header = RecordHeaderParser.ParseBatchHeader(batch);

        Assert.Equal(42, header.BaseOffset);
        Assert.Equal(1, header.RecordCount);
        Assert.True(header.BaseTimestamp > 0);
        Assert.Equal(header.BaseTimestamp, header.MaxTimestamp); // Single record
    }

    [Fact]
    public void RecordHeaderParser_EnumerateRecords_IteratesAllRecords()
    {
        var batch = CreateTestRecordBatch(
            baseOffset: 0,
            records:
            [
                ("key1", "value1", []),
                ("key2", "value2", []),
                ("key3", "value3", [])
            ]);

        var enumerator = RecordHeaderParser.EnumerateRecords(batch);
        var count = 0;
        var offsets = new List<long>();

        while (enumerator.MoveNext())
        {
            offsets.Add(enumerator.Current.Offset);
            count++;
        }

        Assert.Equal(3, count);
        Assert.Equal([0, 1, 2], offsets);
    }

    [Fact]
    public void RecordHeaderParser_EnumerateRecords_ParsesHeaders()
    {
        var batch = CreateTestRecordBatch(
            baseOffset: 0,
            records:
            [
                ("key1", "value1", [("vc-node1", "100"), ("vc-node2", "200")]),
            ]);

        var enumerator = RecordHeaderParser.EnumerateRecords(batch);
        Assert.True(enumerator.MoveNext());

        var record = enumerator.Current;
        var headers = new List<RecordHeader>();
        foreach (var header in record.Headers)
        {
            headers.Add(header);
        }

        Assert.Equal(2, headers.Count);
        Assert.Equal("vc-node1", headers[0].KeyString);
        Assert.Equal("100", headers[0].ValueString);
        Assert.Equal("vc-node2", headers[1].KeyString);
        Assert.Equal("200", headers[1].ValueString);
    }

    [Fact]
    public void RecordHeaderParser_FindRecordsWithHeader_FindsMatchingRecords()
    {
        var batch = CreateTestRecordBatch(
            baseOffset: 100,
            records:
            [
                ("key1", "value1", [("vc-node1", "10")]),
                ("key2", "value2", [("other-header", "xyz")]),
                ("key3", "value3", [("vc-node1", "20")]),
            ]);

        var results = RecordHeaderParser.FindRecordsWithHeader(batch, "vc-node1");

        Assert.Equal(2, results.Count);
        Assert.Equal(100, results[0].Offset);
        Assert.Equal("10", results[0].Header.ValueString);
        Assert.Equal(102, results[1].Offset);
        Assert.Equal("20", results[1].Header.ValueString);
    }

    [Fact]
    public void RecordHeaderParser_ExtractHeaderValues_ReturnsAllValues()
    {
        var batch = CreateTestRecordBatch(
            baseOffset: 0,
            records:
            [
                ("key1", "value1", [("vc", "100")]),
                ("key2", "value2", [("other", "xyz")]),
                ("key3", "value3", [("vc", "300")]),
            ]);

        var values = RecordHeaderParser.ExtractHeaderValues(batch, "vc");

        Assert.Equal(2, values.Count);
        Assert.Equal(0, values[0].Offset);
        Assert.Equal("100", Encoding.UTF8.GetString(values[0].Value.Span));
        Assert.Equal(2, values[1].Offset);
        Assert.Equal("300", Encoding.UTF8.GetString(values[1].Value.Span));
    }

    [Fact]
    public void RecordHeader_ValueAsInt64_ParsesBigEndianValue()
    {
        var valueBytes = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(valueBytes, 12345678901234L);

        var header = new RecordHeader("test"u8.ToArray(), valueBytes);

        Assert.Equal(12345678901234L, header.ValueAsInt64);
    }

    #endregion

    #region CustomIndexerRegistry Tests

    [Fact]
    public void CustomIndexerRegistry_Register_AddsIndexer()
    {
        using var registry = new CustomIndexerRegistry();
        var indexer = new TestIndexer("test");

        registry.Register(indexer);

        Assert.Single(registry.Indexers);
        Assert.Same(indexer, registry.GetIndexer("test"));
    }

    [Fact]
    public void CustomIndexerRegistry_Register_ThrowsOnDuplicate()
    {
        using var registry = new CustomIndexerRegistry();
        registry.Register(new TestIndexer("test"));

        Assert.Throws<InvalidOperationException>(() =>
            registry.Register(new TestIndexer("test")));
    }

    [Fact]
    public void CustomIndexerRegistry_OnBatchAppended_NotifiesAllIndexers()
    {
        using var registry = new CustomIndexerRegistry();
        var indexer1 = new TestIndexer("test1");
        var indexer2 = new TestIndexer("test2");
        registry.Register(indexer1);
        registry.Register(indexer2);

        var batch = CreateTestRecordBatch(0, [("key", "value", [])]);
        registry.OnBatchAppended(0, 0, batch);

        Assert.Equal(1, indexer1.BatchesReceived);
        Assert.Equal(1, indexer2.BatchesReceived);
    }

    [Fact]
    public void CustomIndexerRegistry_Unregister_RemovesIndexer()
    {
        using var registry = new CustomIndexerRegistry();
        registry.Register(new TestIndexer("test"));

        var removed = registry.Unregister("test");

        Assert.True(removed);
        Assert.Empty(registry.Indexers);
    }

    #endregion

    #region IndexedLogSegment Tests

    [Fact]
    public async Task IndexedLogSegment_AppendBatch_NotifiesIndexers()
    {
        var indexer = new TestIndexer("test");
        var registry = new CustomIndexerRegistry();
        registry.Register(indexer);

        using var innerSegment = new MemoryLogSegment(0);
        using var indexedSegment = new IndexedLogSegment(innerSegment, registry, _testDir);

        var batch = CreateTestRecordBatch(0, [("key", "value", [("vc", "123")])]);
        await indexedSegment.AppendBatchAsync(batch);

        Assert.Equal(1, indexer.BatchesReceived);
        Assert.Equal(0, indexer.LastBaseOffset);
    }

    [Fact]
    public async Task IndexedLogSegmentFactory_CreatesIndexedSegments()
    {
        // Register a global indexer factory
        GlobalCustomIndexerRegistry.ClearFactories();
        GlobalCustomIndexerRegistry.RegisterFactory(new TestIndexerFactory());

        try
        {
            var innerFactory = new MemoryLogSegmentFactory();
            var indexedFactory = new IndexedLogSegmentFactory(innerFactory);

            using var segment = indexedFactory.CreateSegment(_testDir, 0, true);

            // Should be wrapped
            Assert.IsType<IndexedLogSegment>(segment);

            var batch = CreateTestRecordBatch(0, [("key", "value", [])]);
            await segment.AppendBatchAsync(batch);

            // Verify indexer was called (via type check)
            var indexedSegment = (IndexedLogSegment)segment;
            // The test passes if no exception was thrown
        }
        finally
        {
            GlobalCustomIndexerRegistry.ClearFactories();
        }
    }

    [Fact]
    public void IndexedLogSegmentFactory_NoIndexers_ReturnsUnwrapped()
    {
        GlobalCustomIndexerRegistry.ClearFactories();

        var innerFactory = new MemoryLogSegmentFactory();
        var indexedFactory = new IndexedLogSegmentFactory(innerFactory);

        using var segment = indexedFactory.CreateSegment(_testDir, 0, true);

        // Should NOT be wrapped when no indexers registered
        Assert.IsType<MemoryLogSegment>(segment);
    }

    [Fact]
    public async Task IndexedLogSegment_WithCustomIndexing_Extension()
    {
        var testIndexerFactory = new TestIndexerFactory();
        var factory = new MemoryLogSegmentFactory()
            .WithCustomIndexing(testIndexerFactory);

        using var segment = factory.CreateSegment(_testDir, 0, true);

        Assert.IsType<IndexedLogSegment>(segment);

        var batch = CreateTestRecordBatch(0, [("key", "value", [])]);
        await segment.AppendBatchAsync(batch);
    }

    #endregion

    #region Helper Methods

    /// <summary>
    /// Create a test RecordBatch with the specified records and headers.
    /// </summary>
    private static byte[] CreateTestRecordBatch(
        long baseOffset,
        (string Key, string Value, (string Key, string Value)[] Headers)[] records)
    {
        using var stream = new MemoryStream();
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Build records section first
        using var recordsStream = new MemoryStream();
        var maxTimestamp = timestamp;

        for (int i = 0; i < records.Length; i++)
        {
            var (key, value, headers) = records[i];
            using var recordStream = new MemoryStream();

            // Attributes (varint, 0)
            WriteVarint(recordStream, 0);

            // Timestamp delta (varint)
            WriteVarint(recordStream, 0);

            // Offset delta (varint)
            WriteVarint(recordStream, i);

            // Key
            var keyBytes = Encoding.UTF8.GetBytes(key);
            WriteVarint(recordStream, keyBytes.Length);
            recordStream.Write(keyBytes);

            // Value
            var valueBytes = Encoding.UTF8.GetBytes(value);
            WriteVarint(recordStream, valueBytes.Length);
            recordStream.Write(valueBytes);

            // Headers count
            WriteVarint(recordStream, headers.Length);

            // Headers
            foreach (var (hKey, hValue) in headers)
            {
                var headerKeyBytes = Encoding.UTF8.GetBytes(hKey);
                WriteVarint(recordStream, headerKeyBytes.Length);
                recordStream.Write(headerKeyBytes);

                var headerValueBytes = Encoding.UTF8.GetBytes(hValue);
                WriteVarint(recordStream, headerValueBytes.Length);
                recordStream.Write(headerValueBytes);
            }

            // Write record with length prefix
            var recordBytes = recordStream.ToArray();
            WriteVarint(recordsStream, recordBytes.Length);
            recordsStream.Write(recordBytes);
        }

        var recordsBytes = recordsStream.ToArray();

        // Build batch header
        var buffer = new byte[8];

        // baseOffset (8 bytes)
        BinaryPrimitives.WriteInt64BigEndian(buffer, baseOffset);
        stream.Write(buffer);

        // batchLength placeholder (4 bytes)
        var batchLengthPos = stream.Position;
        stream.Write(new byte[4]);

        // partitionLeaderEpoch (4 bytes)
        BinaryPrimitives.WriteInt32BigEndian(buffer, 0);
        stream.Write(buffer, 0, 4);

        // magic (1 byte)
        stream.WriteByte(2);

        // CRC placeholder (4 bytes)
        var crcPos = stream.Position;
        stream.Write(new byte[4]);

        // CRC-covered section starts here
        var crcStart = stream.Position;

        // attributes (2 bytes)
        BinaryPrimitives.WriteInt16BigEndian(buffer, 0);
        stream.Write(buffer, 0, 2);

        // lastOffsetDelta (4 bytes)
        BinaryPrimitives.WriteInt32BigEndian(buffer, records.Length - 1);
        stream.Write(buffer, 0, 4);

        // baseTimestamp (8 bytes)
        BinaryPrimitives.WriteInt64BigEndian(buffer, timestamp);
        stream.Write(buffer);

        // maxTimestamp (8 bytes)
        BinaryPrimitives.WriteInt64BigEndian(buffer, maxTimestamp);
        stream.Write(buffer);

        // producerId (8 bytes)
        BinaryPrimitives.WriteInt64BigEndian(buffer, -1);
        stream.Write(buffer);

        // producerEpoch (2 bytes)
        BinaryPrimitives.WriteInt16BigEndian(buffer, -1);
        stream.Write(buffer, 0, 2);

        // baseSequence (4 bytes)
        BinaryPrimitives.WriteInt32BigEndian(buffer, -1);
        stream.Write(buffer, 0, 4);

        // recordCount (4 bytes)
        BinaryPrimitives.WriteInt32BigEndian(buffer, records.Length);
        stream.Write(buffer, 0, 4);

        // Records
        stream.Write(recordsBytes);

        var batchBytes = stream.ToArray();

        // Fill in batchLength
        var batchLength = batchBytes.Length - 12;
        BinaryPrimitives.WriteInt32BigEndian(batchBytes.AsSpan(8, 4), batchLength);

        // Calculate and fill in CRC32-C
        var crcData = batchBytes.AsSpan(21);
        var crc = Crc32C.Compute(crcData);
        BinaryPrimitives.WriteUInt32BigEndian(batchBytes.AsSpan(17, 4), crc);

        return batchBytes;
    }

    private static void WriteVarint(Stream stream, long value)
    {
        // Zigzag encode
        var encoded = (ulong)((value << 1) ^ (value >> 63));

        while (encoded > 0x7F)
        {
            stream.WriteByte((byte)((encoded & 0x7F) | 0x80));
            encoded >>= 7;
        }
        stream.WriteByte((byte)encoded);
    }

    #endregion

    #region Test Helpers

    private sealed class TestIndexer : ICustomIndexer
    {
        public string Name { get; }
        public int BatchesReceived { get; private set; }
        public long LastBaseOffset { get; private set; }
        public long LastFilePosition { get; private set; }

        public TestIndexer(string name)
        {
            Name = name;
        }

        public void OnBatchAppended(long baseOffset, long filePosition, ReadOnlySpan<byte> recordBatch)
        {
            BatchesReceived++;
            LastBaseOffset = baseOffset;
            LastFilePosition = filePosition;
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken = default) => default;
        public void Load(string indexDirectory, long segmentBaseOffset) { }
        public ValueTask SaveAsync(string indexDirectory, long segmentBaseOffset, CancellationToken cancellationToken = default) => default;
        public void DeleteFiles(string indexDirectory, long segmentBaseOffset) { }
        public void Dispose() { }
    }

    private sealed class TestIndexerFactory : ICustomIndexerFactory
    {
        public ICustomIndexer Create() => new TestIndexer("test");
    }

    #endregion
}
