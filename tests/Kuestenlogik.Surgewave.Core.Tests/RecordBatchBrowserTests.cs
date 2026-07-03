using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

[Trait("Category", TestCategories.Unit)]
public class RecordBatchBrowserTests
{
    private sealed record TestRecord(
        string? Key,
        string? Value,
        Dictionary<string, string>? Headers = null,
        long TimestampDelta = 0);

    // ── Uncompressed ────────────────────────────────────────────────────

    [Fact]
    public void Parse_UncompressedBatch_DecodesAllRecords()
    {
        var batch = BuildBatch(
            baseOffset: 100,
            firstTimestamp: 1_700_000_000_000,
            compressionType: KafkaConstants.Compression.None,
            new TestRecord("k0", "value-0", new Dictionary<string, string> { ["trace"] = "abc" }),
            new TestRecord("k1", "value-1", TimestampDelta: 5),
            new TestRecord(null, "value-2"));

        var result = RecordBatchBrowser.Parse(batch);

        Assert.False(result.IsCompressed);
        Assert.False(result.DecompressionFailed);
        Assert.Equal(100, result.BaseOffset);
        Assert.Equal(3, result.HeaderRecordCount);
        Assert.Equal(3, result.Records.Count);

        Assert.Equal(100, result.Records[0].Offset);
        Assert.Equal("k0", Encoding.UTF8.GetString(result.Records[0].Key!));
        Assert.Equal("value-0", Encoding.UTF8.GetString(result.Records[0].Value!));
        Assert.Equal("abc", result.Records[0].Headers["trace"]);

        Assert.Equal(101, result.Records[1].Offset);
        Assert.Equal(1_700_000_000_005, result.Records[1].TimestampMs);

        Assert.Null(result.Records[2].Key);
        Assert.Equal(102, result.Records[2].Offset);
    }

    [Fact]
    public void Parse_TombstoneRecord_ValueIsNull()
    {
        var batch = BuildBatch(0, 0, KafkaConstants.Compression.None,
            new TestRecord("deleted-key", null));

        var result = RecordBatchBrowser.Parse(batch);

        var record = Assert.Single(result.Records);
        Assert.Equal("deleted-key", Encoding.UTF8.GetString(record.Key!));
        Assert.Null(record.Value);
    }

    // ── Compressed ──────────────────────────────────────────────────────

    [Theory]
    [InlineData(KafkaConstants.Compression.Gzip)]
    [InlineData(KafkaConstants.Compression.Snappy)]
    [InlineData(KafkaConstants.Compression.Lz4)]
    [InlineData(KafkaConstants.Compression.Zstd)]
    public void Parse_CompressedBatch_DecompressesAndDecodesRecords(int compressionType)
    {
        var batch = BuildBatch(
            baseOffset: 42,
            firstTimestamp: 1_700_000_000_000,
            compressionType: compressionType,
            new TestRecord("key-a", "compressed payload A", new Dictionary<string, string> { ["h"] = "v" }),
            new TestRecord("key-b", "compressed payload B", TimestampDelta: 10));

        var result = RecordBatchBrowser.Parse(batch);

        Assert.True(result.IsCompressed);
        Assert.False(result.DecompressionFailed);
        Assert.Equal(compressionType, result.CompressionType);
        Assert.Equal(2, result.Records.Count);

        Assert.Equal(42, result.Records[0].Offset);
        Assert.Equal("key-a", Encoding.UTF8.GetString(result.Records[0].Key!));
        Assert.Equal("compressed payload A", Encoding.UTF8.GetString(result.Records[0].Value!));
        Assert.Equal("v", result.Records[0].Headers["h"]);

        Assert.Equal(43, result.Records[1].Offset);
        Assert.Equal(1_700_000_000_010, result.Records[1].TimestampMs);
        Assert.Equal("compressed payload B", Encoding.UTF8.GetString(result.Records[1].Value!));
    }

    [Fact]
    public void Parse_CorruptCompressedPayload_ReportsDecompressionFailed()
    {
        var batch = BuildBatch(7, 1_000, KafkaConstants.Compression.Gzip,
            new TestRecord("k", "v"));
        // Destroy the compressed payload (keep the 61-byte header intact).
        for (var i = 61; i < batch.Length; i++)
        {
            batch[i] = 0xFF;
        }

        var result = RecordBatchBrowser.Parse(batch);

        Assert.True(result.DecompressionFailed);
        Assert.True(result.IsCompressed);
        Assert.Empty(result.Records);
        Assert.Equal(7, result.BaseOffset);
        Assert.Equal(1, result.HeaderRecordCount);
    }

    // ── Malformed input ─────────────────────────────────────────────────

    [Fact]
    public void Parse_TooShortInput_ReturnsEmpty()
    {
        var result = RecordBatchBrowser.Parse(new byte[10]);

        Assert.Empty(result.Records);
        Assert.False(result.DecompressionFailed);
    }

    [Fact]
    public void Parse_TruncatedRecords_StopsWithoutThrowing()
    {
        var batch = BuildBatch(0, 0, KafkaConstants.Compression.None,
            new TestRecord("key", "a value that is long enough to truncate"));
        var truncated = batch.AsSpan(0, batch.Length - 10).ToArray();
        // Keep the declared batch length in sync with the truncation so the
        // records section is clamped by the buffer, not the header field.
        BinaryPrimitives.WriteInt32BigEndian(truncated.AsSpan(8), truncated.Length - 12);

        var result = RecordBatchBrowser.Parse(truncated);

        Assert.Empty(result.Records);
        Assert.False(result.DecompressionFailed);
    }

    // ── Batch builder (Kafka v2 wire format) ────────────────────────────

    private static byte[] BuildBatch(
        long baseOffset, long firstTimestamp, int compressionType, params TestRecord[] records)
    {
        using var recordsStream = new MemoryStream();
        for (var i = 0; i < records.Length; i++)
        {
            WriteRecord(recordsStream, records[i], offsetDelta: i);
        }

        var recordsSection = recordsStream.ToArray();
        if (compressionType != KafkaConstants.Compression.None)
        {
            recordsSection = CompressionCodec.Compress(recordsSection, compressionType);
        }

        var batch = new byte[61 + recordsSection.Length];
        var span = batch.AsSpan();
        BinaryPrimitives.WriteInt64BigEndian(span, baseOffset);
        BinaryPrimitives.WriteInt32BigEndian(span[8..], batch.Length - 12); // batchLength
        BinaryPrimitives.WriteInt32BigEndian(span[12..], 0);                // partitionLeaderEpoch
        span[16] = 2;                                                       // magic
        BinaryPrimitives.WriteUInt32BigEndian(span[17..], 0);               // crc (unchecked by browser)
        BinaryPrimitives.WriteInt16BigEndian(span[21..], (short)compressionType);
        BinaryPrimitives.WriteInt32BigEndian(span[23..], records.Length - 1); // lastOffsetDelta
        BinaryPrimitives.WriteInt64BigEndian(span[27..], firstTimestamp);
        BinaryPrimitives.WriteInt64BigEndian(span[35..], firstTimestamp);   // maxTimestamp
        BinaryPrimitives.WriteInt64BigEndian(span[43..], -1);               // producerId
        BinaryPrimitives.WriteInt16BigEndian(span[51..], -1);               // producerEpoch
        BinaryPrimitives.WriteInt32BigEndian(span[53..], -1);               // baseSequence
        BinaryPrimitives.WriteInt32BigEndian(span[57..], records.Length);
        recordsSection.CopyTo(span[61..]);
        return batch;
    }

    private static void WriteRecord(MemoryStream target, TestRecord record, int offsetDelta)
    {
        using var body = new MemoryStream();
        body.WriteByte(0); // record attributes (int8)
        WriteVarLong(body, record.TimestampDelta);
        WriteVarInt(body, offsetDelta);
        WriteSizedBytes(body, record.Key is null ? null : Encoding.UTF8.GetBytes(record.Key));
        WriteSizedBytes(body, record.Value is null ? null : Encoding.UTF8.GetBytes(record.Value));

        var headers = record.Headers ?? [];
        WriteVarInt(body, headers.Count);
        foreach (var (key, value) in headers)
        {
            WriteSizedBytes(body, Encoding.UTF8.GetBytes(key));
            WriteSizedBytes(body, Encoding.UTF8.GetBytes(value));
        }

        WriteVarInt(target, (int)body.Length);
        body.WriteTo(target);
    }

    private static void WriteSizedBytes(MemoryStream target, byte[]? bytes)
    {
        if (bytes is null)
        {
            WriteVarInt(target, -1);
            return;
        }

        WriteVarInt(target, bytes.Length);
        target.Write(bytes);
    }

    private static void WriteVarInt(MemoryStream target, int value)
        => WriteVarLong(target, value);

    private static void WriteVarLong(MemoryStream target, long value)
    {
        var zigzag = (ulong)((value << 1) ^ (value >> 63));
        while (zigzag >= 0x80)
        {
            target.WriteByte((byte)(zigzag | 0x80));
            zigzag >>= 7;
        }

        target.WriteByte((byte)zigzag);
    }
}
