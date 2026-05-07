using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

[Trait("Category", TestCategories.Unit)]
public class TtlTests : IDisposable
{
    private readonly TtlConfig _config;
    private readonly TtlIndex _ttlIndex;
    private readonly TopicPartition _tp = new() { Topic = "ttl-topic", Partition = 0 };

    public TtlTests()
    {
        _config = new TtlConfig
        {
            Enabled = true,
            DefaultTtlMs = 0,
            MaxTtlMs = 7 * 24 * 60 * 60 * 1000L,
            IndexCleanupIntervalMs = 600_000 // Long interval so sweep doesn't interfere
        };
        _ttlIndex = new TtlIndex(_config, null, NullLogger<TtlIndex>.Instance);
    }

    [Fact]
    public void ParseTtlHeader_ExtractsValue()
    {
        // Build a minimal record batch with a surgewave-ttl-ms header
        var ttlMs = 60_000L;
        var batch = CreateBatchWithTtlHeader(ttlMs);

        var extracted = TtlHeaderParser.ExtractExpiryTimestamp(batch);

        Assert.NotNull(extracted);
        // Expiry = baseTimestamp + ttlMs; baseTimestamp is set to UtcNow in the builder
        // So the expiry should be roughly now + 60s
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        Assert.True(extracted.Value > nowMs);
        Assert.True(extracted.Value < nowMs + 120_000); // Within 2 minutes tolerance
    }

    [Fact]
    public void TtlIndex_TracksExpiry()
    {
        var futureMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 60_000;
        _ttlIndex.RecordTtlBatch(_tp, offset: 0, expiryMs: futureMs);

        Assert.True(_ttlIndex.HasTtlRecords(_tp));
        Assert.Equal(1, _ttlIndex.TrackedCount);
    }

    [Fact]
    public void TtlFilter_ExcludesExpiredMessages()
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Record two batches -- one expired, one still valid
        _ttlIndex.RecordTtlBatch(_tp, offset: 0, expiryMs: nowMs - 1000); // Expired
        _ttlIndex.RecordTtlBatch(_tp, offset: 1, expiryMs: nowMs + 60_000); // Still valid

        var batches = new List<byte[]>
        {
            CreateBatchWithOffset(0),
            CreateBatchWithOffset(1),
            CreateBatchWithOffset(2) // Not in TTL index -- should pass through
        };

        var filtered = TtlFilter.FilterExpiredBatches(batches, _ttlIndex, _tp, nowMs);

        // Batch at offset 0 should be filtered out (expired)
        Assert.Equal(2, filtered.Count);

        // Verify remaining batches are offset 1 (valid) and offset 2 (not tracked)
        var offset1 = BinaryPrimitives.ReadInt64BigEndian(filtered[0].AsSpan(0, 8));
        var offset2 = BinaryPrimitives.ReadInt64BigEndian(filtered[1].AsSpan(0, 8));
        Assert.Equal(1, offset1);
        Assert.Equal(2, offset2);
    }

    [Fact]
    public void TtlFilter_IncludesNonExpiredMessages()
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        _ttlIndex.RecordTtlBatch(_tp, offset: 0, expiryMs: nowMs + 60_000);
        _ttlIndex.RecordTtlBatch(_tp, offset: 1, expiryMs: nowMs + 120_000);

        var batches = new List<byte[]>
        {
            CreateBatchWithOffset(0),
            CreateBatchWithOffset(1)
        };

        var filtered = TtlFilter.FilterExpiredBatches(batches, _ttlIndex, _tp, nowMs);

        // Both messages are still valid, none should be filtered
        Assert.Equal(2, filtered.Count);
    }

    [Fact]
    public void DefaultTtl_AppliedWhenNoHeader()
    {
        // Config with a default TTL
        var configWithDefault = new TtlConfig
        {
            Enabled = true,
            DefaultTtlMs = 5000, // 5 seconds
            MaxTtlMs = 7 * 24 * 60 * 60 * 1000L,
            IndexCleanupIntervalMs = 600_000
        };

        // Verify config value is set correctly
        Assert.Equal(5000, configWithDefault.DefaultTtlMs);
        Assert.True(configWithDefault.Enabled);

        // When a message has no TTL header and DefaultTtlMs > 0,
        // the DataApiHandler should apply the default TTL.
        // Here we verify that TtlHeaderParser returns null for a batch without TTL header,
        // which triggers the default TTL path in the handler.
        var batchWithoutTtl = CreateBatchWithOffset(0);
        var extracted = TtlHeaderParser.ExtractExpiryTimestamp(batchWithoutTtl);
        Assert.Null(extracted);

        // And verify the TTL index can track the default expiry
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var defaultExpiry = nowMs + configWithDefault.DefaultTtlMs;

        using var index = new TtlIndex(configWithDefault, null, NullLogger<TtlIndex>.Instance);
        index.RecordTtlBatch(_tp, offset: 0, expiryMs: defaultExpiry);

        Assert.True(index.HasTtlRecords(_tp));
        Assert.False(index.IsExpired(_tp, offset: 0, currentTimeMs: nowMs));
        Assert.True(index.IsExpired(_tp, offset: 0, currentTimeMs: nowMs + 6000));
    }

    /// <summary>
    /// Creates a minimal record batch with the specified base offset.
    /// </summary>
    private static byte[] CreateBatchWithOffset(long baseOffset)
    {
        var batch = new byte[KafkaConstants.RecordBatch.HeaderSize + 10];
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), baseOffset);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);
        return batch;
    }

    /// <summary>
    /// Creates a record batch containing one record with a surgewave-ttl-ms header.
    /// Uses proper Kafka record format with VarInt encoding.
    /// </summary>
    private static byte[] CreateBatchWithTtlHeader(long ttlMs)
    {
        // Build the record (inside the batch, after 61-byte header)
        var headerKey = Encoding.UTF8.GetBytes(TtlHeaderParser.TtlHeaderKey);
        var headerValue = Encoding.UTF8.GetBytes(ttlMs.ToString());

        using var recordMs = new MemoryStream();

        using var innerMs = new MemoryStream();

        innerMs.WriteByte(0); // attributes
        WriteVarInt(innerMs, 0); // timestampDelta
        WriteVarInt(innerMs, 0); // offsetDelta
        WriteVarInt(innerMs, -1); // keyLength (-1 = null)
        WriteVarInt(innerMs, -1); // valueLength (-1 = null)
        WriteVarInt(innerMs, 1); // headersCount = 1

        // Header: key
        WriteVarInt(innerMs, headerKey.Length);
        innerMs.Write(headerKey);

        // Header: value
        WriteVarInt(innerMs, headerValue.Length);
        innerMs.Write(headerValue);

        var innerBytes = innerMs.ToArray();

        // Write record length + inner content
        WriteVarInt(recordMs, innerBytes.Length);
        recordMs.Write(innerBytes);

        var recordBytes = recordMs.ToArray();

        // Build the full batch: 61-byte header + record data
        var batch = new byte[KafkaConstants.RecordBatch.HeaderSize + recordBytes.Length];
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), 0); // baseOffset
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12); // batchLength
        BinaryPrimitives.WriteInt64BigEndian(
            batch.AsSpan(KafkaConstants.RecordBatch.BaseTimestampOffset, 8),
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()); // baseTimestamp
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1); // recordCount = 1

        // Copy record data after header
        recordBytes.CopyTo(batch, KafkaConstants.RecordBatch.HeaderSize);

        return batch;
    }

    /// <summary>
    /// Write a ZigZag-encoded variable-length integer to a stream.
    /// </summary>
    private static void WriteVarInt(Stream stream, int value)
    {
        // ZigZag encode
        var zigzag = (uint)((value << 1) ^ (value >> 31));

        while (zigzag > 0x7F)
        {
            stream.WriteByte((byte)(zigzag | 0x80));
            zigzag >>= 7;
        }
        stream.WriteByte((byte)zigzag);
    }

    public void Dispose()
    {
        _ttlIndex.Dispose();
    }
}
