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
public class DelayedDeliveryTests : IDisposable
{
    private readonly DeliveryDelayConfig _config;
    private readonly DelayIndex _delayIndex;
    private readonly TopicPartition _tp = new() { Topic = "delayed-topic", Partition = 0 };

    public DelayedDeliveryTests()
    {
        _config = new DeliveryDelayConfig
        {
            Enabled = true,
            MaxDelayMs = 7 * 24 * 60 * 60 * 1000L, // 7 days
            IndexCleanupIntervalMs = 600_000 // Long interval so sweep doesn't interfere
        };
        _delayIndex = new DelayIndex(_config, null, NullLogger<DelayIndex>.Instance);
    }

    [Fact]
    public void RecordDelayedBatch_TracksInIndex()
    {
        var futureMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 60_000;
        _delayIndex.RecordDelayedBatch(_tp, offset: 0, deliverAtMs: futureMs);

        Assert.True(_delayIndex.HasDelayedRecords(_tp));
        Assert.Equal(1, _delayIndex.PendingCount);
    }

    [Fact]
    public void IsDelayed_FutureDelivery_ReturnsTrue()
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var futureMs = nowMs + 60_000;
        _delayIndex.RecordDelayedBatch(_tp, offset: 5, deliverAtMs: futureMs);

        Assert.True(_delayIndex.IsDelayed(_tp, offset: 5, currentTimeMs: nowMs));
    }

    [Fact]
    public void IsDelayed_PastDelivery_ReturnsFalse()
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var pastMs = nowMs - 10_000;
        _delayIndex.RecordDelayedBatch(_tp, offset: 5, deliverAtMs: pastMs);

        Assert.False(_delayIndex.IsDelayed(_tp, offset: 5, currentTimeMs: nowMs));
    }

    [Fact]
    public void IsDelayed_UnknownOffset_ReturnsFalse()
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _delayIndex.RecordDelayedBatch(_tp, offset: 5, deliverAtMs: nowMs + 60_000);

        Assert.False(_delayIndex.IsDelayed(_tp, offset: 99, currentTimeMs: nowMs));
    }

    [Fact]
    public void HasDelayedRecords_EmptyPartition_ReturnsFalse()
    {
        Assert.False(_delayIndex.HasDelayedRecords(_tp));
    }

    [Fact]
    public void MaxDelay_CapsExcessiveDelay()
    {
        var shortMaxConfig = new DeliveryDelayConfig
        {
            Enabled = true,
            MaxDelayMs = 10_000, // 10 seconds max
            IndexCleanupIntervalMs = 600_000
        };
        var index = new DelayIndex(shortMaxConfig, null, NullLogger<DelayIndex>.Instance);

        var beforeRecord = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var excessiveDelay = beforeRecord + 999_999_999; // Way beyond max
        index.RecordDelayedBatch(_tp, offset: 0, deliverAtMs: excessiveDelay);
        var afterRecord = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // RecordDelayedBatch reads its own UtcNow to compute the cap (nowInternal + MaxDelayMs).
        // Between our captured timestamps and that internal read, time can drift — under load
        // the drift is normally a few ms but can spike to hundreds of ms on CI. Use a margin
        // comfortably larger than that drift so the test is not timing-sensitive.
        var driftMarginMs = Math.Max(500L, (afterRecord - beforeRecord) + 200);
        var afterMaxDelay = afterRecord + 10_000 + driftMarginMs;
        Assert.False(index.IsDelayed(_tp, offset: 0, currentTimeMs: afterMaxDelay));

        index.Dispose();
    }

    [Fact]
    public void FilterDelayedBatches_ExcludesFutureBatches()
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Record two delayed batches — one ready, one still delayed
        _delayIndex.RecordDelayedBatch(_tp, offset: 0, deliverAtMs: nowMs - 1000); // Ready
        _delayIndex.RecordDelayedBatch(_tp, offset: 1, deliverAtMs: nowMs + 60_000); // Still delayed

        var batches = new List<byte[]>
        {
            CreateBatchWithOffset(0),
            CreateBatchWithOffset(1),
            CreateBatchWithOffset(2) // Not in delay index — should pass through
        };

        var filtered = DelayFilter.FilterDelayedBatches(batches, _delayIndex, _tp, nowMs);

        // Batch at offset 1 should be filtered out (still delayed)
        Assert.Equal(2, filtered.Count);

        // Verify the remaining batches are offset 0 (ready) and offset 2 (not delayed)
        var offset0 = BinaryPrimitives.ReadInt64BigEndian(filtered[0].AsSpan(0, 8));
        var offset2 = BinaryPrimitives.ReadInt64BigEndian(filtered[1].AsSpan(0, 8));
        Assert.Equal(0, offset0);
        Assert.Equal(2, offset2);
    }

    [Fact]
    public void FilterDelayedBatches_AllReady_ReturnsAll()
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        _delayIndex.RecordDelayedBatch(_tp, offset: 0, deliverAtMs: nowMs - 5000);
        _delayIndex.RecordDelayedBatch(_tp, offset: 1, deliverAtMs: nowMs - 1000);

        var batches = new List<byte[]>
        {
            CreateBatchWithOffset(0),
            CreateBatchWithOffset(1)
        };

        var filtered = DelayFilter.FilterDelayedBatches(batches, _delayIndex, _tp, nowMs);

        Assert.Equal(2, filtered.Count);
    }

    [Fact]
    public void MultiplePartitions_IndependentTracking()
    {
        var tp2 = new TopicPartition { Topic = "delayed-topic", Partition = 1 };
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        _delayIndex.RecordDelayedBatch(_tp, offset: 0, deliverAtMs: nowMs + 60_000);

        Assert.True(_delayIndex.HasDelayedRecords(_tp));
        Assert.False(_delayIndex.HasDelayedRecords(tp2));
    }

    [Fact]
    public void PendingCount_AcrossPartitions()
    {
        var tp2 = new TopicPartition { Topic = "delayed-topic", Partition = 1 };
        var futureMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 60_000;

        _delayIndex.RecordDelayedBatch(_tp, offset: 0, deliverAtMs: futureMs);
        _delayIndex.RecordDelayedBatch(_tp, offset: 1, deliverAtMs: futureMs);
        _delayIndex.RecordDelayedBatch(tp2, offset: 0, deliverAtMs: futureMs);

        Assert.Equal(3, _delayIndex.PendingCount);
    }

    [Fact]
    public void DelayHeaderParser_ExtractsDeliverAtHeader()
    {
        // Build a minimal record batch with a surgewave-deliver-at-ms header
        var deliverAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 30_000;
        var batch = CreateBatchWithDeliverAtHeader(deliverAt);

        var extracted = DelayHeaderParser.ExtractDeliverAtTimestamp(batch);

        Assert.NotNull(extracted);
        Assert.Equal(deliverAt, extracted.Value);
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
    /// Creates a record batch containing one record with a surgewave-deliver-at-ms header.
    /// Uses proper Kafka record format with VarInt encoding.
    /// </summary>
    private static byte[] CreateBatchWithDeliverAtHeader(long deliverAtMs)
    {
        // Build the record (inside the batch, after 61-byte header)
        var headerKey = Encoding.UTF8.GetBytes(DelayHeaderParser.DeliverAtHeaderKey);
        var headerValue = Encoding.UTF8.GetBytes(deliverAtMs.ToString());

        using var recordMs = new MemoryStream();

        // Record fields: attributes(1) + timestampDelta(varint) + offsetDelta(varint)
        //   + keyLength(varint) + key + valueLength(varint) + value
        //   + headersCount(varint) + [headerKeyLen(varint) + headerKey + headerValLen(varint) + headerVal]

        // We'll build inner content first, then prepend the length
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
        _delayIndex.Dispose();
    }
}
