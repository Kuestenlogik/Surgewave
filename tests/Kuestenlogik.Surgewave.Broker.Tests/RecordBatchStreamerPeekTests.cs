using System.Buffers;
using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Broker.Native;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// PeekRecordCount sizes the fetch/push response writer up front (#83). The count it reads is
/// producer-controlled — Kafka-compat produce stores client batches verbatim — so these tests
/// pin down both the happy path and that a hostile count cannot blow up the estimate.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class RecordBatchStreamerPeekTests
{
    private static readonly RecordBatchSerializer Serializer = new(NullLogger<RecordBatchSerializer>.Instance);

    private static byte[] BuildBatch(int recordCount, int valueSize)
    {
        var messages = new List<Message>();
        for (var i = 0; i < recordCount; i++)
        {
            var value = new byte[valueSize];
            Random.Shared.NextBytes(value);
            messages.Add(new Message
            {
                Offset = i,
                Timestamp = 1_700_000_000_000,
                Key = ReadOnlyMemory<byte>.Empty,
                Value = value,
                Headers = ReadOnlyMemory<byte>.Empty
            });
        }

        return Serializer.SerializeMessages(messages);
    }

    [Theory]
    [InlineData(1, 10)]
    [InlineData(100, 10)]
    [InlineData(5, 1024)]
    public void PeekRecordCount_MatchesSerializedCount(int recordCount, int valueSize)
    {
        var batch = BuildBatch(recordCount, valueSize);

        Assert.Equal(recordCount, RecordBatchStreamer.PeekRecordCount(batch));
    }

    [Fact]
    public void PeekRecordCount_TruncatedBatch_ReturnsZero()
    {
        Assert.Equal(0, RecordBatchStreamer.PeekRecordCount(new byte[60]));
        Assert.Equal(0, RecordBatchStreamer.PeekRecordCount([]));
    }

    [Theory]
    [InlineData(int.MaxValue)]
    [InlineData(-1)]
    [InlineData(1_000_000)]
    public void PeekRecordCount_HostileCount_IsClampedToWhatTheBytesCanHold(int forgedCount)
    {
        // Unclamped, "count * 24" would overflow or go negative and make the writer rent throw
        // outside the caller's per-batch try/catch — killing the connection instead of skipping
        // one corrupt batch.
        var batch = BuildBatch(recordCount: 3, valueSize: 10);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), forgedCount);

        var count = RecordBatchStreamer.PeekRecordCount(batch);

        Assert.InRange(count, 0, (batch.Length - 61) / 7);
        // The estimate the fetch path derives from it must stay sane.
        Assert.True(12 + batch.Length + count * 24 > 0);
    }

    [Fact]
    public void StreamBatchRawToWriter_SmallRecords_FitsTheEstimate()
    {
        // The whole point of the estimate: the re-framed output must fit without a grow+copy.
        const int recordCount = 100;
        var batch = BuildBatch(recordCount, valueSize: 10);
        var estimate = 12 + batch.Length + RecordBatchStreamer.PeekRecordCount(batch) * 24;

        using var writer = BigEndianWriter.Rent(estimate);
        var written = RecordBatchStreamer.StreamBatchRawToWriter(batch, writer);

        Assert.Equal(recordCount, written);
        Assert.True(writer.Length <= estimate,
            $"Re-framed output {writer.Length} exceeded the pre-sized estimate {estimate}");
    }
}
