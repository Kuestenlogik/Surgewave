using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Exceptions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests.Storage;

/// <summary>
/// The follower-ingest batch split (#92/#93). The leader packs N complete batches behind one
/// records-length prefix; the follower must split them and append each offset-preserving with its
/// own CRC. These tests pin the bug: the old whole-blob append stamped a whole-blob CRC into
/// batch 1 (#92) and advanced NextOffset only by batch 1's record count (#93).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class FollowerBatchSplitTests
{
    private const int BatchSize = 100;

    private static PartitionLog NewLog(string name)
        => new("in-memory", new TopicPartition { Topic = name, Partition = 0 }, new MemoryLogSegmentFactory());

    /// <summary>Appends a concatenated section the way the two follower fetchers now do.</summary>
    private static async Task<long> SplitAppendAsync(IPartitionLog log, byte[] section)
    {
        var cursor = 0;
        while (RecordBatchValidator.TryReadBatchBoundary(section, cursor, out var total, out var baseOffset, out _))
        {
            if (baseOffset >= log.NextOffset)
                await log.AppendBatchAtOffsetAsync(section, cursor, total, baseOffset, BatchCrcMode.Validate);
            cursor += total;
        }
        return log.NextOffset;
    }

    [Fact]
    public async Task SplitAppend_TwoBatchSection_AdvancesPastEveryBatch_AndKeepsEachCrc()
    {
        using var log = NewLog("split-repro");
        // Batch 1: offsets 0..2 (3 records), Batch 2: offsets 3..4 (2 records).
        var section = Concat(CreateValidBatch(0, 3), CreateValidBatch(3, 2));

        var leo = await SplitAppendAsync(log, section);

        // #93: NextOffset must be 5, not 3 (the old path advanced only by batch 1's record count).
        Assert.Equal(5, leo);
        Assert.Equal(5, log.NextOffset);

        // #92: both stored batches must validate against their OWN CRC.
        var stored = await log.ReadBatchesAsync(0);
        Assert.Equal(2, stored.Count);
        Assert.True(RecordBatchValidator.ValidateCrc(stored[0]), "batch 1 CRC must survive the split append");
        Assert.True(RecordBatchValidator.ValidateCrc(stored[1]), "batch 2 CRC must survive the split append");
    }

    [Fact]
    public async Task WholeBlobAppend_IsTheBug_LockingTheRegression()
    {
        // The OLD behaviour: hand the whole concatenation to one AppendBatchAtOffsetAsync. This is
        // exactly what the follower used to do — assert it is broken so the fix cannot silently regress.
        using var log = NewLog("whole-blob-bug");
        var section = Concat(CreateValidBatch(0, 3), CreateValidBatch(3, 2));

        await log.AppendBatchAtOffsetAsync(section, targetOffset: 0); // 2-arg = Recompute, whole blob

        // #93 symptom: only batch 1's count (3) is applied.
        Assert.Equal(3, log.NextOffset);

        // #92 symptom: a whole-blob CRC was stamped into batch 1's header, so batch 1 fails to validate.
        var stored = await log.ReadBatchesAsync(0);
        Assert.False(RecordBatchValidator.ValidateCrc(stored[0].AsSpan(0, BatchSize)),
            "the whole-blob append corrupts batch 1's CRC — this is the bug the split fixes");
    }

    [Fact]
    public async Task SplitAppend_ReRun_IsIdempotent_NoDuplicates()
    {
        using var log = NewLog("split-idempotent");
        var section = Concat(CreateValidBatch(0, 3), CreateValidBatch(3, 2));

        await SplitAppendAsync(log, section);
        var afterFirst = log.NextOffset;
        // Leader re-sends the same section (connection drop / partial-commit recovery).
        await SplitAppendAsync(log, section);

        Assert.Equal(afterFirst, log.NextOffset);        // no double-advance
        Assert.Equal(2, (await log.ReadBatchesAsync(0)).Count); // no duplicate on disk
    }

    [Fact]
    public async Task Validate_CorruptSecondBatch_KeepsGoodPrefix_Throws()
    {
        using var log = NewLog("split-corrupt");
        var b1 = CreateValidBatch(0, 3);
        var b2 = CreateValidBatch(3, 2);
        b2[^1] ^= 0xFF; // flip a body byte in batch 2 → its stored CRC no longer matches
        var section = Concat(b1, b2);

        var cursor = 0;
        var appended = 0;
        await Assert.ThrowsAsync<DataCorruptionException>(async () =>
        {
            while (RecordBatchValidator.TryReadBatchBoundary(section, cursor, out var total, out var baseOffset, out _))
            {
                await log.AppendBatchAtOffsetAsync(section, cursor, total, baseOffset, BatchCrcMode.Validate);
                appended++;
                cursor += total;
            }
        });

        Assert.Equal(1, appended);           // batch 1 committed, batch 2 refused
        Assert.Equal(3, log.NextOffset);     // only the good prefix advanced
    }

    [Theory]
    [InlineData(BatchCrcMode.Validate)]
    [InlineData(BatchCrcMode.Recompute)]
    public async Task AppendAtOffset_StampsBaseOffset_ButValidateKeepsCrc(BatchCrcMode mode)
    {
        using var log = NewLog($"offset-stamp-{mode}");
        var batch = CreateValidBatch(baseOffset: 0, recordCount: 2);
        var producerCrc = BinaryPrimitives.ReadUInt32BigEndian(batch.AsSpan(17, 4));

        // Append at a non-zero target so the base offset is genuinely re-stamped.
        await log.AppendBatchAtOffsetAsync(batch, 0, batch.Length, targetOffset: 10, mode);

        Assert.Equal(10, BinaryPrimitives.ReadInt64BigEndian(batch.AsSpan(0, 8)));
        if (mode == BatchCrcMode.Validate)
        {
            // Validate must not touch the CRC field — the batch keeps its producer checksum.
            Assert.Equal(producerCrc, BinaryPrimitives.ReadUInt32BigEndian(batch.AsSpan(17, 4)));
        }
        Assert.True(RecordBatchValidator.ValidateCrc(batch));
    }

    #region TryReadBatchBoundary

    [Fact]
    public void TryReadBatchBoundary_SingleBatch()
    {
        var batch = CreateValidBatch(0, 1);
        Assert.True(RecordBatchValidator.TryReadBatchBoundary(batch, 0, out var total, out var baseOffset, out var count));
        Assert.Equal(BatchSize, total);
        Assert.Equal(0, baseOffset);
        Assert.Equal(1, count);
        // No second batch.
        Assert.False(RecordBatchValidator.TryReadBatchBoundary(batch, BatchSize, out _, out _, out _));
    }

    [Fact]
    public void TryReadBatchBoundary_ThreeContiguousBatches()
    {
        var section = Concat(CreateValidBatch(0, 1), CreateValidBatch(1, 4), CreateValidBatch(5, 2));
        var expected = new (int total, long baseOffset, int count)[] { (BatchSize, 0, 1), (BatchSize, 1, 4), (BatchSize, 5, 2) };

        var cursor = 0;
        var i = 0;
        while (RecordBatchValidator.TryReadBatchBoundary(section, cursor, out var total, out var baseOffset, out var count))
        {
            Assert.Equal(expected[i], (total, baseOffset, count));
            cursor += total;
            i++;
        }
        Assert.Equal(3, i);
        Assert.Equal(section.Length, cursor);
    }

    [Fact]
    public void TryReadBatchBoundary_TruncatedTail_StopsCleanly_DoesNotThrow()
    {
        // A remote fetch may truncate the last batch at maxBytes — the walker must stop, not throw.
        var section = Concat(CreateValidBatch(0, 1), CreateValidBatch(1, 1));
        var truncated = section.AsSpan(0, section.Length - 10).ToArray(); // cut into batch 2

        Assert.True(RecordBatchValidator.TryReadBatchBoundary(truncated, 0, out var total, out _, out _));
        Assert.False(RecordBatchValidator.TryReadBatchBoundary(truncated, total, out _, out _, out _));
    }

    [Fact]
    public void TryReadBatchBoundary_UndersizedAndEmpty_ReturnFalse()
    {
        Assert.False(RecordBatchValidator.TryReadBatchBoundary([], 0, out _, out _, out _));
        Assert.False(RecordBatchValidator.TryReadBatchBoundary(new byte[8], 0, out _, out _, out _)); // < 12 prefix

        // Valid 12-byte prefix but batchLength below the fixed-header minimum.
        var bad = new byte[20];
        BinaryPrimitives.WriteInt32BigEndian(bad.AsSpan(8, 4), 4); // batchLength 4 << 49
        Assert.False(RecordBatchValidator.TryReadBatchBoundary(bad, 0, out _, out _, out _));
    }

    #endregion

    private static byte[] Concat(params byte[][] parts)
    {
        var result = new byte[parts.Sum(p => p.Length)];
        var pos = 0;
        foreach (var p in parts)
        {
            p.CopyTo(result, pos);
            pos += p.Length;
        }
        return result;
    }

    private static byte[] CreateValidBatch(long baseOffset, int recordCount)
    {
        var batch = new byte[BatchSize];
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), baseOffset);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(12, 4), 0);
        batch[16] = 2; // magic
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(21, 2), 0);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(23, 4), recordCount - 1); // lastOffsetDelta
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(27, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(35, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(43, 8), -1);
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(51, 2), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(53, 4), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), recordCount);
        var crc = Crc32C.Compute(batch.AsSpan(21));
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), crc);
        return batch;
    }
}
