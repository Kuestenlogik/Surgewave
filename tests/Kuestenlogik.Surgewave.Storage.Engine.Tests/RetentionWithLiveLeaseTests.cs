using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// Retention running while a fetch still holds a borrowed read of the data being deleted — on the
/// real File engine with memory-mapped reads.
///
/// <para><b>What was suspected, and what the measurement says.</b> A borrowed read can own a
/// memory-mapped view, and Windows refuses to delete a file that has a live mapping — so the worry
/// was that retention would silently fail to delete and the records would come back on the next
/// start (<c>LoadExistingSegments</c> picks up whatever <c>.log</c> files it finds). These tests
/// were written to reproduce that and <b>could not</b>: with a borrowed read open across a
/// retention pass, the segment file is removed all the same. The chain needs a mapped view held by
/// a read of a <i>deletable</i> segment, and a read that starts in an already-rolled segment does
/// not borrow at all — it takes the multi-segment path, which combines into its own buffer.</para>
///
/// <para>So these pin the behaviour that actually holds: retention removes what it drops, and a
/// read in flight keeps serving valid bytes while it happens. The deferred-deletion retry behind
/// them (<c>PendingSegmentDeletions</c>, unit-tested separately) stays as defence in depth — a
/// deletion can fail for reasons that have nothing to do with leases (a scanner, another handle, a
/// storage plugin with its own mapping), and forgetting one is what makes deleted records
/// reappear.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class RetentionWithLiveLeaseTests : IDisposable
{
    private const string Topic = "retention-lease-topic";

    private readonly string _dataDir;

    public RetentionWithLiveLeaseTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-retention-lease-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task RetentionWhileABorrowedReadIsOpen_LeavesNoOrphanSegmentFile()
    {
        using var logManager = CreateLogManager();
        await logManager.CreateTopicAsync(Topic, partitionCount: 1,
            config: new Dictionary<string, string> { ["segment"] = "8192" });

        var tp = new TopicPartition { Topic = Topic, Partition = 0 };

        // Fill the first segment while it is still the ACTIVE one: only a read served from the
        // active segment borrows. A read starting in a rolled segment takes the multi-segment path
        // and holds nothing, which is why it could never pin a file in the first place.
        for (var i = 0; i < 4; i++)
            await logManager.AppendBatchAsync(tp, CreateRecordBatch(i, payloadBytes: 1024));

        var read = await logManager.ReadContiguousAsync(tp, startOffset: 0, maxBytes: 4096);
        Assert.NotEqual(0, read.Data.Length);
        Assert.NotNull(read.Lifetime);

        // The log moves on: the segment the reader is holding rolls away and becomes a deletion
        // candidate while that read is still open.
        for (var i = 4; i < 40; i++)
            await logManager.AppendBatchAsync(tp, CreateRecordBatch(i, payloadBytes: 1024));

        var log = Assert.IsType<PartitionLog>(logManager.GetLog(tp));
        Assert.True(log.Segments.Count > 1, "the topic did not roll — the test would delete nothing");

        // Retention does not wait for readers, and must not.
        logManager.ApplyRetentionPolicy();
        read.Dispose();

        // A second pass is what would collect anything that could not be deleted the first time.
        logManager.ApplyRetentionPolicy();

        var remainingFiles = Directory.GetFiles(_dataDir, "*.log", SearchOption.AllDirectories);
        Assert.True(remainingFiles.Length <= log.Segments.Count,
            $"{remainingFiles.Length} segment files are on disk but the partition only knows {log.Segments.Count} segments — a dropped segment's file survived and would be served again after a restart");
    }

    [Fact]
    public async Task ReadTakenBeforeRetention_KeepsServingItsBytes_WhileTheSegmentIsBeingDropped()
    {
        // The other side of the same moment: retention must not pull the memory out from under a
        // read that is already in flight. With a mapped view that would not be stale data — it
        // would be an access violation.
        using var logManager = CreateLogManager();
        await logManager.CreateTopicAsync(Topic, partitionCount: 1,
            config: new Dictionary<string, string> { ["segment"] = "8192" });

        var tp = new TopicPartition { Topic = Topic, Partition = 0 };
        for (var i = 0; i < 4; i++)
            await logManager.AppendBatchAsync(tp, CreateRecordBatch(i, payloadBytes: 1024));

        using var read = await logManager.ReadContiguousAsync(tp, startOffset: 0, maxBytes: 4096);
        Assert.NotNull(read.Lifetime);
        var expected = read.Data.ToArray();

        for (var i = 4; i < 40; i++)
            await logManager.AppendBatchAsync(tp, CreateRecordBatch(i, payloadBytes: 1024));

        logManager.ApplyRetentionPolicy();

        Assert.True(expected.AsSpan().SequenceEqual(read.Data.Span),
            "the record bytes changed while retention ran — the read's memory was recycled underneath it");
    }

    private LogManager CreateLogManager() => new(
        _dataDir,
        FileLogSegmentFactory.Create(useMmap: true),
        persistTopicsToFile: false,
        retentionPolicy: new RetentionPolicy { RetentionHours = -1, RetentionBytes = 4096, MinSegmentsToKeep = 1 });

    private static byte[] CreateRecordBatch(int index, int payloadBytes)
    {
        var batch = new byte[KafkaConstants.RecordBatch.HeaderSize + payloadBytes];
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), index);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);
        batch.AsSpan(KafkaConstants.RecordBatch.HeaderSize).Fill((byte)(index + 1));
        return batch;
    }
}
