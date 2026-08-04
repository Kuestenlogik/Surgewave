using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Creating a topic must never replace a partition log that already holds records.
///
/// <para><b>Where this bites.</b> A follower's replication brings a partition log into being
/// through <see cref="LogManager.GetOrCreateLog"/> and registers no topic metadata — nothing on the
/// replication path does. So a broker can hold every record of a partition while
/// <see cref="LogManager.GetTopicMetadata"/> still answers <see langword="null"/> for its topic.
/// Promote that broker and let any client ask it for metadata: with auto-create enabled the
/// metadata handler creates the topic on the spot, and creating a topic used to overwrite the
/// partition-log entry with a fresh empty one. The records were not deleted — they were simply
/// unreachable, the partition came back empty, and nothing logged an error (#97).</para>
///
/// <para>The end-to-end failover test reaches that state only when metadata propagation loses a
/// race, so the invariant is pinned here instead, where it is deterministic.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class CreateTopicPreservesExistingLogTests : IDisposable
{
    private const string Topic = "replica-before-metadata";

    private readonly string _dataDir;
    private readonly LogManager _logManager;

    public CreateTopicPreservesExistingLogTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-createtopic-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task CreateTopic_WhenAPartitionLogAlreadyHoldsRecords_KeepsThatLog()
    {
        var tp = new TopicPartition { Topic = Topic, Partition = 0 };

        // Exactly what replication does: create the log directly, write to it, register no metadata.
        var replicaLog = _logManager.GetOrCreateLog(tp);
        for (var i = 0; i < 5; i++)
            await replicaLog.AppendBatchAsync(CreateRecordBatch(i));

        Assert.Equal(5, replicaLog.NextOffset);
        Assert.Null(_logManager.GetTopicMetadata(Topic));

        // What a metadata request with auto-create does next.
        await _logManager.CreateTopicAsync(Topic, partitionCount: 1);

        var afterCreate = _logManager.GetLog(tp);
        Assert.NotNull(afterCreate);
        Assert.Same(replicaLog, afterCreate);
        Assert.Equal(5, afterCreate!.NextOffset);

        // And the records are still readable, not just counted.
        var batches = await _logManager.ReadBatchesAsync(tp, 0, maxBytes: 1024 * 1024);
        Assert.Equal(5, batches.Count);
    }

    [Fact]
    public async Task CreateTopic_ForPartitionsThatDoNotExistYet_StillCreatesThem()
    {
        // The guard must not turn into "never create anything": only partition 0 exists here, and
        // the other two still have to appear.
        var existing = new TopicPartition { Topic = Topic, Partition = 0 };
        var replicaLog = _logManager.GetOrCreateLog(existing);
        await replicaLog.AppendBatchAsync(CreateRecordBatch(0));

        await _logManager.CreateTopicAsync(Topic, partitionCount: 3);

        Assert.Same(replicaLog, _logManager.GetLog(existing));
        Assert.NotNull(_logManager.GetLog(new TopicPartition { Topic = Topic, Partition = 1 }));
        Assert.NotNull(_logManager.GetLog(new TopicPartition { Topic = Topic, Partition = 2 }));
    }

    private static byte[] CreateRecordBatch(int index)
    {
        var batch = new byte[KafkaConstants.RecordBatch.HeaderSize + 16];
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), index);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);
        batch.AsSpan(KafkaConstants.RecordBatch.HeaderSize).Fill((byte)(index + 1));
        return batch;
    }
}
