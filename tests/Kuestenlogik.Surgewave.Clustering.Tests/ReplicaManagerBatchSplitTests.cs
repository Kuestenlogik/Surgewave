using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// The intra-cluster follower ingest path (#92/#93): ReplicaManager.AppendAsync receives the
/// leader's concatenated records section and must split it into individual offset-preserving,
/// CRC-validated batches rather than appending the whole blob as one.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ReplicaManagerBatchSplitTests
{
    private const int BatchSize = 100;

    private static (ReplicaManager rm, LogManager lm) NewManager()
    {
        var config = new ClusteringConfig { BrokerId = 0, Host = "localhost", Port = 9092 };
        var state = new ClusterState();
        state.AddBroker(new BrokerNode { BrokerId = 0, Host = "localhost", Port = 9092 });
        var lm = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-rm-split-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        var rm = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, state, lm, config,
            new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());
        return (rm, lm);
    }

    [Fact]
    public async Task AppendAsync_MultiBatchSection_ReturnsTrueLeo_AndKeepsEachCrc()
    {
        var (rm, lm) = NewManager();
        var tp = new TopicPartition { Topic = "repl", Partition = 0 };
        var section = Concat(CreateValidBatch(0, 3), CreateValidBatch(3, 2));

        var leo = await rm.AppendAsync(tp, section, CancellationToken.None);

        Assert.Equal(5, leo); // #93: LEO past both batches, not 3
        var stored = await lm.GetLog(tp)!.ReadBatchesAsync(0);
        Assert.Equal(2, stored.Count);
        Assert.True(RecordBatchValidator.ValidateCrc(stored[0])); // #92
        Assert.True(RecordBatchValidator.ValidateCrc(stored[1]));
    }

    [Fact]
    public async Task AppendAsync_ReRun_NoDuplicates()
    {
        var (rm, lm) = NewManager();
        var tp = new TopicPartition { Topic = "repl-idem", Partition = 0 };
        var section = Concat(CreateValidBatch(0, 3), CreateValidBatch(3, 2));

        await rm.AppendAsync(tp, section, CancellationToken.None);
        var leo = await rm.AppendAsync(tp, section, CancellationToken.None); // leader re-sends

        Assert.Equal(5, leo);
        Assert.Equal(2, (await lm.GetLog(tp)!.ReadBatchesAsync(0)).Count); // no duplicate on disk
    }

    [Fact]
    public async Task AppendAsync_CorruptSecondBatch_CommitsGoodPrefixOnly()
    {
        var (rm, lm) = NewManager();
        var tp = new TopicPartition { Topic = "repl-corrupt", Partition = 0 };
        var b2 = CreateValidBatch(3, 2);
        b2[^1] ^= 0xFF;
        var section = Concat(CreateValidBatch(0, 3), b2);

        var leo = await rm.AppendAsync(tp, section, CancellationToken.None); // must not throw out

        Assert.Equal(3, leo); // only batch 1 committed
        var stored = await lm.GetLog(tp)!.ReadBatchesAsync(0);
        Assert.Single(stored);
        Assert.True(RecordBatchValidator.ValidateCrc(stored[0]));
    }

    private static byte[] Concat(params byte[][] parts)
    {
        var result = new byte[parts.Sum(p => p.Length)];
        var pos = 0;
        foreach (var p in parts) { p.CopyTo(result, pos); pos += p.Length; }
        return result;
    }

    private static byte[] CreateValidBatch(long baseOffset, int recordCount)
    {
        var batch = new byte[BatchSize];
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), baseOffset);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(12, 4), 0);
        batch[16] = 2;
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(21, 2), 0);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(23, 4), recordCount - 1);
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
