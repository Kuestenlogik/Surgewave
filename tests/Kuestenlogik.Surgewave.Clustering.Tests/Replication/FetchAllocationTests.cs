using System.Buffers.Binary;
using System.Linq;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests.Replication;

/// <summary>
/// #82 allocation proof. Drives a real fetch RPC from a follower <see cref="LeaderConnection"/> against a
/// loopback <see cref="ReplicationServer"/> and asserts, via a process-wide GC-allocation delta, that a
/// ~2&#160;MB fetch frame is served and received without any MB-scale copy or LOH/Gen2 churn — i.e. the
/// pooled path is intact: leader response frame (S1), follower response body (S2), leader contiguous read
/// (S3), and the follower's per-partition record slice into the pooled body (S4). A reintroduced
/// <c>ToArray</c>/copy would allocate ~2&#160;MB per RPC and fail these ceilings.
///
/// Deterministic because the pooled path makes allocation byte-stable and the test project runs
/// non-parallel (<c>xunit.runner.json: parallelizeTestCollections=false</c>), so the process-wide
/// allocation delta over the measured window is attributable only to the leader-serve + follower-fetch
/// work. Ceilings are deliberately loose so the assertion never flakes while still catching an MB-scale
/// regression.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class FetchAllocationTests
{
    private const int BatchBytes = 64 * 1024;
    private const int BatchCount = 32; // ~2 MB fetch frame

    [Fact]
    public async Task Fetch_OverLoopback_PooledPath_NoMbScaleAllocationPerRpc()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var tp = new TopicPartition { Topic = "repl", Partition = 0 };
        var transport = new TcpPeerTransport();

        // ---- Leader: seed ~2 MB of batches into one memory segment, mark leader, start on an ephemeral port.
        var state = new ClusterState();
        state.AddBroker(new BrokerNode { BrokerId = 0, Host = "127.0.0.1", Port = 9092 });
        var cfg = new ClusteringConfig { BrokerId = 0, Host = "127.0.0.1", Port = 9092, ReplicationPort = 0 };
        var logs = new LogManager(
            Path.Combine(Path.GetTempPath(), $"sw-fetchalloc-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        var log = logs.GetOrCreateLog(tp);
        for (var i = 0; i < BatchCount; i++)
            await log.AppendBatchAsync(CreateBatch(i, BatchBytes));

        await using var replicas = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, state, logs, cfg, transport);
        await replicas.BecomeLeaderAsync(tp, leaderEpoch: 0, cts.Token);

        await using var server = new ReplicationServer(
            NullLogger<ReplicationServer>.Instance, state, logs, replicas, cfg, transport);
        await server.StartAsync(cts.Token);
        var port = server.BoundEndPoint!.Port;

        // ---- Follower: connect an internal LeaderConnection straight at the bound port.
        var broker = new BrokerNode { BrokerId = 0, Host = "127.0.0.1", Port = 9092, ReplicationPort = port };
        await using var conn = new LeaderConnection(broker, transport, NullLogger.Instance);
        await conn.ConnectAsync(cts.Token);

        static ReplicaFetchRequest Req(TopicPartition tp) => new()
        {
            ReplicaId = 1,
            MaxWaitMs = 0,
            MinBytes = 1,
            MaxBytes = 8 << 20,
            IsolationLevel = 0,
            Topics = [ new() { Topic = tp.Topic, Partitions = [ new() { Partition = tp.Partition, FetchOffset = 0, PartitionMaxBytes = 8 << 20 } ] } ],
        };

        // Warm up JIT + both ArrayPools (the first RPCs rent fresh arrays), and confirm the fetch really
        // transfers ~2 MB — otherwise the allocation assertion below would be meaningless.
        var transferred = 0;
        for (var i = 0; i < 5; i++)
        {
            using var w = await conn.SendFetchRequestAsync(Req(tp), cts.Token);
            transferred = w.Topics.Sum(t => t.Partitions.Sum(p => p.RecordBatch.Length));
        }
        Assert.True(transferred > BatchCount * BatchBytes / 2, $"fetch returned only {transferred} bytes; frame is not ~2 MB");

        const int n = 30;
        var gen2Before = GC.CollectionCount(2);
        var allocBefore = GC.GetTotalAllocatedBytes(precise: true);
        for (var i = 0; i < n; i++)
        {
            using var r = await conn.SendFetchRequestAsync(Req(tp), cts.Token);
        }
        var allocPerFetch = (GC.GetTotalAllocatedBytes(precise: true) - allocBefore) / n;
        var gen2 = GC.CollectionCount(2) - gen2Before;

        // Measured baseline: ~5.5 KB/fetch and 0 Gen2 for a 2 MB frame (all pooled). A reintroduced
        // per-frame/body/partition copy would add ~2 MB/fetch — an ~8x jump past this ceiling plus Gen2 churn.
        Assert.True(gen2 <= 1, $"expected no Gen2/LOH churn from the pooled fetch path, saw {gen2} collection(s)");
        Assert.True(allocPerFetch < 256 * 1024,
            $"alloc/fetch={allocPerFetch} B for a ~2 MB frame — a reintroduced per-frame/body/partition copy would land here");
    }

    /// <summary>A CRC-valid v2 record batch of the requested total size (payload is zero-filled).</summary>
    private static byte[] CreateBatch(long baseOffset, int totalBytes)
    {
        var batch = new byte[totalBytes];
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), baseOffset);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(12, 4), 0);
        batch[16] = 2; // magic
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(21, 2), 0);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(23, 4), 0); // lastOffsetDelta (recordCount - 1)
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(27, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(35, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(43, 8), -1);
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(51, 2), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(53, 4), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1); // recordsCount
        var crc = Crc32C.Compute(batch.AsSpan(21));
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), crc);
        return batch;
    }
}
