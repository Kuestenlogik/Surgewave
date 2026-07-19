using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Clustering;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Benchmarks.Unit;

/// <summary>
/// #82: the follower-ingest split-append. The leader packs N complete record batches behind one
/// records-length prefix; <see cref="ReplicaManager.AppendAsync(TopicPartition, ReadOnlyMemory{byte}, CancellationToken)"/>
/// splits the section and appends each batch offset-preserving — slicing into the (pooled) fetch body
/// rather than copying each partition's records out (S4). This benchmarks that split over a whole
/// multi-batch section so the byte-exact allocation gate catches a reintroduced per-batch/per-section copy.
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[BenchmarkCategory("Unit", "Replication")]
public class ReplicationAppendBenchmarks
{
    private const int BatchBytes = 512;
    private const int BatchCount = 8;

    private LogManager _lm = null!;
    private ReplicaManager _rm = null!;
    private TopicPartition _tp;
    private byte[] _section = null!;
    private ReadOnlyMemory<byte> _slice;

    [GlobalSetup]
    public void Setup()
    {
        var config = new ClusteringConfig { BrokerId = 0, Host = "localhost", Port = 9092 };
        var state = new ClusterState();
        state.AddBroker(new BrokerNode { BrokerId = 0, Host = "localhost", Port = 9092 });
        _lm = new LogManager(
            Path.Combine(Path.GetTempPath(), $"sw-bench-append-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        _rm = new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, _lm, config, new TcpPeerTransport());
        _tp = new TopicPartition { Topic = "repl-bench", Partition = 0 };

        // A section of BatchCount CRC-valid batches at base offsets 0..BatchCount-1.
        _section = new byte[BatchCount * BatchBytes];
        for (var i = 0; i < BatchCount; i++)
            CreateBatch(i, BatchBytes).CopyTo(_section, i * BatchBytes);

        // Present the ReadOnlyMemory overload with a NON-ZERO-offset slice, exercising the
        // segment.Offset + cursor arithmetic the byte[] overload (offset 0) never hits.
        var backing = new byte[_section.Length + 16];
        _section.CopyTo(backing, 8);
        _slice = new ReadOnlyMemory<byte>(backing, 8, _section.Length);

        // Append the section once so NextOffset advances past all batches. Every benchmarked call then
        // re-appends the SAME batches and takes the idempotent-skip path (baseOffset < NextOffset): it
        // still scans the whole section boundary-by-boundary, but writes nothing, so there is no log
        // growth across BenchmarkDotNet's invocation count. This is exactly the follower's re-fetch path
        // after a leader resend, and isolates the split/boundary-scan allocation.
        _rm.AppendAsync(_tp, _section, CancellationToken.None).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _rm.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _lm.Dispose();
    }

    [Benchmark(Baseline = true)]
    public Task<long> Split_ByteArray() => _rm.AppendAsync(_tp, _section, CancellationToken.None);

    [Benchmark]
    public Task<long> Split_ReadOnlyMemory() => _rm.AppendAsync(_tp, _slice, CancellationToken.None);

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
