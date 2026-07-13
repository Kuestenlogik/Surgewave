using System.Net;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc7 — native transaction-marker replication: the leader-grouped native replicator sends a
/// commit/abort marker to the partition's leader over TCP, where the receiver appends the control
/// batch to the log and records it in the transaction index (via <see cref="ITransactionMarkerSink"/>),
/// plus the leadership guard and the transport gate.
/// </summary>
public class NativeTransactionMarkerTests
{
    internal sealed class StubMarkerSink : ITransactionMarkerSink
    {
        public readonly List<(long ProducerId, TopicPartition Tp, long Offset, bool Commit)> Calls = [];
        public void CommitTransaction(long producerId, IEnumerable<TopicPartition> partitions, long commitOffset)
        {
            foreach (var tp in partitions) Calls.Add((producerId, tp, commitOffset, true));
        }
        public void AbortTransaction(long producerId, IEnumerable<TopicPartition> partitions, long abortOffset)
        {
            foreach (var tp in partitions) Calls.Add((producerId, tp, abortOffset, false));
        }
    }

    private static ClusterStateInterBrokerService NewService(ClusterState state, int localBrokerId, LogManager logs, ITransactionMarkerSink sink)
    {
        var config = new ClusteringConfig { BrokerId = localBrokerId };
        var replicas = new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, logs, config, new TcpPeerTransport());
        return new ClusterStateInterBrokerService(
            NullLogger<ClusterStateInterBrokerService>.Instance, state, replicas, logs, localBrokerId, markerSink: sink);
    }

    private static LogManager NewLogs() => new(
        Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}"), new MemoryLogSegmentFactory());

    // ── Receiver leadership guard (direct) ───────────────────────────────────

    [Fact]
    public async Task ApplyWriteTxnMarkers_AsLeader_WritesMarkerAndRecordsInIndex()
    {
        var state = new ClusterState();
        var tp = new TopicPartition { Topic = "orders", Partition = 0 };
        state.TryApplyControllerPartitionState(tp, leaderId: 1, leaderEpoch: 3, replicas: [1, 2], isr: [1, 2]);
        var sink = new StubMarkerSink();
        var service = NewService(state, localBrokerId: 1, NewLogs(), sink);

        var status = await service.ApplyWriteTxnMarkersAsync(
            new WriteTxnMarkersRequestPayload("txn-1", ProducerId: 555, ProducerEpoch: 3, [tp], Commit: true, CoordinatorEpoch: 1));

        Assert.Equal(ClusterRpcStatus.None, status);
        var call = Assert.Single(sink.Calls);
        Assert.Equal((555L, tp, true), (call.ProducerId, call.Tp, call.Commit));
    }

    [Fact]
    public async Task ApplyWriteTxnMarkers_NotLeader_RepliesNotLeaderWithoutWriting()
    {
        var state = new ClusterState();
        var tp = new TopicPartition { Topic = "orders", Partition = 0 };
        state.TryApplyControllerPartitionState(tp, leaderId: 2, leaderEpoch: 3, replicas: [2, 1], isr: [2, 1]); // led by broker 2
        var sink = new StubMarkerSink();
        var service = NewService(state, localBrokerId: 1, NewLogs(), sink); // this broker is 1, not the leader

        var status = await service.ApplyWriteTxnMarkersAsync(
            new WriteTxnMarkersRequestPayload("txn-1", 555, 3, [tp], Commit: false, CoordinatorEpoch: 1));

        Assert.Equal(ClusterRpcStatus.NotLeaderForPartition, status);
        Assert.Empty(sink.Calls);
    }

    [Fact]
    public async Task ApplyWriteTxnMarkers_MultiPartitionAllLed_WritesAll()
    {
        var state = new ClusterState();
        var a = new TopicPartition { Topic = "orders", Partition = 0 };
        var b = new TopicPartition { Topic = "orders", Partition = 1 };
        state.TryApplyControllerPartitionState(a, leaderId: 1, leaderEpoch: 3, replicas: [1, 2], isr: [1, 2]);
        state.TryApplyControllerPartitionState(b, leaderId: 1, leaderEpoch: 3, replicas: [1, 2], isr: [1, 2]);
        var sink = new StubMarkerSink();
        var service = NewService(state, localBrokerId: 1, NewLogs(), sink);

        var status = await service.ApplyWriteTxnMarkersAsync(
            new WriteTxnMarkersRequestPayload("txn-1", 555, 3, [a, b], Commit: true, CoordinatorEpoch: 1));

        Assert.Equal(ClusterRpcStatus.None, status);
        Assert.Equal(2, sink.Calls.Count);
    }

    [Fact]
    public async Task ApplyWriteTxnMarkers_MultiPartitionOneNotLed_RejectsWholeFrameWithoutWriting()
    {
        var state = new ClusterState();
        var a = new TopicPartition { Topic = "orders", Partition = 0 };
        var b = new TopicPartition { Topic = "orders", Partition = 1 };
        state.TryApplyControllerPartitionState(a, leaderId: 1, leaderEpoch: 3, replicas: [1, 2], isr: [1, 2]); // led by us
        state.TryApplyControllerPartitionState(b, leaderId: 2, leaderEpoch: 3, replicas: [2, 1], isr: [2, 1]); // led by broker 2
        var sink = new StubMarkerSink();
        var service = NewService(state, localBrokerId: 1, NewLogs(), sink);

        var status = await service.ApplyWriteTxnMarkersAsync(
            new WriteTxnMarkersRequestPayload("txn-1", 555, 3, [a, b], Commit: true, CoordinatorEpoch: 1));

        // All-or-nothing: the frame is rejected atomically — NOT a partial write of the led partition A.
        Assert.Equal(ClusterRpcStatus.NotLeaderForPartition, status);
        Assert.Empty(sink.Calls);
    }

    // ── Transport gate ───────────────────────────────────────────────────────

    private sealed class RecordingReplicator : ITransactionMarkerReplicator
    {
        public int Calls;
        public Task<MarkerReplicationResult> ReplicateMarkersAsync(string t, long p, short e, IReadOnlyList<TopicPartition> parts, bool c, int ce, CancellationToken ct)
        { Calls++; return Task.FromResult(MarkerReplicationResult.Success()); }
    }

    [Fact]
    public async Task Gate_AllNative_RoutesToNativeReplicator()
    {
        var state = new ClusterState();
        state.AddBroker(new BrokerNode { BrokerId = 1, Host = "h", Port = 9092, InterBrokerProtocol = InterBrokerProtocolFeature.Native });
        var (native, fallback) = (new RecordingReplicator(), new RecordingReplicator());
        var gate = new GatedTransactionMarkerReplicator(state, native, fallback, NullLogger<GatedTransactionMarkerReplicator>.Instance);

        await gate.ReplicateMarkersAsync("t", 1, 1, [], true, 1, CancellationToken.None);

        Assert.Equal(1, native.Calls);
        Assert.Equal(0, fallback.Calls);
    }

    [Fact]
    public async Task Gate_OneKafkaWirePeer_RoutesToFallback()
    {
        var state = new ClusterState();
        state.AddBroker(new BrokerNode { BrokerId = 1, Host = "h", Port = 9092, InterBrokerProtocol = InterBrokerProtocolFeature.Native });
        state.AddBroker(new BrokerNode { BrokerId = 2, Host = "h", Port = 9093, InterBrokerProtocol = InterBrokerProtocolFeature.KafkaWire });
        var (native, fallback) = (new RecordingReplicator(), new RecordingReplicator());
        var gate = new GatedTransactionMarkerReplicator(state, native, fallback, NullLogger<GatedTransactionMarkerReplicator>.Instance);

        await gate.ReplicateMarkersAsync("t", 1, 1, [], true, 1, CancellationToken.None);

        Assert.Equal(0, native.Calls);
        Assert.Equal(1, fallback.Calls);
    }

    [Fact]
    public async Task Gate_KafkaWirePinned_NoFallback_ReportsFailure()
    {
        var state = new ClusterState(); // empty → finalized KafkaWire
        var gate = new GatedTransactionMarkerReplicator(state, new RecordingReplicator(), kafkaWireFallback: null, NullLogger<GatedTransactionMarkerReplicator>.Instance);

        var result = await gate.ReplicateMarkersAsync("t", 1, 1, [new TopicPartition { Topic = "x", Partition = 0 }], true, 1, CancellationToken.None);

        Assert.False(result.IsSuccess);
    }

    // ── End-to-end over TCP loopback ─────────────────────────────────────────

    [Fact]
    public async Task NativeReplicate_OverTcp_LeaderWritesMarkerAndRecordsIt()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var transport = new TcpPeerTransport();
        var tp = new TopicPartition { Topic = "orders", Partition = 0 };

        // Leader (broker 1): applies the marker, behind a TCP listener.
        var leaderState = new ClusterState();
        leaderState.TryApplyControllerPartitionState(tp, leaderId: 1, leaderEpoch: 3, replicas: [1, 2], isr: [1, 2]);
        var leaderLogs = NewLogs();
        var sink = new StubMarkerSink();
        var leaderReplicas = new ReplicaManager(NullLogger<ReplicaManager>.Instance, leaderState, leaderLogs, new ClusteringConfig { BrokerId = 1 }, transport);
        var server = new NativeInterBrokerServer(
            NullLogger<NativeInterBrokerServer>.Instance,
            new ClusterStateInterBrokerService(NullLogger<ClusterStateInterBrokerService>.Instance, leaderState, leaderReplicas, leaderLogs, localBrokerId: 1, markerSink: sink));

        await using var listener = transport.CreateListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync();
        var serverTask = Task.Run(async () =>
        {
            await using var conn = await listener.AcceptAsync(cts.Token);
            await using var lease = await conn.AcceptInboundStreamAsync(cts.Token);
            while (await server.HandleSingleAsync(lease.Stream, cts.Token)) { }
        }, cts.Token);

        // Sender (broker 2): the partition is led by broker 1 at the listener's replication port.
        var senderState = new ClusterState();
        senderState.TryApplyControllerPartitionState(tp, leaderId: 1, leaderEpoch: 3, replicas: [1, 2], isr: [1, 2]);
        senderState.AddBroker(new BrokerNode { BrokerId = 1, Host = "127.0.0.1", Port = 9092, ReplicationPort = listener.LocalEndPoint.Port });
        using var pool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, transport);
        var replicator = new NativeTransactionMarkerReplicator(pool, senderState, localBrokerId: 2, NullLogger<NativeTransactionMarkerReplicator>.Instance);

        var result = await replicator.ReplicateMarkersAsync("txn-1", producerId: 777, producerEpoch: 4, [tp], commit: true, coordinatorEpoch: 2, cts.Token);

        Assert.True(result.IsSuccess);
        Assert.Contains(1, result.SuccessfulBrokers);
        var call = Assert.Single(sink.Calls);
        Assert.Equal((777L, tp, true), (call.ProducerId, call.Tp, call.Commit));

        cts.Cancel();
        try { await serverTask; } catch (OperationCanceledException) { }
        await leaderReplicas.DisposeAsync();
    }
}
