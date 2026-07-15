using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #72 Inc6 — transaction-marker replication records per-partition OUTCOMES, so a no-leader /
/// unknown-partition skip is visible instead of silently dropped or folded into a blanket success, and
/// both transports share ONE min.insync.replicas assessment (log-only — it does not gate the result).
/// The native-side cases here exercise the classification without touching the network (every partition
/// is skipped or local-led, so nothing is sent).
/// </summary>
public class MarkerReplicationOutcomeTests
{
    private static readonly TopicPartition P0 = new() { Topic = "orders", Partition = 0 };

    private static NativeTransactionMarkerReplicator NewReplicator(ClusterState state, int localBrokerId, ConnectionPool pool)
        => new(pool, state, localBrokerId, NullLogger<NativeTransactionMarkerReplicator>.Instance);

    [Fact]
    public async Task Native_NoLeaderPartition_IsRecordedAsSkippedNoLeader_AndSuccessUnchanged()
    {
        var state = new ClusterState();
        state.TryApplyControllerPartitionState(P0, leaderId: -1, leaderEpoch: 0, replicas: [1, 2], isr: [1, 2]);
        using var pool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, new TcpPeerTransport());
        var replicator = NewReplicator(state, localBrokerId: 3, pool);

        var result = await replicator.ReplicateMarkersAsync("txn-1", 1, 1, [P0], commit: true, coordinatorEpoch: 1, CancellationToken.None);

        // The skip is now VISIBLE …
        Assert.Equal(MarkerPartitionOutcome.SkippedNoLeader, result.PartitionOutcomes[P0]);
        // … while the success contract is intentionally preserved (log-only; gating is a later increment).
        Assert.True(result.IsSuccess);
        Assert.Empty(result.SuccessfulBrokers);
        Assert.Empty(result.FailedBrokers);
    }

    [Fact]
    public async Task Native_UnknownPartition_IsRecordedAsSkippedUnknownPartition()
    {
        var state = new ClusterState(); // P0 was never added to cluster state
        using var pool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, new TcpPeerTransport());
        var replicator = NewReplicator(state, localBrokerId: 3, pool);

        var result = await replicator.ReplicateMarkersAsync("txn-1", 1, 1, [P0], commit: false, coordinatorEpoch: 1, CancellationToken.None);

        Assert.Equal(MarkerPartitionOutcome.SkippedUnknownPartition, result.PartitionOutcomes[P0]);
        Assert.True(result.IsSuccess);
    }

    [Fact]
    public async Task Native_LocalLeaderPartition_IsRecordedAsLocalLeader()
    {
        var state = new ClusterState();
        state.TryApplyControllerPartitionState(P0, leaderId: 3, leaderEpoch: 1, replicas: [3, 1], isr: [3, 1]);
        using var pool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, new TcpPeerTransport());
        var replicator = NewReplicator(state, localBrokerId: 3, pool); // this broker leads P0 → written locally

        var result = await replicator.ReplicateMarkersAsync("txn-1", 1, 1, [P0], commit: true, coordinatorEpoch: 1, CancellationToken.None);

        Assert.Equal(MarkerPartitionOutcome.LocalLeader, result.PartitionOutcomes[P0]);
        Assert.True(result.IsSuccess);
    }

    [Fact]
    public void UnderMinIsr_ReturnsOnlyThePartitionsBelowTheirMinInSyncReplicas()
    {
        var a = new TopicPartition { Topic = "t", Partition = 0 }; // isr 1 < minIsr 2 → under
        var b = new TopicPartition { Topic = "t", Partition = 1 }; // isr 2 >= minIsr 2 → ok
        var c = new TopicPartition { Topic = "t", Partition = 2 }; // isr 0 < minIsr 1 → under

        var under = MarkerReplicationAssessment.UnderMinIsr(
        [
            (a, 1, 2),
            (b, 2, 2),
            (c, 0, 1),
        ]);

        Assert.Equal(new[] { a, c }, under);
        Assert.False(MarkerReplicationAssessment.IsUnderMinIsr(isrCount: 2, minInSyncReplicas: 2));
        Assert.True(MarkerReplicationAssessment.IsUnderMinIsr(isrCount: 1, minInSyncReplicas: 2));
    }
}
