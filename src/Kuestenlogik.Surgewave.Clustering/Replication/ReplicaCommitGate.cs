using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Replication;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Answers <see cref="IPartitionCommitGate"/> from the cluster's in-sync replica set.
/// </summary>
/// <remarks>
/// A partition with fewer in-sync replicas than its configured minimum cannot honour a durable
/// write, so the write is refused instead of being appended and reported as successful — which is
/// what happened before, for every producer that asked for acks=all.
/// </remarks>
public sealed class ReplicaCommitGate : IPartitionCommitGate
{
    private readonly ClusterState _clusterState;
    private readonly ReplicaManager? _replicaManager;

    public ReplicaCommitGate(ClusterState clusterState, ReplicaManager? replicaManager = null)
    {
        _clusterState = clusterState;
        _replicaManager = replicaManager;
    }

    public bool CanAdmitDurableWrite(in TopicPartition partition)
    {
        var state = _clusterState.GetPartitionState(partition);

        // No partition state means this broker is not replicating it — a single-node partition can
        // always commit what its own log accepts, and refusing here would break every unclustered
        // producer that asks for acks=all.
        if (state is null)
            return true;

        return state.Isr.Count >= state.MinInSyncReplicas;
    }

    public async ValueTask<bool> WaitForDurableCommitAsync(
        TopicPartition partition,
        long committedThroughOffset,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        // Without a replica manager this broker observes no follower progress, so there is nothing
        // to wait for and admission was the whole guarantee.
        if (_replicaManager is null)
            return true;

        var state = _clusterState.GetPartitionState(partition);

        // Not replicated here, or the leader is the only in-sync replica: the leader's own append
        // IS the commit. Waiting would block forever on a watermark that nothing else advances.
        if (state is null || state.Isr.Count <= 1)
            return true;

        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Register BEFORE re-reading the watermark. The other order has a race: the watermark can
        // pass our offset between the read and the registration, and nothing would ever complete
        // the registration afterwards — the producer would hang until its timeout for a write that
        // was already replicated.
        _replicaManager.RegisterPendingAck(partition, committedThroughOffset, tcs);

        if (_replicaManager.GetHighWatermark(partition) >= committedThroughOffset)
        {
            _replicaManager.CancelPendingAck(partition, committedThroughOffset, tcs);
            return true;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(timeout);

        try
        {
            return await tcs.Task.WaitAsync(cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Timed out, or the connection went away. Either way nobody will observe this
            // registration again, so drop it rather than leaving it for a watermark that — for the
            // partition whose followers just died — may never arrive.
            _replicaManager.CancelPendingAck(partition, committedThroughOffset, tcs);
            return false;
        }
    }
}
