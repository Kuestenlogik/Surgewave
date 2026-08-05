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

    public ReplicaCommitGate(ClusterState clusterState)
    {
        _clusterState = clusterState;
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
}
