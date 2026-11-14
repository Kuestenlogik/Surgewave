using Kuestenlogik.Surgewave.Broker.Abstractions.Routing;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Reads partition leadership from <see cref="ClusterState"/> (#164).
/// </summary>
/// <remarks>
/// Wired only where clustering is. A deployment without it supplies no
/// <see cref="IPartitionLeadership"/> at all, and the produce path treats that as "no
/// opinion" rather than as "not the leader" — which is what keeps single-broker and
/// embedded runtimes working unchanged.
/// </remarks>
public sealed class ClusterStatePartitionLeadership : IPartitionLeadership
{
    private readonly ClusterState _clusterState;
    private readonly int _localBrokerId;

    public ClusterStatePartitionLeadership(ClusterState clusterState, int localBrokerId)
    {
        _clusterState = clusterState;
        _localBrokerId = localBrokerId;
    }

    /// <inheritdoc />
    public bool IsNotLedHere(TopicPartition partition)
    {
        var state = _clusterState.GetPartitionState(partition);

        // No state: this partition was never touched by a clustering path, so nothing
        // here knows who leads it. Not an answer of "someone else does".
        if (state is null) return false;

        // -1 is "no leader" (#253): the partition's last in-sync replica is gone. Nobody appends to it or serves it,
        // least of all a replica that is not in sync; the client is sent back to the metadata, which says so.
        return state.LeaderBrokerId != _localBrokerId;
    }

    /// <inheritdoc />
    public PartitionLeader? LeaderOf(TopicPartition partition)
    {
        var state = _clusterState.GetPartitionState(partition);
        return state is null || state.LeaderBrokerId < 0 ? null : new PartitionLeader(state.LeaderBrokerId, state.LeaderEpoch);
    }
}
