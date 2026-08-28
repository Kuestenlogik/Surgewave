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
    public bool IsLedByAnotherBroker(TopicPartition partition)
    {
        var state = _clusterState.GetPartitionState(partition);

        // No state: this partition was never touched by a clustering path, so nothing
        // here knows who leads it. Not an answer of "someone else does".
        if (state is null) return false;

        // -1 is "no leader", which is a partition mid-election rather than one led
        // elsewhere. Refusing there would turn every election into a client-visible
        // error even for the broker that is about to win it.
        if (state.LeaderBrokerId < 0) return false;

        return state.LeaderBrokerId != _localBrokerId;
    }
}
