using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// Places a partition in a given shape for tests that need one to exist.
/// </summary>
/// <remarks>
/// These call sites used <c>ClusterState.TryApplyControllerPartitionState</c>, which was the
/// receiving half of a controller push and went with it (#163 step 3). They never depended on the
/// push — they wanted a partition with a leader, a replica set and an ISR — so this states that
/// directly through the primitives the cluster still has.
/// </remarks>
internal static class PartitionStateSetup
{
    public static void Place(
        ClusterState state,
        TopicPartition tp,
        int leaderId,
        IReadOnlyList<int> replicas,
        IReadOnlyList<int> isr)
    {
        state.AssignReplicas(tp, [.. replicas]);

        // AssignReplicas elects the first replica when there is no leader yet; anything else has
        // to be said explicitly. A negative id means "no leader", which is a state a test may want.
        if (leaderId >= 0)
            state.ElectLeader(tp, leaderId);
        else
            state.GetPartitionState(tp)!.LeaderBrokerId = -1;

        state.UpdateIsr(tp, [.. isr]);
    }
}
