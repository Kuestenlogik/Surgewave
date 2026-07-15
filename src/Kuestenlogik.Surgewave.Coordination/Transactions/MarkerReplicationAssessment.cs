using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Coordination.Transactions;

/// <summary>
/// #72 Inc6 — the single min.insync.replicas semantic shared by BOTH transaction-marker replication
/// transports (the native SRWV replicator and the Kafka-wire replicator). Previously each transport
/// carried its own placeholder "succeeded if ≥1 broker acked" check; this centralizes the one rule so
/// the assessment is identical on both wires.
/// <para>
/// <b>Log-only:</b> this reports which involved partitions are under-replicated for marker durability
/// (their in-sync replica set is below min.insync.replicas); it does NOT gate the marker result. Acting
/// on the shortfall (fencing/retrying the commit) is a later, sign-off-gated increment.
/// </para>
/// </summary>
public static class MarkerReplicationAssessment
{
    /// <summary>
    /// A partition is under-replicated for marker durability when its in-sync replica count is below
    /// its <c>min.insync.replicas</c>.
    /// </summary>
    public static bool IsUnderMinIsr(int isrCount, int minInSyncReplicas) => isrCount < minInSyncReplicas;

    /// <summary>
    /// Returns the involved partitions whose current in-sync replica set is below
    /// <c>min.insync.replicas</c>. Both transports pass the same neutral
    /// <c>(partition, isrCount, minInSyncReplicas)</c> triples, so they log one identical assessment.
    /// </summary>
    public static IReadOnlyList<TopicPartition> UnderMinIsr(
        IEnumerable<(TopicPartition Partition, int IsrCount, int MinInSyncReplicas)> partitions)
    {
        var under = new List<TopicPartition>();
        foreach (var (partition, isrCount, minInSyncReplicas) in partitions)
        {
            if (IsUnderMinIsr(isrCount, minInSyncReplicas))
                under.Add(partition);
        }

        return under;
    }
}
