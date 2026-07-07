using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Controller-side counterpart of <see cref="IIsrChangeNotifier"/> (#69). Applies
/// an ISR update reported by a partition leader (via AlterPartition) to the
/// controller's authoritative <see cref="ClusterState"/> and re-broadcasts
/// LeaderAndIsr so the whole cluster — and the Kafka Metadata the controller
/// serves — converges to the new ISR.
/// </summary>
public interface IIsrUpdateApplier
{
    /// <summary>Whether this broker is currently the controller.</summary>
    bool IsController { get; }

    /// <summary>
    /// Apply the ISR reported for partition <paramref name="tp"/> by leader
    /// <paramref name="leaderId"/> at <paramref name="leaderEpoch"/>. Returns the
    /// updated <see cref="PartitionState"/>, or <c>null</c> if this broker is not
    /// the controller or the partition is unknown. A stale leader epoch is
    /// rejected (the current state is returned unchanged).
    /// </summary>
    Task<PartitionState?> ApplyIsrUpdateAsync(
        TopicPartition tp,
        int leaderId,
        int leaderEpoch,
        IReadOnlyList<int> newIsr,
        CancellationToken ct = default);
}
