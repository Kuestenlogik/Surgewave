using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Leader-side hook fired when a partition leader's in-sync replica set actually
/// changes, so the change can be propagated back to the controller (reverse ISR
/// propagation, #69). The controller applies the update to its authoritative
/// <see cref="Cluster.ClusterState"/> — which serves Kafka Metadata — and
/// re-broadcasts LeaderAndIsr so every broker converges.
/// <para>
/// Implementations must be non-blocking-friendly: they are invoked from the
/// replication fetch path (via a background task) and must tolerate an
/// unreachable controller without throwing.
/// </para>
/// </summary>
public interface IIsrChangeNotifier
{
    /// <summary>
    /// Report that partition <paramref name="tp"/>, led by
    /// <paramref name="leaderId"/> at <paramref name="leaderEpoch"/>, now has
    /// the given in-sync replica set.
    /// </summary>
    Task NotifyIsrChangedAsync(
        TopicPartition tp,
        int leaderId,
        int leaderEpoch,
        IReadOnlyList<int> isr,
        CancellationToken ct = default);
}
