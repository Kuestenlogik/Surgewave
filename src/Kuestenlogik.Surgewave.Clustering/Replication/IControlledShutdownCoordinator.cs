using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Controller-side half of a controlled shutdown (#180): moves partition leaderships off a broker
/// that is about to leave.
/// </summary>
/// <remarks>
/// Electing a leader is the controller's privilege, so a departing broker cannot do this for
/// itself — which is why a graceful shutdown on any other broker used to hand nothing over and the
/// cluster waited out the heartbeat timeout with a leader that had already gone.
/// </remarks>
public interface IControlledShutdownCoordinator
{
    /// <summary>Whether this broker is currently the controller.</summary>
    bool IsController { get; }

    /// <summary>
    /// Elects a new leader for every partition <paramref name="brokerId"/> currently leads, and
    /// drops it from the ISR of the rest. Returns the partitions that could NOT be moved — a
    /// partition whose ISR holds no other broker has no successor, and saying so is more useful
    /// than a bare failure.
    /// </summary>
    Task<IReadOnlyList<TopicPartition>> MoveLeadershipAwayAsync(int brokerId, CancellationToken ct = default);
}
