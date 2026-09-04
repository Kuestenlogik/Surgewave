using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Sending half of a controlled shutdown (#180): a departing broker asking the controller to take
/// its partition leaderships away.
/// </summary>
/// <remarks>
/// The counterpart of <see cref="IControlledShutdownCoordinator"/>, kept as its own interface so
/// the controller does not depend on a transport and the shutdown path can be exercised without
/// one.
/// </remarks>
public interface IControlledShutdownRequester
{
    /// <summary>
    /// Asks the controller to move every partition <paramref name="brokerId"/> leads. Returns the
    /// partitions it still leads afterwards — empty when all of them moved — or
    /// <see langword="null"/> when the controller could not be reached or refused, in which case
    /// the caller has nothing better than the heartbeat timeout to fall back on.
    /// </summary>
    Task<IReadOnlyList<TopicPartition>?> RequestControlledShutdownAsync(
        int brokerId, long brokerEpoch, CancellationToken ct = default);
}
