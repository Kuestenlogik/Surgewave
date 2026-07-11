using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Neutral controller-to-replica inter-broker RPC surface (#59 b5). The controller
/// uses this to broadcast LeaderAndIsr / UpdateMetadata / StopReplica when partition
/// topology changes, and (via <see cref="IIsrChangeNotifier"/>) to report reverse ISR
/// changes back to the controller. All three send operations return a bare
/// <see cref="Task"/>: every caller drives them fire-and-forget (best-effort) and
/// discards any per-broker response, so no Kafka DTO is exposed here. The concrete
/// wire implementation lives in <c>ControllerClient</c> (in the Kafka plugin, #59 b5).
/// </summary>
public interface IControllerReplicaRpc : IIsrChangeNotifier
{
    /// <summary>
    /// Send LeaderAndIsr to every affected broker, one request per broker.
    /// </summary>
    Task SendLeaderAndIsrAsync(
        IEnumerable<(TopicPartition Tp, PartitionState State)> partitionChanges,
        CancellationToken ct = default);

    /// <summary>
    /// Send UpdateMetadata to all brokers (all partitions if <paramref name="partitionStates"/> is null).
    /// </summary>
    Task SendUpdateMetadataAsync(
        IEnumerable<(TopicPartition Tp, PartitionState State)>? partitionStates = null,
        CancellationToken ct = default);

    /// <summary>
    /// Send StopReplica to a specific broker.
    /// </summary>
    Task SendStopReplicaAsync(
        int brokerId,
        IEnumerable<(TopicPartition Tp, int LeaderEpoch, bool DeletePartition)> partitions,
        CancellationToken ct = default);
}
