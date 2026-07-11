using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Coordination.Transactions;

/// <summary>
/// Protocol-neutral inter-broker transaction-marker replication surface (#59 b5). The
/// cluster-aware transaction coordinator replicates commit/abort markers to follower
/// brokers through this seam instead of naming the Kafka-wire <c>TransactionMarkerReplicator</c>
/// directly; the concrete WriteTxnMarkers codec implementation lives in the Kafka plugin.
/// Arguments are neutral scalars/records so no broker-internal transaction type is exposed.
/// </summary>
public interface ITransactionMarkerReplicator
{
    /// <summary>
    /// Replicates transaction markers (commit/abort) to all follower brokers that host
    /// replicas for the partitions involved in the transaction.
    /// </summary>
    Task<MarkerReplicationResult> ReplicateMarkersAsync(
        string transactionalId,
        long producerId,
        short producerEpoch,
        IReadOnlyList<TopicPartition> partitions,
        bool commit,
        int coordinatorEpoch,
        CancellationToken cancellationToken);
}

/// <summary>
/// Result of replicating transaction markers to the follower brokers.
/// </summary>
public sealed class MarkerReplicationResult
{
    public bool IsSuccess { get; set; }
    public HashSet<int> SuccessfulBrokers { get; } = [];
    public Dictionary<int, string> FailedBrokers { get; } = [];

    public static MarkerReplicationResult Success() => new() { IsSuccess = true };
}
