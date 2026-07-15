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

    /// <summary>
    /// #72 Inc6 — per-partition outcome for every involved partition, so a partition that was
    /// SKIPPED (no elected leader, or unknown to cluster state) is VISIBLE instead of silently
    /// dropped or folded into a blanket success. Observability only — this does not gate
    /// <see cref="IsSuccess"/> (acting on it is a later, sign-off-gated increment).
    /// </summary>
    public Dictionary<TopicPartition, MarkerPartitionOutcome> PartitionOutcomes { get; } = [];

    public static MarkerReplicationResult Success() => new() { IsSuccess = true };
}

/// <summary>
/// #72 Inc6 — the outcome of transaction-marker replication for a single involved partition.
/// </summary>
public enum MarkerPartitionOutcome
{
    /// <summary>The marker was dispatched to the partition's remote target (leader or followers).</summary>
    Replicated,

    /// <summary>This broker leads the partition; the marker was written locally by the coordinator.</summary>
    LocalLeader,

    /// <summary>Skipped: the partition has no elected leader (LeaderBrokerId &lt; 0).</summary>
    SkippedNoLeader,

    /// <summary>Skipped: the partition is unknown to cluster state (no PartitionState).</summary>
    SkippedUnknownPartition,

    /// <summary>The remote target for this partition rejected the marker or failed.</summary>
    Failed,
}
