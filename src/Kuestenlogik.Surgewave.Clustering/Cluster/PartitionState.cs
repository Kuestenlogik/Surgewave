using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Tracks the replication state of a partition across the cluster.
/// </summary>
public sealed class PartitionState
{
    public required TopicPartition TopicPartition { get; init; }

    /// <summary>
    /// Current leader broker ID. -1 if no leader.
    /// </summary>
    public int LeaderBrokerId { get; set; } = -1;

    /// <summary>
    /// Current leader epoch (increments on each election).
    /// </summary>
    public int LeaderEpoch { get; set; }

    /// <summary>
    /// All assigned replicas for this partition (ordered by preference).
    /// </summary>
    public List<int> Replicas { get; init; } = [];

    /// <summary>
    /// In-Sync Replicas - replicas that are caught up with the leader.
    /// </summary>
    public List<int> Isr { get; init; } = [];

    /// <summary>
    /// Offline replicas.
    /// </summary>
    public List<int> OfflineReplicas { get; init; } = [];

    /// <summary>
    /// The preferred leader (first in replica list).
    /// </summary>
    public int PreferredLeader => Replicas.Count > 0 ? Replicas[0] : -1;

    /// <summary>
    /// Whether leader election is needed.
    /// </summary>
    public bool NeedsElection => LeaderBrokerId == -1 || !Isr.Contains(LeaderBrokerId);

    /// <summary>
    /// Minimum ISR count required for writes (from topic config).
    /// </summary>
    public int MinInSyncReplicas { get; set; } = 1;

    /// <summary>
    /// Whether there are enough ISR for writes.
    /// </summary>
    public bool HasMinIsr => Isr.Count >= MinInSyncReplicas;

    /// <summary>
    /// High watermark - highest offset acknowledged by all ISR.
    /// </summary>
    public long HighWatermark { get; set; }

    /// <summary>
    /// Log start offset - earliest available offset.
    /// </summary>
    public long LogStartOffset { get; set; }
}
