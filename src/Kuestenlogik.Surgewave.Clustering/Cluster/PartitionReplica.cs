using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Represents the replica state for a single partition.
/// </summary>
public sealed class PartitionReplica
{
    public required TopicPartition TopicPartition { get; init; }
    public required int BrokerId { get; init; }

    /// <summary>
    /// Current state of this replica.
    /// </summary>
    public ReplicaState State { get; set; } = ReplicaState.Offline;

    /// <summary>
    /// The log end offset (LEO) - highest offset written to local log.
    /// </summary>
    public long LogEndOffset { get; set; }

    /// <summary>
    /// The high watermark (HW) - highest offset replicated to all ISR.
    /// </summary>
    public long HighWatermark { get; set; }

    /// <summary>
    /// Last time this replica fetched from the leader.
    /// </summary>
    public DateTimeOffset LastFetchTime { get; set; }

    /// <summary>
    /// Last time this replica caught up with the leader.
    /// </summary>
    public DateTimeOffset LastCaughtUpTime { get; set; }

    /// <summary>
    /// Epoch of the current leader (increments on each election).
    /// </summary>
    public int LeaderEpoch { get; set; }

    /// <summary>
    /// Whether this replica is in the ISR (In-Sync Replicas).
    /// </summary>
    public bool IsInSync { get; set; }

    /// <summary>
    /// Whether this is the leader replica.
    /// </summary>
    public bool IsLeader => State == ReplicaState.Leader;

    /// <summary>
    /// How far behind the leader this replica is.
    /// </summary>
    public long Lag { get; set; }
}

/// <summary>
/// Possible states for a replica.
/// </summary>
public enum ReplicaState
{
    /// <summary>Replica is not available.</summary>
    Offline,

    /// <summary>Replica is starting up, syncing with leader.</summary>
    Starting,

    /// <summary>Replica is a follower, actively replicating from leader.</summary>
    Follower,

    /// <summary>Replica is the leader for this partition.</summary>
    Leader,

    /// <summary>Replica is shutting down.</summary>
    ShuttingDown
}
