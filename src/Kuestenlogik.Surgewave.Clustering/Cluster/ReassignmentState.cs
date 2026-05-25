namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// State of a partition reassignment operation.
/// </summary>
public enum ReassignmentStatus
{
    /// <summary>
    /// Reassignment is pending, not yet started.
    /// </summary>
    Pending,

    /// <summary>
    /// New replicas are being added but not yet syncing.
    /// </summary>
    Adding,

    /// <summary>
    /// New replicas are syncing data from the leader.
    /// </summary>
    Syncing,

    /// <summary>
    /// Sync complete, completing the reassignment.
    /// </summary>
    Completing,

    /// <summary>
    /// Reassignment completed successfully.
    /// </summary>
    Completed,

    /// <summary>
    /// Reassignment failed.
    /// </summary>
    Failed,

    /// <summary>
    /// Reassignment was cancelled.
    /// </summary>
    Cancelled
}

/// <summary>
/// Tracks the state of a single partition reassignment.
/// </summary>
public sealed class PartitionReassignmentState
{
    /// <summary>
    /// Topic name.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Partition number.
    /// </summary>
    public int Partition { get; init; }

    /// <summary>
    /// Original replica assignment before reassignment.
    /// </summary>
    public List<int> OriginalReplicas { get; init; } = [];

    /// <summary>
    /// Target replica assignment after reassignment.
    /// </summary>
    public List<int> TargetReplicas { get; init; } = [];

    /// <summary>
    /// Replicas being added (in TargetReplicas but not in OriginalReplicas).
    /// </summary>
    public List<int> AddingReplicas { get; init; } = [];

    /// <summary>
    /// Replicas being removed (in OriginalReplicas but not in TargetReplicas).
    /// </summary>
    public List<int> RemovingReplicas { get; init; } = [];

    /// <summary>
    /// Current status of the reassignment.
    /// </summary>
    public ReassignmentStatus Status { get; set; } = ReassignmentStatus.Pending;

    /// <summary>
    /// Timestamp when the reassignment was started.
    /// </summary>
    public DateTimeOffset StartedAt { get; set; }

    /// <summary>
    /// Timestamp when the reassignment was completed.
    /// </summary>
    public DateTimeOffset? CompletedAt { get; set; }

    /// <summary>
    /// Bytes replicated to new replicas.
    /// </summary>
    public long BytesReplicated { get; set; }

    /// <summary>
    /// Estimated total bytes to replicate.
    /// </summary>
    public long TotalBytes { get; set; }

    /// <summary>
    /// Leader's log end offset when syncing started.
    /// Used as the target for new replicas to catch up to.
    /// </summary>
    public long TargetLeaderOffset { get; set; }

    /// <summary>
    /// Starting offsets for each adding replica when syncing began.
    /// Key: broker ID, Value: starting LEO
    /// </summary>
    public Dictionary<int, long> AddingReplicaStartOffsets { get; } = [];

    /// <summary>
    /// Progress percentage (0-100).
    /// </summary>
    public int ProgressPercent => TotalBytes > 0
        ? (int)(BytesReplicated * 100 / TotalBytes)
        : 0;

    /// <summary>
    /// Error message if the reassignment failed.
    /// </summary>
    public string? ErrorMessage { get; set; }
}

/// <summary>
/// Reassignment plan for multiple partitions.
/// </summary>
public sealed class ReassignmentPlan
{
    /// <summary>
    /// Schema version for the plan format.
    /// </summary>
    public int Version { get; set; } = 1;

    /// <summary>
    /// List of partition reassignments.
    /// </summary>
    public List<PartitionReassignment> Partitions { get; set; } = [];
}

/// <summary>
/// A single partition reassignment in a plan.
/// </summary>
public sealed class PartitionReassignment
{
    /// <summary>
    /// Topic name.
    /// </summary>
    public required string Topic { get; set; }

    /// <summary>
    /// Partition number.
    /// </summary>
    public int Partition { get; set; }

    /// <summary>
    /// Target replica assignment (first replica is preferred leader).
    /// </summary>
    public List<int> Replicas { get; set; } = [];
}

/// <summary>
/// Summary of reassignment progress.
/// </summary>
public sealed class ReassignmentSummary
{
    /// <summary>
    /// Total partitions in the reassignment.
    /// </summary>
    public int TotalPartitions { get; init; }

    /// <summary>
    /// Partitions still pending.
    /// </summary>
    public int Pending { get; init; }

    /// <summary>
    /// Partitions currently in progress.
    /// </summary>
    public int InProgress { get; init; }

    /// <summary>
    /// Partitions completed successfully.
    /// </summary>
    public int Completed { get; init; }

    /// <summary>
    /// Partitions that failed.
    /// </summary>
    public int Failed { get; init; }

    /// <summary>
    /// Overall progress percentage.
    /// </summary>
    public int OverallProgressPercent { get; init; }

    /// <summary>
    /// Whether all reassignments are done (completed or failed).
    /// </summary>
    public bool IsDone => Pending == 0 && InProgress == 0;

    /// <summary>
    /// Whether all reassignments completed successfully.
    /// </summary>
    public bool IsSuccess => IsDone && Failed == 0;
}
