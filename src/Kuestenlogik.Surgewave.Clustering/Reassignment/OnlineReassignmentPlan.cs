using Kuestenlogik.Surgewave.Clustering.Cluster;

namespace Kuestenlogik.Surgewave.Clustering.Reassignment;

/// <summary>
/// A managed reassignment plan that wraps individual <see cref="PartitionReassignment"/> entries
/// with plan-level metadata including throttling, progress tracking, and lifecycle status.
/// This is different from the lower-level <see cref="ReassignmentPlan"/> which only holds
/// a list of partition-replica target assignments.
/// </summary>
public sealed class OnlineReassignmentPlan
{
    /// <summary>
    /// Unique plan identifier.
    /// </summary>
    public string Id { get; init; } = Guid.NewGuid().ToString("N");

    /// <summary>
    /// The underlying partition reassignment entries.
    /// </summary>
    public List<OnlinePartitionReassignment> Assignments { get; init; } = [];

    /// <summary>
    /// Current plan status.
    /// </summary>
    public ReassignmentPlanStatus Status { get; set; } = ReassignmentPlanStatus.Proposed;

    /// <summary>
    /// When the plan was created.
    /// </summary>
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// When the plan started executing.
    /// </summary>
    public DateTimeOffset? StartedAt { get; set; }

    /// <summary>
    /// When the plan completed (success, failure, or cancellation).
    /// </summary>
    public DateTimeOffset? CompletedAt { get; set; }

    /// <summary>
    /// Throttle rate for data replication during reassignment (bytes/sec).
    /// </summary>
    public int ThrottleRateBytesPerSec { get; set; } = 50_000_000;

    /// <summary>
    /// Optional human-readable description of the plan purpose.
    /// </summary>
    public string? Description { get; init; }
}

/// <summary>
/// Tracks an individual partition reassignment within a plan,
/// including source and target replicas and per-partition progress.
/// </summary>
public sealed class OnlinePartitionReassignment
{
    /// <summary>
    /// Topic name.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Partition number.
    /// </summary>
    public required int Partition { get; init; }

    /// <summary>
    /// Current replica assignment before reassignment.
    /// </summary>
    public required IReadOnlyList<int> CurrentReplicas { get; init; }

    /// <summary>
    /// Target replica assignment after reassignment.
    /// </summary>
    public required IReadOnlyList<int> TargetReplicas { get; init; }

    /// <summary>
    /// Current status of this partition reassignment.
    /// </summary>
    public ReassignmentStatus Status { get; set; } = ReassignmentStatus.Pending;

    /// <summary>
    /// Progress as a ratio between 0.0 and 1.0.
    /// </summary>
    public double Progress { get; set; }

    /// <summary>
    /// Number of bytes copied to new replicas so far.
    /// </summary>
    public long BytesCopied { get; set; }

    /// <summary>
    /// Total bytes to replicate (estimated from partition size).
    /// </summary>
    public long TotalBytes { get; set; }

    /// <summary>
    /// When this partition reassignment started.
    /// </summary>
    public DateTimeOffset? StartedAt { get; set; }

    /// <summary>
    /// When this partition reassignment completed.
    /// </summary>
    public DateTimeOffset? CompletedAt { get; set; }

    /// <summary>
    /// Error message if the reassignment failed.
    /// </summary>
    public string? Error { get; set; }
}
