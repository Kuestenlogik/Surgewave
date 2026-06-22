using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;

/// <summary>
/// Tracks the state of a single consumer group using the v2 protocol (KIP-848).
/// Server-side assignment — no SyncGroup needed. Public for JSON-based persistence
/// (KIP-848/932 9c); broker code still treats the type as internal-by-convention.
/// </summary>
public sealed class ConsumerGroupV2State
{
    [JsonInclude]
    public string GroupId { get; set; } = "";

    /// <summary>Monotonically increasing epoch; bumped on membership or subscription changes.</summary>
    public int GroupEpoch { get; set; } = 1;

    /// <summary>The epoch of the current assignment. Tracks when the last assignment was computed.</summary>
    public int AssignmentEpoch { get; set; } = 1;

    /// <summary>The server-side assignor name.</summary>
    public string AssignorName { get; set; } = "range";

    /// <summary>
    /// Rebalance timeout sourced from the founding member's first heartbeat (KIP-955).
    /// Subsequent heartbeats can lower it but never raise it back above the original
    /// value — slow members must not push the group's tolerance up after the fact.
    /// <c>-1</c> means "use the broker default".
    /// </summary>
    public int RebalanceTimeoutMs { get; set; } = -1;

    /// <summary>
    /// True once the very first heartbeat has been processed for this group. Used to
    /// distinguish initial-metadata establishment from subsequent rejoins.
    /// </summary>
    public bool Initialized { get; set; }

    /// <summary>Members keyed by MemberId.</summary>
    public Dictionary<string, ConsumerGroupV2Member> Members { get; set; } = [];
}

/// <summary>
/// Represents a single member of a consumer group v2 (KIP-848). Public for the
/// same JSON-persistence reason as <see cref="ConsumerGroupV2State"/>.
/// </summary>
public sealed class ConsumerGroupV2Member
{
    [JsonInclude]
    public string MemberId { get; set; } = "";
    public string? InstanceId { get; set; }
    public string? RackId { get; set; }
    public string? ClientId { get; set; }
    public string? ClientHost { get; set; }
    public int MemberEpoch { get; set; }
    public List<string> SubscribedTopicNames { get; set; } = [];
    public string? SubscribedTopicRegex { get; set; }

    /// <summary>
    /// Current assignment as the broker has revealed it to this member. Equivalent to the
    /// "communicated" assignment in the KIP-848 reconciliation state machine.
    /// </summary>
    public List<TopicPartitionAssignment> Assignment { get; set; } = [];

    /// <summary>
    /// Target assignment computed by the server-side assignor — what the member should
    /// converge to once all reconciliation steps complete.
    /// </summary>
    public List<TopicPartitionAssignment> TargetAssignment { get; set; } = [];

    /// <summary>
    /// Partitions the member reports owning (i.e. actively consuming). Sourced from the
    /// <c>TopicPartitions</c> field on the heartbeat request. Used by the reconciler to
    /// decide whether the member has finished revoking partitions that another member is
    /// supposed to take over.
    /// </summary>
    public List<TopicPartitionAssignment> OwnedTopicPartitions { get; set; } = [];

    public DateTime LastHeartbeat { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// A topic and its assigned partition indexes for a consumer group v2 member.
/// Public for JSON persistence.
/// </summary>
public sealed class TopicPartitionAssignment
{
    [JsonInclude]
    public Guid TopicId { get; set; }

    [JsonInclude]
    public List<int> Partitions { get; set; } = [];

    /// <summary>
    /// KIP-1251 — per-partition assignment epoch, same length as
    /// <see cref="Partitions"/>. The epoch at which each partition was last
    /// (re-)assigned to this member: stable across rebalances when the
    /// partition stays with the same member, bumped to the group's current
    /// AssignmentEpoch when newly assigned. <c>null</c> on records persisted
    /// before KIP-1251 landed; <see cref="TargetAssignmentComputer"/>
    /// repopulates it on the next assignment compute.
    ///
    /// Today this is used as a forward-compat persistence shape and assignor
    /// signal. Per-partition fencing on the OffsetCommit / TxnOffsetCommit
    /// path — the KIP's motivating use case — is a documented follow-up:
    /// Surgewave currently fences at group level (strictly more
    /// conservative), so adding the state now enables the finer-grained
    /// fence to be wired in a separate, narrowly-scoped change.
    /// </summary>
    [JsonInclude]
    public List<int>? AssignmentEpochs { get; set; }
}
