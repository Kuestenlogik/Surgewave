namespace Kuestenlogik.Surgewave.Coordination.Consumer;

/// <summary>
/// Neutral fence status for KIP-848 heartbeat + offset paths (maps to a wire error code in the
/// adapter / classic coordinator). <see cref="NotAV2Group"/> is a sentinel used only on the offset
/// path and MUST stay distinct from a real fence so the classic coordinator can fall through.
/// </summary>
public enum ConsumerGroupFenceStatus
{
    /// <summary>Valid (wire ErrorCode.None).</summary>
    Ok,
    /// <summary>Member id unknown/empty (wire UnknownMemberId).</summary>
    UnknownMember,
    /// <summary>Member epoch older than the group's (wire StaleMemberEpoch) — heartbeat fence only.</summary>
    StaleEpoch,
    /// <summary>Member epoch ahead of the group's (wire FencedMemberEpoch).</summary>
    FencedEpoch,
    /// <summary>Not a KIP-848 group — offset path only; caller falls through to the classic coordinator (wire UnknownTopicOrPartition sentinel).</summary>
    NotAV2Group,
}

/// <summary>Neutral describe status.</summary>
public enum ConsumerGroupDescribeStatus
{
    /// <summary>Success.</summary>
    Ok,
    /// <summary>The requested group id does not exist.</summary>
    GroupNotFound,
}

/// <summary>Runtime phase of a KIP-848 group (maps to the wire group-state string in the adapter).</summary>
public enum ConsumerGroupPhase
{
    /// <summary>No members.</summary>
    Empty,
    /// <summary>All members on the target assignment.</summary>
    Stable,
    /// <summary>A rebalance is in progress.</summary>
    Reconciling,
}

/// <summary>A topic's assigned/owned/target partitions, keyed by topic id (protocol-neutral).</summary>
public sealed record ConsumerTopicPartitions(Guid TopicId, IReadOnlyList<int> Partitions);

/// <summary>
/// A KIP-848 member heartbeat in neutral form. <see cref="MemberEpoch"/> drives the lifecycle:
/// -1 = leave, 0 = join/rejoin, &gt;0 = steady-state. <see cref="ClientId"/> is domain data (used to
/// generate the member id on join). <see cref="SubscribedTopicNames"/> and
/// <see cref="OwnedTopicPartitions"/> are null when unchanged since the last heartbeat.
/// </summary>
public sealed record ConsumerHeartbeatCommand
{
    /// <summary>Target group id.</summary>
    public required string GroupId { get; init; }
    /// <summary>Member id ("" on first join — the coordinator generates one).</summary>
    public required string MemberId { get; init; }
    /// <summary>Current member epoch (see remarks for the lifecycle encoding).</summary>
    public int MemberEpoch { get; init; }
    /// <summary>Client id — used to derive the generated member id on join.</summary>
    public string? ClientId { get; init; }
    /// <summary>Optional static group instance id.</summary>
    public string? InstanceId { get; init; }
    /// <summary>Optional rack id.</summary>
    public string? RackId { get; init; }
    /// <summary>Rebalance timeout the member declares (ms); -1 when unset.</summary>
    public int RebalanceTimeoutMs { get; init; } = -1;
    /// <summary>Requested server-side assignor, if any.</summary>
    public string? ServerAssignor { get; init; }
    /// <summary>Declared topic subscription; null when unchanged.</summary>
    public IReadOnlyList<string>? SubscribedTopicNames { get; init; }
    /// <summary>Partitions the member currently owns (reported for reconciliation); null when unchanged.</summary>
    public IReadOnlyList<ConsumerTopicPartitions>? OwnedTopicPartitions { get; init; }
}

/// <summary>The member's assignment + fence status after a heartbeat.</summary>
public sealed record ConsumerHeartbeatResult
{
    /// <summary>Fence status — <see cref="ConsumerGroupFenceStatus.Ok"/> on the normal path.</summary>
    public ConsumerGroupFenceStatus Status { get; init; }
    /// <summary>The member id (generated on join, echoed otherwise).</summary>
    public required string MemberId { get; init; }
    /// <summary>The member's current epoch (echoed on a fence).</summary>
    public int MemberEpoch { get; init; }
    /// <summary>Heartbeat interval the member should use (ms).</summary>
    public int HeartbeatIntervalMs { get; init; }
    /// <summary>The member's assigned partitions (empty when none / on a fence).</summary>
    public IReadOnlyList<ConsumerTopicPartitions> Assignment { get; init; } = [];
}

/// <summary>One member's projection in a group description.</summary>
public sealed record ConsumerGroupMemberDescription
{
    /// <summary>Member id.</summary>
    public required string MemberId { get; init; }
    /// <summary>Optional static instance id.</summary>
    public string? InstanceId { get; init; }
    /// <summary>Optional rack id.</summary>
    public string? RackId { get; init; }
    /// <summary>Member epoch.</summary>
    public int MemberEpoch { get; init; }
    /// <summary>Client id.</summary>
    public string? ClientId { get; init; }
    /// <summary>Client host.</summary>
    public string? ClientHost { get; init; }
    /// <summary>The member's topic subscription.</summary>
    public IReadOnlyList<string> SubscribedTopicNames { get; init; } = [];
    /// <summary>Current assignment (assigned ∪ still-owned, i.e. including pending revocations — KAFKA-20431).</summary>
    public IReadOnlyList<ConsumerTopicPartitions> MemberAssignment { get; init; } = [];
    /// <summary>Target assignment.</summary>
    public IReadOnlyList<ConsumerTopicPartitions> TargetAssignment { get; init; } = [];
}

/// <summary>A KIP-848 group description (one per requested group id).</summary>
public sealed record ConsumerGroupDescription
{
    /// <summary>The group id.</summary>
    public required string GroupId { get; init; }
    /// <summary>Operation status — <see cref="ConsumerGroupDescribeStatus.GroupNotFound"/> for unknown ids.</summary>
    public ConsumerGroupDescribeStatus Status { get; init; }
    /// <summary>Runtime phase (meaningful only when <see cref="Status"/> is Ok).</summary>
    public ConsumerGroupPhase Phase { get; init; }
    /// <summary>Group epoch.</summary>
    public int GroupEpoch { get; init; }
    /// <summary>Assignment epoch.</summary>
    public int AssignmentEpoch { get; init; }
    /// <summary>Server assignor name.</summary>
    public string? AssignorName { get; init; }
    /// <summary>The group's members.</summary>
    public IReadOnlyList<ConsumerGroupMemberDescription> Members { get; init; } = [];
}
