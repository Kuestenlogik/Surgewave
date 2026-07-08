namespace Kuestenlogik.Surgewave.Coordination.ShareGroups;

// Protocol-neutral membership contracts for share groups (KIP-932): ShareGroupHeartbeat + ShareGroupDescribe (#59).

// ── ShareGroupHeartbeat ─────────────────────────────────────────────────────

/// <summary>Neutral ShareGroupHeartbeat request. MemberEpoch -1 signals a leave.</summary>
public sealed record ShareGroupHeartbeatCommand
{
    public required string GroupId { get; init; }
    public required string MemberId { get; init; }
    public required int MemberEpoch { get; init; }
    public required string ClientId { get; init; }
    public string? RackId { get; init; }
    public IReadOnlyList<string>? SubscribedTopicNames { get; init; }
}

/// <summary>A topic + its partitions in a heartbeat assignment (by topic id).</summary>
public sealed record ShareTopicPartitions(Guid TopicId, IReadOnlyList<int> Partitions);

/// <summary>The member's heartbeat assignment; null means "no assignment".</summary>
public sealed record ShareAssignment(IReadOnlyList<ShareTopicPartitions> TopicPartitions);

/// <summary>Neutral ShareGroupHeartbeat result. The classic path has no error branch (join/leave both succeed).</summary>
public sealed record ShareGroupHeartbeatResult
{
    public required string MemberId { get; init; }
    public required int MemberEpoch { get; init; }
    public required int HeartbeatIntervalMs { get; init; }
    public ShareAssignment? Assignment { get; init; }
}

// ── ShareGroupDescribe ──────────────────────────────────────────────────────

/// <summary>A topic + partitions in a describe assignment (carries the topic name too).</summary>
public sealed record ShareDescribeTopicPartitions
{
    public required Guid TopicId { get; init; }
    public required string TopicName { get; init; }
    public required IReadOnlyList<int> Partitions { get; init; }
}

public sealed record ShareDescribeAssignment(IReadOnlyList<ShareDescribeTopicPartitions> TopicPartitions);

public sealed record ShareGroupMemberDescription
{
    public required string MemberId { get; init; }
    public string? RackId { get; init; }
    public required int MemberEpoch { get; init; }
    public required string ClientId { get; init; }
    public required string ClientHost { get; init; }
    public required IReadOnlyList<string> SubscribedTopicNames { get; init; }
    public required ShareDescribeAssignment Assignment { get; init; }
}

/// <summary>
/// A described share group. When <see cref="Status"/> is <see cref="ShareGroupErrorStatus.InvalidGroupId"/>
/// the group was not found and only <see cref="GroupId"/> is meaningful.
/// </summary>
public sealed record ShareGroupDescription
{
    public required ShareGroupErrorStatus Status { get; init; }
    public required string GroupId { get; init; }
    public required string GroupState { get; init; }
    public int GroupEpoch { get; init; }
    public int AssignmentEpoch { get; init; }
    public required string AssignorName { get; init; }
    public required IReadOnlyList<ShareGroupMemberDescription> Members { get; init; }
}
