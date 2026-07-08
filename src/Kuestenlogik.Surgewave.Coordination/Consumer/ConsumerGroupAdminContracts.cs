namespace Kuestenlogik.Surgewave.Coordination.Consumer;

// Protocol-neutral admin contracts (DescribeGroups / ListGroups / DeleteGroups), #59.

// ── DescribeGroups ──────────────────────────────────────────────────────────

/// <summary>A member as reported by DescribeGroups (opaque metadata/assignment bytes echoed verbatim).</summary>
public sealed record GroupDescriptionMember
{
    public required string MemberId { get; init; }
    public string? GroupInstanceId { get; init; }
    public required string ClientId { get; init; }
    public required string ClientHost { get; init; }
    public required byte[] MemberMetadata { get; init; }
    public required byte[] MemberAssignment { get; init; }
}

/// <summary>
/// A described group. When <see cref="Status"/> is <see cref="ConsumerGroupErrorStatus.InvalidGroupId"/>
/// the group was not found and only <see cref="GroupId"/> is meaningful.
/// </summary>
public sealed record GroupDescription
{
    public required ConsumerGroupErrorStatus Status { get; init; }
    public required string GroupId { get; init; }
    public required string GroupState { get; init; }
    public required string ProtocolType { get; init; }
    public required string ProtocolData { get; init; }
    public required IReadOnlyList<GroupDescriptionMember> Members { get; init; }
    public int AuthorizedOperations { get; init; }
}

// ── ListGroups ──────────────────────────────────────────────────────────────

/// <summary>A group as reported by ListGroups.</summary>
public sealed record GroupListing(string GroupId, string ProtocolType, string GroupState);

// ── DeleteGroups ────────────────────────────────────────────────────────────

/// <summary>Per-group result of a DeleteGroups request (idempotent: not-found reports success).</summary>
public sealed record DeleteGroupResult(string GroupId, ConsumerGroupErrorStatus Status);
