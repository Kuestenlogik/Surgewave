namespace Kuestenlogik.Surgewave.Coordination.Consumer;

// Protocol-neutral membership contracts for the classic consumer-group rebalance
// protocol (JoinGroup / SyncGroup / Heartbeat / LeaveGroup), #59. Assignment and
// subscription metadata are opaque byte payloads: the coordinator stores and echoes
// them verbatim, so they are carried by reference (no copy) as byte[].

// ── JoinGroup ───────────────────────────────────────────────────────────────

/// <summary>A join protocol candidate: its name and the opaque subscription metadata bytes.</summary>
public sealed record GroupJoinProtocol(string Name, byte[] Metadata);

/// <summary>Neutral JoinGroup request.</summary>
public sealed record JoinGroupCommand
{
    public required string GroupId { get; init; }
    public required string MemberId { get; init; }
    public required string ClientId { get; init; }
    public string? GroupInstanceId { get; init; }
    public required string ProtocolType { get; init; }
    public required int SessionTimeoutMs { get; init; }
    public required IReadOnlyList<GroupJoinProtocol> Protocols { get; init; }
}

/// <summary>A member as echoed to the elected leader in a JoinGroup response.</summary>
public sealed record JoinGroupMemberInfo(string MemberId, string? GroupInstanceId, byte[] Metadata);

/// <summary>Neutral JoinGroup result. The classic path has no error branch, so no status is carried.</summary>
public sealed record JoinGroupResult
{
    public required int GenerationId { get; init; }
    public required string ProtocolName { get; init; }
    public required string LeaderId { get; init; }
    public required string MemberId { get; init; }
    public required IReadOnlyList<JoinGroupMemberInfo> Members { get; init; }
}

// ── SyncGroup ───────────────────────────────────────────────────────────────

/// <summary>A leader-supplied assignment for a member: opaque assignment bytes.</summary>
public sealed record SyncGroupAssignmentInput(string MemberId, byte[] Assignment);

/// <summary>Neutral SyncGroup request.</summary>
public sealed record SyncGroupCommand
{
    public required string GroupId { get; init; }
    public required string MemberId { get; init; }
    public required int GenerationId { get; init; }
    public required IReadOnlyList<SyncGroupAssignmentInput> Assignments { get; init; }
}

/// <summary>Neutral SyncGroup result: the member's assignment bytes (empty when none).</summary>
public sealed record SyncGroupResult(ConsumerGroupErrorStatus Status, byte[] Assignment);

// ── Heartbeat ───────────────────────────────────────────────────────────────

/// <summary>Neutral classic-protocol Heartbeat request.</summary>
public sealed record GroupHeartbeatCommand(string GroupId, string MemberId);

/// <summary>Neutral classic-protocol Heartbeat result.</summary>
public sealed record GroupHeartbeatResult(ConsumerGroupErrorStatus Status);

// ── LeaveGroup ──────────────────────────────────────────────────────────────

/// <summary>A member identified in a batch (v3+) LeaveGroup request.</summary>
public sealed record LeaveGroupMemberInput(string MemberId, string? GroupInstanceId);

/// <summary>Neutral LeaveGroup request. A non-empty <see cref="Members"/> list selects the batch path.</summary>
public sealed record LeaveGroupCommand
{
    public required string GroupId { get; init; }
    public required string MemberId { get; init; }
    public required IReadOnlyList<LeaveGroupMemberInput> Members { get; init; }
}

/// <summary>Per-member result of a batch LeaveGroup.</summary>
public sealed record LeaveGroupMemberResult(string MemberId, string? GroupInstanceId, ConsumerGroupErrorStatus Status);

/// <summary>
/// Neutral LeaveGroup result. <see cref="Members"/> is null for the single-member (v0-2)
/// path and non-null (possibly empty) for the batch (v3+) path.
/// </summary>
public sealed record LeaveGroupResult(ConsumerGroupErrorStatus Status, IReadOnlyList<LeaveGroupMemberResult>? Members);
