namespace Kuestenlogik.Surgewave.Coordination.Streams;

/// <summary>Neutral status for a streams-group operation (maps to a wire error code in the adapter).</summary>
public enum StreamsGroupStatus
{
    /// <summary>Success.</summary>
    Ok,
    /// <summary>The requested group id does not exist.</summary>
    GroupNotFound,
}

/// <summary>Runtime phase of a streams group (maps to the wire group-state string in the adapter).</summary>
public enum StreamsGroupPhase
{
    /// <summary>No members.</summary>
    Empty,
    /// <summary>At least one member.</summary>
    Stable,
}

/// <summary>A subtopology's active/standby/warmup task ids (a subtopology id and its partitions).</summary>
public sealed record StreamsTaskAssignment(string SubtopologyId, IReadOnlyList<int> Partitions);

/// <summary>One subtopology of a streams topology, reduced to the fields the coordinator tracks.</summary>
public sealed record StreamsSubtopology(string SubtopologyId, IReadOnlyList<string> SourceTopics);

/// <summary>A streams topology (epoch + subtopologies), protocol-neutral.</summary>
public sealed record StreamsTopology(int Epoch, IReadOnlyList<StreamsSubtopology> Subtopologies);

/// <summary>
/// A streams-group member heartbeat request in neutral form. <see cref="MemberEpoch"/> drives the
/// lifecycle: -1 = leave, -2 = shutdown-then-rejoin (static), 0 = join, &gt;0 = steady-state.
/// <see cref="ClientId"/> is domain data here (used to generate the member id on join), not just wire envelope.
/// </summary>
public sealed record StreamsHeartbeatCommand
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
    /// <summary>Optional streams process id.</summary>
    public string? ProcessId { get; init; }
    /// <summary>Declared topology (only sent while it changes); null when unchanged.</summary>
    public StreamsTopology? Topology { get; init; }
}

/// <summary>The member's assignment + heartbeat cadence after a heartbeat.</summary>
public sealed record StreamsHeartbeatResult
{
    /// <summary>The member id (generated on join, echoed otherwise).</summary>
    public required string MemberId { get; init; }
    /// <summary>The member's current epoch.</summary>
    public int MemberEpoch { get; init; }
    /// <summary>Heartbeat interval the member should use (ms).</summary>
    public int HeartbeatIntervalMs { get; init; }
    /// <summary>Acceptable standby recovery lag (records).</summary>
    public int AcceptableRecoveryLag { get; init; }
    /// <summary>Task-offset reporting interval (ms).</summary>
    public int TaskOffsetIntervalMs { get; init; }
    /// <summary>Assigned active tasks (empty when none).</summary>
    public IReadOnlyList<StreamsTaskAssignment> ActiveTasks { get; init; } = [];
    /// <summary>Assigned standby tasks (empty when none).</summary>
    public IReadOnlyList<StreamsTaskAssignment> StandbyTasks { get; init; } = [];
    /// <summary>Assigned warmup tasks (empty when none).</summary>
    public IReadOnlyList<StreamsTaskAssignment> WarmupTasks { get; init; } = [];
}

/// <summary>One member's projection in a streams-group description.</summary>
public sealed record StreamsGroupMemberDescription
{
    /// <summary>Member id.</summary>
    public required string MemberId { get; init; }
    /// <summary>Member epoch.</summary>
    public int MemberEpoch { get; init; }
    /// <summary>Optional static instance id.</summary>
    public string? InstanceId { get; init; }
    /// <summary>Optional rack id.</summary>
    public string? RackId { get; init; }
    /// <summary>Client id.</summary>
    public string? ClientId { get; init; }
    /// <summary>Client host.</summary>
    public string? ClientHost { get; init; }
    /// <summary>Topology epoch the member is on.</summary>
    public int TopologyEpoch { get; init; }
    /// <summary>Streams process id.</summary>
    public string? ProcessId { get; init; }
    /// <summary>Assigned active tasks.</summary>
    public IReadOnlyList<StreamsTaskAssignment> ActiveTasks { get; init; } = [];
    /// <summary>Assigned standby tasks.</summary>
    public IReadOnlyList<StreamsTaskAssignment> StandbyTasks { get; init; } = [];
    /// <summary>Assigned warmup tasks.</summary>
    public IReadOnlyList<StreamsTaskAssignment> WarmupTasks { get; init; } = [];
}

/// <summary>A streams-group description (one per requested group id).</summary>
public sealed record StreamsGroupDescription
{
    /// <summary>The group id.</summary>
    public required string GroupId { get; init; }
    /// <summary>Operation status — <see cref="StreamsGroupStatus.GroupNotFound"/> for unknown ids.</summary>
    public StreamsGroupStatus Status { get; init; }
    /// <summary>Runtime phase (meaningful only when <see cref="Status"/> is <see cref="StreamsGroupStatus.Ok"/>).</summary>
    public StreamsGroupPhase Phase { get; init; }
    /// <summary>Group epoch.</summary>
    public int GroupEpoch { get; init; }
    /// <summary>Assignment epoch (equals the group epoch by construction today).</summary>
    public int AssignmentEpoch { get; init; }
    /// <summary>The group's topology, if any.</summary>
    public StreamsTopology? Topology { get; init; }
    /// <summary>The group's members.</summary>
    public IReadOnlyList<StreamsGroupMemberDescription> Members { get; init; } = [];
}
