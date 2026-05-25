namespace Kuestenlogik.Surgewave.Broker.StreamsGroups;

/// <summary>
/// Tracks the state of a single streams group (KIP-1071).
/// Manages topology-aware task assignment for Kafka Streams applications.
/// </summary>
internal sealed class StreamsGroupState
{
    public required string GroupId { get; init; }

    /// <summary>Monotonically increasing epoch; bumped on membership or topology changes.</summary>
    public int GroupEpoch { get; set; } = 1;

    /// <summary>The epoch of the topology. Set when the first member joins with a topology.</summary>
    public int TopologyEpoch { get; set; }

    /// <summary>Members keyed by MemberId.</summary>
    public Dictionary<string, StreamsGroupMember> Members { get; } = [];

    /// <summary>The current topology of the streams application, submitted by the first member.</summary>
    public StoredTopology? Topology { get; set; }
}

/// <summary>
/// Represents a single member of a streams group (KIP-1071).
/// </summary>
internal sealed class StreamsGroupMember
{
    public required string MemberId { get; init; }
    public string? InstanceId { get; set; }
    public string? RackId { get; set; }
    public string? ClientId { get; set; }
    public string? ClientHost { get; set; }
    public string? ProcessId { get; set; }
    public int MemberEpoch { get; set; }
    public int TopologyEpoch { get; set; }

    /// <summary>Active tasks currently assigned to this member.</summary>
    public List<StreamsTaskIds> ActiveTasks { get; set; } = [];

    /// <summary>Standby tasks currently assigned to this member.</summary>
    public List<StreamsTaskIds> StandbyTasks { get; set; } = [];

    /// <summary>Warm-up tasks currently assigned to this member.</summary>
    public List<StreamsTaskIds> WarmupTasks { get; set; } = [];

    public DateTime LastHeartbeat { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// Task assignment for a subtopology — maps a subtopology to the partitions it processes.
/// </summary>
internal sealed class StreamsTaskIds
{
    public required string SubtopologyId { get; init; }
    public required List<int> Partitions { get; init; }
}

/// <summary>
/// Stored topology information for a streams group. Derived from the first member's heartbeat.
/// </summary>
internal sealed class StoredTopology
{
    public int Epoch { get; init; }
    public required List<StoredSubtopology> Subtopologies { get; init; }
}

/// <summary>
/// A subtopology in the stored topology. Tracks source topics for task partition count resolution.
/// </summary>
internal sealed class StoredSubtopology
{
    public required string SubtopologyId { get; init; }
    public required List<string> SourceTopics { get; init; }
}
