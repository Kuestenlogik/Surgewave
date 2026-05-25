using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Broker.ShareGroups;

/// <summary>
/// Tracks the state of a single share group: members, epoch, subscriptions, and per-partition start offsets.
/// Public for JSON-based persistence (KIP-932 9c).
/// </summary>
public sealed class ShareGroupState
{
    [JsonInclude]
    public string GroupId { get; set; } = "";

    /// <summary>Monotonically increasing epoch; bumped on membership or subscription changes.</summary>
    public int GroupEpoch { get; set; } = 1;

    /// <summary>Members keyed by MemberId.</summary>
    public Dictionary<string, ShareGroupMember> Members { get; set; } = [];

    /// <summary>Union of all members' subscribed topic names.</summary>
    public HashSet<string> SubscribedTopics { get; set; } = [];

    /// <summary>Per-partition start offsets keyed by "topic:partition".</summary>
    public Dictionary<string, long> StartOffsets { get; set; } = [];
}

/// <summary>
/// Represents a single member of a share group. Public for JSON persistence.
/// </summary>
public sealed class ShareGroupMember
{
    [JsonInclude]
    public string MemberId { get; set; } = "";
    public string? RackId { get; set; }
    public string? ClientId { get; set; }
    public string? ClientHost { get; set; }
    public List<string> SubscribedTopicNames { get; set; } = [];
    public int MemberEpoch { get; set; }
    public DateTime LastHeartbeat { get; set; } = DateTime.UtcNow;
}
