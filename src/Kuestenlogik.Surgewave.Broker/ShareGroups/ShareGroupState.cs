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

    /// <summary>
    /// KIP-1240 — max attempts to deliver a record before it's archived.
    /// Upstream Kafka clamps this to [2, 10] with a default of 5; we keep
    /// the default but don't enforce the clamp at this layer (the admin /
    /// IncrementalAlterConfigs path will). Today the coordinator captures
    /// the value; full archive-on-overflow semantics is a follow-up.
    /// </summary>
    public int MaxDeliveryCount { get; set; } = 5;

    /// <summary>
    /// KIP-1240 — max in-flight record locks per partition. Upstream
    /// clamps to [100, 10000] with a default of 2000. Captured as state
    /// today; per-partition lock-cap enforcement (return THROTTLING_QUOTA_EXCEEDED
    /// when exceeded) is a follow-up.
    /// </summary>
    public int MaxRecordLocks { get; set; } = 2000;

    /// <summary>
    /// KIP-1240 — gates the KIP-1222 RENEW acknowledgement type. When
    /// <c>false</c>, the coordinator rejects RENEW acks with
    /// <c>INVALID_REQUEST</c>; when <c>true</c> (default), RENEW extends
    /// the in-flight lock as per KIP-1222.
    /// </summary>
    public bool RenewAcknowledgeEnabled { get; set; } = true;
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
