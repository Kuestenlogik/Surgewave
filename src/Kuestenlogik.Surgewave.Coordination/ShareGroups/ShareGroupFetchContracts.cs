namespace Kuestenlogik.Surgewave.Coordination.ShareGroups;

// Protocol-neutral data-plane contracts for share groups (KIP-932): ShareFetch + ShareAcknowledge (#59).
// The fetched record bytes are carried by reference (no copy) as a byte[] to preserve zero-copy on the
// share-consume path; only the small per-range AcquiredRecords descriptors are wrapped.

/// <summary>
/// A run of acknowledge types applied to the offsets [FirstOffset, LastOffset]. Shared by ShareFetch's
/// inline acknowledgements and ShareAcknowledge. AcknowledgeType: 0=Gap,1=Accept,2=Release,3=Reject,4=Renew.
/// </summary>
public sealed record ShareAcknowledgementBatch(long FirstOffset, long LastOffset, IReadOnlyList<sbyte> AcknowledgeTypes);

/// <summary>The current leader for a partition in a share response.</summary>
public sealed record ShareLeader(int LeaderId, int LeaderEpoch);

// ── ShareFetch ──────────────────────────────────────────────────────────────

public sealed record ShareFetchPartition
{
    public required int PartitionIndex { get; init; }
    public required IReadOnlyList<ShareAcknowledgementBatch> AcknowledgementBatches { get; init; }
}

public sealed record ShareFetchTopic
{
    public required Guid TopicId { get; init; }
    public required IReadOnlyList<ShareFetchPartition> Partitions { get; init; }
}

public sealed record ShareFetchCommand
{
    public required string? GroupId { get; init; }
    public required string? MemberId { get; init; }
    public required int MaxRecords { get; init; }
    public required IReadOnlyList<ShareFetchTopic> Topics { get; init; }
}

/// <summary>A contiguous run of acquired records (same delivery count) delivered to the member.</summary>
public sealed record ShareAcquiredRecords(long FirstOffset, long LastOffset, short DeliveryCount);

public sealed record ShareFetchPartitionResult
{
    public required int PartitionIndex { get; init; }
    public required ShareGroupErrorStatus Status { get; init; }
    public required ShareGroupErrorStatus AcknowledgeStatus { get; init; }
    public required ShareLeader CurrentLeader { get; init; }
    /// <summary>Concatenated record bytes (by reference, zero-copy); null when nothing was acquired.</summary>
    public byte[]? Records { get; init; }
    public required IReadOnlyList<ShareAcquiredRecords> AcquiredRecords { get; init; }
}

public sealed record ShareFetchTopicResult(Guid TopicId, IReadOnlyList<ShareFetchPartitionResult> Partitions);

public sealed record ShareFetchResult(IReadOnlyList<ShareFetchTopicResult> Responses);

// ── ShareAcknowledge ────────────────────────────────────────────────────────

public sealed record ShareAcknowledgePartition
{
    public required int PartitionIndex { get; init; }
    public required IReadOnlyList<ShareAcknowledgementBatch> AcknowledgementBatches { get; init; }
}

public sealed record ShareAcknowledgeTopic
{
    public required Guid TopicId { get; init; }
    public required IReadOnlyList<ShareAcknowledgePartition> Partitions { get; init; }
}

public sealed record ShareAcknowledgeCommand
{
    public required string? GroupId { get; init; }
    public required IReadOnlyList<ShareAcknowledgeTopic> Topics { get; init; }
}

public sealed record ShareAcknowledgePartitionResult
{
    public required int PartitionIndex { get; init; }
    public required ShareGroupErrorStatus Status { get; init; }
    public required ShareLeader CurrentLeader { get; init; }
}

public sealed record ShareAcknowledgeTopicResult(Guid TopicId, IReadOnlyList<ShareAcknowledgePartitionResult> Partitions);

public sealed record ShareAcknowledgeResult(IReadOnlyList<ShareAcknowledgeTopicResult> Responses);
