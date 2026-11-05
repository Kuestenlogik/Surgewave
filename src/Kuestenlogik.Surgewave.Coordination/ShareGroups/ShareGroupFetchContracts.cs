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

    /// <summary>
    /// How long a fetch with nothing to deliver waits for records, in milliseconds (#231): until a record arrives in a
    /// partition it serves, one goes back to the group, or a member's lock runs out. 0 answers at once.
    /// </summary>
    public int MaxWaitMs { get; init; }

    /// <summary>
    /// The topics to fetch — with a share session, those that join it; without one, all of them — and the
    /// acknowledgements to apply.
    /// </summary>
    public required IReadOnlyList<ShareFetchTopic> Topics { get; init; }

    /// <summary>
    /// The member's share session epoch (KIP-932): 0 opens a session, -1 closes it, any other continues it; null for a
    /// request without a session, which fetches the topics it names (#228).
    /// </summary>
    public int? ShareSessionEpoch { get; init; }

    /// <summary>The partitions that leave the share session.</summary>
    public IReadOnlyList<ShareFetchTopic> ForgottenTopics { get; init; } = [];
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

public sealed record ShareFetchResult(IReadOnlyList<ShareFetchTopicResult> Responses)
{
    /// <summary>Why the whole request was refused — its share session — or None.</summary>
    public ShareGroupErrorStatus Status { get; init; }
}

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

    /// <summary>The member acknowledging: only the records it holds can be acknowledged (#228).</summary>
    public string? MemberId { get; init; }
    public required IReadOnlyList<ShareAcknowledgeTopic> Topics { get; init; }

    /// <summary>The member's share session epoch: -1 closes the session, any other but 0 continues it; null for none.</summary>
    public int? ShareSessionEpoch { get; init; }
}

public sealed record ShareAcknowledgePartitionResult
{
    public required int PartitionIndex { get; init; }
    public required ShareGroupErrorStatus Status { get; init; }
    public required ShareLeader CurrentLeader { get; init; }
}

public sealed record ShareAcknowledgeTopicResult(Guid TopicId, IReadOnlyList<ShareAcknowledgePartitionResult> Partitions);

public sealed record ShareAcknowledgeResult(IReadOnlyList<ShareAcknowledgeTopicResult> Responses)
{
    /// <summary>Why the whole request was refused — its share session — or None.</summary>
    public ShareGroupErrorStatus Status { get; init; }
}
