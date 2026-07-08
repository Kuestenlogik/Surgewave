namespace Kuestenlogik.Surgewave.Coordination.Consumer;

// Protocol-neutral offset contracts (OffsetCommit / OffsetFetch / OffsetDelete), #59.
//
// The Kafka wire shapes differ by version — OffsetFetch is single-group below v8 and a
// group batch from v8, and topic identity is a name below v10 and a Guid topic id from v10.
// Those differences are wire concerns owned by the adapter, which lowers every request into
// the uniform group-list shape below (a single-group request becomes a one-element list) and
// re-encodes the result into the version-appropriate envelope. The coordinator keeps the
// topic-id <-> name resolution (it owns the topic registry) and reports the resolution flag
// via <c>UseTopicId</c> rather than an api version.

// ── OffsetCommit ────────────────────────────────────────────────────────────

/// <summary>A single (partition, offset) to commit. Metadata/leader-epoch are not persisted.</summary>
public sealed record OffsetCommitPartition(int PartitionIndex, long CommittedOffset);

/// <summary>A topic's partitions to commit. Exactly one of name / id is populated per <c>UseTopicId</c>.</summary>
public sealed record OffsetCommitTopic
{
    public required string Topic { get; init; }
    public required Guid TopicId { get; init; }
    public required IReadOnlyList<OffsetCommitPartition> Partitions { get; init; }
}

/// <summary>Neutral OffsetCommit request. Member/epoch feed the KIP-848 v2 fall-through fence.</summary>
public sealed record OffsetCommitCommand
{
    public required string GroupId { get; init; }
    public required string? MemberId { get; init; }
    public required int GenerationIdOrMemberEpoch { get; init; }
    public required bool UseTopicId { get; init; }
    public required IReadOnlyList<OffsetCommitTopic> Topics { get; init; }
}

public sealed record OffsetCommitPartitionResult(int PartitionIndex, ConsumerGroupErrorStatus Status);

public sealed record OffsetCommitTopicResult
{
    public required string Topic { get; init; }
    public required Guid TopicId { get; init; }
    public required IReadOnlyList<OffsetCommitPartitionResult> Partitions { get; init; }
}

public sealed record OffsetCommitResult(IReadOnlyList<OffsetCommitTopicResult> Topics);

// ── OffsetFetch ─────────────────────────────────────────────────────────────

/// <summary>A topic + partition selector to fetch offsets for. One of name / id per <c>UseTopicId</c>.</summary>
public sealed record OffsetFetchTopicRequest
{
    public required string Topic { get; init; }
    public required Guid TopicId { get; init; }
    public required IReadOnlyList<int> PartitionIndexes { get; init; }
}

/// <summary>A group's topic selectors within an OffsetFetch request.</summary>
public sealed record OffsetFetchGroupRequest
{
    public required string GroupId { get; init; }
    public required IReadOnlyList<OffsetFetchTopicRequest> Topics { get; init; }
}

/// <summary>
/// Neutral OffsetFetch request. Always a list of groups: the single-group (pre-v8) wire
/// shape is lowered to a one-element list by the adapter.
/// </summary>
public sealed record OffsetFetchCommand
{
    public required bool UseTopicId { get; init; }
    public required IReadOnlyList<OffsetFetchGroupRequest> Groups { get; init; }
}

public sealed record OffsetFetchPartitionResult(int PartitionIndex, long CommittedOffset, ConsumerGroupErrorStatus Status);

public sealed record OffsetFetchTopicResult
{
    public required string Topic { get; init; }
    public required Guid TopicId { get; init; }
    public required IReadOnlyList<OffsetFetchPartitionResult> Partitions { get; init; }
}

public sealed record OffsetFetchGroupResult
{
    public required string GroupId { get; init; }
    public required IReadOnlyList<OffsetFetchTopicResult> Topics { get; init; }
}

public sealed record OffsetFetchResult(IReadOnlyList<OffsetFetchGroupResult> Groups);

// ── OffsetDelete (KIP-496) ──────────────────────────────────────────────────

/// <summary>A topic + partitions whose committed offsets should be deleted (by name).</summary>
public sealed record OffsetDeleteTopic
{
    public required string Name { get; init; }
    public required IReadOnlyList<int> Partitions { get; init; }
}

public sealed record OffsetDeleteCommand
{
    public required string GroupId { get; init; }
    public required IReadOnlyList<OffsetDeleteTopic> Topics { get; init; }
}

public sealed record OffsetDeletePartitionResult(int PartitionIndex, ConsumerGroupErrorStatus Status);

public sealed record OffsetDeleteTopicResult
{
    public required string Name { get; init; }
    public required IReadOnlyList<OffsetDeletePartitionResult> Partitions { get; init; }
}

public sealed record OffsetDeleteResult(IReadOnlyList<OffsetDeleteTopicResult> Topics);
