namespace Kuestenlogik.Surgewave.Coordination.ShareGroups;

// Protocol-neutral share-group offset admin contracts (KIP-932):
// DescribeShareGroupOffsets / AlterShareGroupOffsets / DeleteShareGroupOffsets (#59).

// ── DescribeShareGroupOffsets ───────────────────────────────────────────────

/// <summary>A requested topic + partitions to describe offsets for.</summary>
public sealed record DescribeShareOffsetsTopic
{
    public required string TopicName { get; init; }
    public required IReadOnlyList<int> Partitions { get; init; }
}

/// <summary>A group's describe-offsets request; null <see cref="Topics"/> means "all subscribed topics".</summary>
public sealed record DescribeShareOffsetsGroup
{
    public required string GroupId { get; init; }
    public IReadOnlyList<DescribeShareOffsetsTopic>? Topics { get; init; }
}

public sealed record DescribeShareOffsetsCommand(IReadOnlyList<DescribeShareOffsetsGroup> Groups);

public sealed record DescribeShareOffsetsPartitionResult
{
    public required int PartitionIndex { get; init; }
    public required long StartOffset { get; init; }
    public required int LeaderEpoch { get; init; }
    public required long Lag { get; init; }
    public required ShareGroupErrorStatus Status { get; init; }
}

public sealed record DescribeShareOffsetsTopicResult
{
    public required string TopicName { get; init; }
    public required Guid TopicId { get; init; }
    public required IReadOnlyList<DescribeShareOffsetsPartitionResult> Partitions { get; init; }
}

public sealed record DescribeShareOffsetsGroupResult
{
    public required string GroupId { get; init; }
    public required IReadOnlyList<DescribeShareOffsetsTopicResult> Topics { get; init; }
    public required ShareGroupErrorStatus Status { get; init; }
    public string? ErrorMessage { get; init; }
}

public sealed record DescribeShareOffsetsResult(IReadOnlyList<DescribeShareOffsetsGroupResult> Groups);

// ── AlterShareGroupOffsets ──────────────────────────────────────────────────

public sealed record AlterShareOffsetsPartition(int PartitionIndex, long StartOffset);

public sealed record AlterShareOffsetsTopic
{
    public required string TopicName { get; init; }
    public required IReadOnlyList<AlterShareOffsetsPartition> Partitions { get; init; }
}

public sealed record AlterShareOffsetsCommand
{
    public required string GroupId { get; init; }
    public required IReadOnlyList<AlterShareOffsetsTopic> Topics { get; init; }
}

public sealed record AlterShareOffsetsPartitionResult(int PartitionIndex, ShareGroupErrorStatus Status);

public sealed record AlterShareOffsetsTopicResult
{
    public required string TopicName { get; init; }
    public required Guid TopicId { get; init; }
    public required IReadOnlyList<AlterShareOffsetsPartitionResult> Partitions { get; init; }
}

public sealed record AlterShareOffsetsResult
{
    public required ShareGroupErrorStatus Status { get; init; }
    public string? ErrorMessage { get; init; }
    public required IReadOnlyList<AlterShareOffsetsTopicResult> Responses { get; init; }
}

// ── DeleteShareGroupOffsets ─────────────────────────────────────────────────

public sealed record DeleteShareOffsetsTopic(string TopicName);

public sealed record DeleteShareOffsetsCommand
{
    public required string GroupId { get; init; }
    public required IReadOnlyList<DeleteShareOffsetsTopic> Topics { get; init; }
}

public sealed record DeleteShareOffsetsTopicResult
{
    public required string TopicName { get; init; }
    public required Guid TopicId { get; init; }
    public required ShareGroupErrorStatus Status { get; init; }
}

public sealed record DeleteShareOffsetsResult
{
    public required ShareGroupErrorStatus Status { get; init; }
    public string? ErrorMessage { get; init; }
    public required IReadOnlyList<DeleteShareOffsetsTopicResult> Responses { get; init; }
}
