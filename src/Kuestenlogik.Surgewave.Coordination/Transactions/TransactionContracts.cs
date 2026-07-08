namespace Kuestenlogik.Surgewave.Coordination.Transactions;

// Protocol-neutral command/result contracts for the transaction-coordinator wire APIs (#59).
// The Kafka DTO conversion + wire envelope (CorrelationId/ApiVersion/ThrottleTimeMs) live in
// the TransactionApiHandler adapter; the coordinator keeps its topic-id <-> name resolution.

// ── InitProducerId ──────────────────────────────────────────────────────────

public sealed record InitProducerIdCommand
{
    public required string? TransactionalId { get; init; }
    public required int TransactionTimeoutMs { get; init; }
    public required long ProducerId { get; init; }
    public required short ProducerEpoch { get; init; }
}

public sealed record InitProducerIdResult(TxnErrorStatus Status, long ProducerId, short ProducerEpoch);

// ── AddPartitionsToTxn ──────────────────────────────────────────────────────

public sealed record AddPartitionsTopic(string Topic, IReadOnlyList<int> Partitions);

public sealed record AddPartitionsToTxnCommand
{
    public required string? TransactionalId { get; init; }
    public required long ProducerId { get; init; }
    public required short ProducerEpoch { get; init; }
    public required IReadOnlyList<AddPartitionsTopic> Topics { get; init; }
}

public sealed record TxnPartitionStatus(int Partition, TxnErrorStatus Status);

public sealed record AddPartitionsTopicResult(string Topic, IReadOnlyList<TxnPartitionStatus> Partitions);

public sealed record AddPartitionsToTxnResult(IReadOnlyList<AddPartitionsTopicResult> Topics);

// ── AddOffsetsToTxn ─────────────────────────────────────────────────────────

public sealed record AddOffsetsToTxnCommand
{
    public required string? TransactionalId { get; init; }
    public required long ProducerId { get; init; }
    public required short ProducerEpoch { get; init; }
    public required string GroupId { get; init; }
}

public sealed record AddOffsetsToTxnResult(TxnErrorStatus Status);

// ── TxnOffsetCommit ─────────────────────────────────────────────────────────

/// <summary>A (partition, offset, metadata) tuple to stage for a transactional commit.</summary>
public sealed record TxnOffsetCommitPartition(int Partition, long CommittedOffset, string? Metadata);

/// <summary>
/// A topic within a TxnOffsetCommit. Exactly one of <see cref="Name"/> / <see cref="TopicId"/>
/// is populated on the wire (KIP-1319 v6 sends only the id); the coordinator resolves the pair.
/// </summary>
public sealed record TxnOffsetCommitTopic
{
    public required string? Name { get; init; }
    public required Guid TopicId { get; init; }
    public required IReadOnlyList<TxnOffsetCommitPartition> Partitions { get; init; }
}

public sealed record TxnOffsetCommitCommand
{
    public required string? TransactionalId { get; init; }
    public required string GroupId { get; init; }
    public required long ProducerId { get; init; }
    public required short ProducerEpoch { get; init; }
    public required IReadOnlyList<TxnOffsetCommitTopic> Topics { get; init; }
}

public sealed record TxnOffsetCommitPartitionResult(int Partition, TxnErrorStatus Status);

public sealed record TxnOffsetCommitTopicResult
{
    public required string? Name { get; init; }
    public required Guid TopicId { get; init; }
    public required IReadOnlyList<TxnOffsetCommitPartitionResult> Partitions { get; init; }
}

public sealed record TxnOffsetCommitResult(IReadOnlyList<TxnOffsetCommitTopicResult> Topics);

// ── EndTxn ──────────────────────────────────────────────────────────────────

public sealed record EndTxnCommand
{
    public required string? TransactionalId { get; init; }
    public required long ProducerId { get; init; }
    public required short ProducerEpoch { get; init; }
    public required bool Committed { get; init; }
}

public sealed record EndTxnResult(TxnErrorStatus Status);

// ── DescribeProducers (KIP-664) ─────────────────────────────────────────────

public sealed record DescribeProducersTopic(string Name, IReadOnlyList<int> PartitionIndexes);

public sealed record DescribeProducersCommand(IReadOnlyList<DescribeProducersTopic> Topics);

public sealed record TxnProducerState
{
    public required long ProducerId { get; init; }
    public required short ProducerEpoch { get; init; }
    public required int LastSequence { get; init; }
    public required long LastTimestamp { get; init; }
    public required int CoordinatorEpoch { get; init; }
    public required long CurrentTxnStartOffset { get; init; }
}

public sealed record DescribeProducersPartitionResult
{
    public required int PartitionIndex { get; init; }
    public required TxnErrorStatus Status { get; init; }
    public required string? ErrorMessage { get; init; }
    public required IReadOnlyList<TxnProducerState> ActiveProducers { get; init; }
}

public sealed record DescribeProducersTopicResult(string Name, IReadOnlyList<DescribeProducersPartitionResult> Partitions);

public sealed record DescribeProducersResult(IReadOnlyList<DescribeProducersTopicResult> Topics);
