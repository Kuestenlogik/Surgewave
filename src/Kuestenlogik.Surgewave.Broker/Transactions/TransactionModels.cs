using Kuestenlogik.Surgewave.Core.Models;
using TransactionState = Kuestenlogik.Surgewave.Core.KafkaConstants.TransactionState;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// Transaction listing for ListTransactions API.
/// </summary>
public record TransactionListing(string TransactionalId, long ProducerId, string State);

/// <summary>
/// Transaction description for DescribeTransactions API.
/// </summary>
public record TransactionDescription(
    string TransactionalId,
    string State,
    long ProducerId,
    short ProducerEpoch,
    int TransactionTimeoutMs,
    long TransactionStartTimeMs,
    List<(string Topic, int Partition)> Partitions,
    int ErrorCode);

/// <summary>
/// Internal transaction metadata tracking state of an active transaction.
/// </summary>
internal sealed class TransactionMetadata
{
    public required string TransactionalId { get; init; }
    public long ProducerId { get; set; }
    public short ProducerEpoch { get; set; }
    public TransactionState State { get; set; }
    public int TransactionTimeoutMs { get; set; }
    public DateTimeOffset LastActivityTime { get; set; } = DateTimeOffset.UtcNow;
    public HashSet<TopicPartition> Partitions { get; } = new();
    public HashSet<string> ConsumerGroups { get; } = new();
    public List<PendingTxnOffset> PendingOffsets { get; } = new();

    public bool IsTimedOut => State == TransactionState.Ongoing &&
        TransactionTimeoutMs > 0 &&
        DateTimeOffset.UtcNow - LastActivityTime > TimeSpan.FromMilliseconds(TransactionTimeoutMs);
}

/// <summary>
/// Pending offset to be committed as part of a transaction.
/// </summary>
internal sealed class PendingTxnOffset
{
    public required string GroupId { get; init; }
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long Offset { get; init; }
    public string? Metadata { get; init; }
}
