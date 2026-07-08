using Kuestenlogik.Surgewave.Core.Models;
using TransactionState = Kuestenlogik.Surgewave.Core.KafkaConstants.TransactionState;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

// TransactionListing and TransactionDescription (the neutral ListTransactions /
// DescribeTransactions projections) now live in Kuestenlogik.Surgewave.Coordination.Transactions
// so the neutral ITransactionCoordinator contract can expose them without a Kafka dependency (#59).

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
