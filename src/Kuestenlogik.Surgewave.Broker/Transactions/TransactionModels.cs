using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Models;
using TransactionState = Kuestenlogik.Surgewave.Core.KafkaConstants.TransactionState;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

// TransactionListing and TransactionDescription (the neutral ListTransactions /
// DescribeTransactions projections) now live in Kuestenlogik.Surgewave.Coordination.Transactions
// so the neutral ITransactionCoordinator contract can expose them without a Kafka dependency (#59).

/// <summary>
/// Internal transaction metadata tracking state of an active transaction.
/// </summary>
/// <remarks>
/// Every read-modify-write of a transaction holds <see cref="Lock"/>. EndTxn and the timeout abort
/// release it while the markers are written: the transaction is then visibly in PrepareCommit /
/// PrepareAbort, so a concurrent request is answered CONCURRENT_TRANSACTIONS instead of waiting
/// for the markers or changing the transaction under them.
/// </remarks>
internal sealed class TransactionMetadata
{
    public required string TransactionalId { get; init; }
    public long ProducerId { get; set; }
    public short ProducerEpoch { get; set; }

    /// <summary>
    /// The epoch before the last bump (transactions V2 bump on every EndTxn), -1 when there was none.
    /// Persisted, so a retried request is recognised after a restart (Kafka's KAFKA-20357).
    /// </summary>
    public short LastProducerEpoch { get; set; } = -1;

    /// <summary>The producer id before the last rotation (epoch exhausted), -1 when there was none.</summary>
    public long PreviousProducerId { get; set; } = -1;

    /// <summary>The producer id the transaction completes onto when its epoch is exhausted, -1 otherwise.</summary>
    public long NextProducerId { get; set; } = -1;

    /// <summary>The transaction protocol the client last used for this transaction.</summary>
    public TransactionVersion ClientTransactionVersion { get; set; } = TransactionVersion.Tv0;

    public TransactionState State { get; set; }
    public int TransactionTimeoutMs { get; set; }
    public DateTimeOffset LastActivityTime { get; set; } = DateTimeOffset.UtcNow;

    /// <summary>When the transaction went Ongoing; the timeout counts from here, as Kafka's does.</summary>
    public DateTimeOffset TxnStartTime { get; set; } = DateTimeOffset.UtcNow;

    public HashSet<TopicPartition> Partitions { get; } = new();
    public HashSet<string> ConsumerGroups { get; } = new();

    public SemaphoreSlim Lock { get; } = new(1, 1);

    /// <summary>
    /// Markers of this prepared transaction are being written right now. Not persisted: a transaction found
    /// prepared after a restart has nobody completing it, and the expiry pass finishes it.
    /// </summary>
    public bool CompletionInProgress { get; set; }

    /// <summary>
    /// The partitions of this prepared transaction whose marker is written, so a pass that completes it writes only
    /// the others. Not persisted: a coordinator that takes the transaction over writes every marker again.
    /// </summary>
    public HashSet<TopicPartition> MarkersWritten { get; } = new();

    public bool IsTimedOut(DateTimeOffset now) => State == TransactionState.Ongoing &&
        TransactionTimeoutMs > 0 &&
        now - TxnStartTime > TimeSpan.FromMilliseconds(TransactionTimeoutMs);

    /// <summary>
    /// Kafka never hands out <see cref="short.MaxValue"/>, so the coordinator can always fence one more
    /// time; an epoch at <c>MaxValue - 1</c> or above is exhausted and the producer id rotates.
    /// </summary>
    public static bool IsEpochExhausted(short epoch) => epoch >= short.MaxValue - 1;
}
