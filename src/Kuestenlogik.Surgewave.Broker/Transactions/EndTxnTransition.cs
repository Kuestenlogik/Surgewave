using Kuestenlogik.Surgewave.Coordination.Transactions;
using TransactionState = Kuestenlogik.Surgewave.Core.KafkaConstants.TransactionState;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>What EndTxn does with a transaction.</summary>
internal enum EndTxnOutcome
{
    /// <summary>Refused with <see cref="EndTxnDecision.Error"/>; nothing changes.</summary>
    Refuse,

    /// <summary>A retry of an EndTxn that already completed: answer success with the current producer id and epoch.</summary>
    AlreadyDone,

    /// <summary>Prepare, write the markers, complete. <see cref="EndTxnDecision.Empty"/>: no partition was added, so no markers.</summary>
    Complete,
}

internal readonly record struct EndTxnDecision(EndTxnOutcome Outcome, TxnErrorStatus Error = TxnErrorStatus.None, bool Empty = false)
{
    public static EndTxnDecision Refuse(TxnErrorStatus error) => new(EndTxnOutcome.Refuse, error);
    public static readonly EndTxnDecision AlreadyDone = new(EndTxnOutcome.AlreadyDone);
    public static EndTxnDecision Complete(bool empty) => new(EndTxnOutcome.Complete, Empty: empty);
}

/// <summary>
/// EndTxn's answer for a transaction in a given state, as Kafka's coordinator gives it
/// (<c>TransactionCoordinator.endTransactionWithTV1</c> and <c>endTransaction</c>). A pure function of
/// the transaction and the request, so the table is testable on its own.
/// </summary>
/// <remarks>
/// <para><b>Transactions V1</b>: the epoch is the session's and must match exactly. A retry of a completed
/// EndTxn is a success, an EndTxn while the markers are being written is CONCURRENT_TRANSACTIONS (the client
/// retries), anything else out of order is INVALID_TXN_STATE.</para>
/// <para><b>Transactions V2</b>: every EndTxn bumps the epoch, so a retry carries the epoch one below the
/// current one (or, across a producer-id rotation, <c>MaxValue - 1</c> on the previous id). The same epoch
/// on a completed transaction is the next transaction, which may be empty. A COMMIT retried at the
/// pre-abort epoch on an aborted transaction means the coordinator aborted it (timeout) while the commit
/// was in flight: PRODUCER_FENCED, which the client recovers from, not the fatal INVALID_TXN_STATE
/// (KAFKA-20785).</para>
/// </remarks>
internal static class EndTxnTransition
{
    public static EndTxnDecision Decide(
        TransactionMetadata txn, long producerId, short producerEpoch, bool commit, TransactionVersion version)
        => version >= TransactionVersion.Tv2
            ? DecideV2(txn, producerId, producerEpoch, commit)
            : DecideV1(txn, producerId, producerEpoch, commit);

    private static EndTxnDecision DecideV1(TransactionMetadata txn, long producerId, short producerEpoch, bool commit)
    {
        if (producerId != txn.ProducerId)
            return EndTxnDecision.Refuse(TxnErrorStatus.InvalidProducerIdMapping);
        if (producerEpoch != txn.ProducerEpoch)
            return EndTxnDecision.Refuse(TxnErrorStatus.ProducerFenced);

        return txn.State switch
        {
            TransactionState.Ongoing => EndTxnDecision.Complete(empty: false),
            TransactionState.CompleteCommit => commit ? EndTxnDecision.AlreadyDone : InvalidTransition,
            TransactionState.CompleteAbort => !commit ? EndTxnDecision.AlreadyDone : InvalidTransition,
            TransactionState.PrepareCommit => commit ? Concurrent : InvalidTransition,
            TransactionState.PrepareAbort => !commit ? Concurrent : InvalidTransition,
            _ => InvalidTransition, // Empty: nothing to end; Dead: the transactional id is gone
        };
    }

    private static EndTxnDecision DecideV2(TransactionMetadata txn, long producerId, short producerEpoch, bool commit)
    {
        // A retry across a producer-id rotation: the client still holds the old id at the last epoch it saw.
        var retryOnOverflow = txn.PreviousProducerId == producerId
            && producerEpoch == short.MaxValue - 1
            && txn.ProducerEpoch == 0;
        // A retry of an EndTxn whose bump the client never saw.
        var retryOnEpochBump = txn.ProducerEpoch == producerEpoch + 1;
        var isRetry = retryOnEpochBump || retryOnOverflow;

        if (producerId != txn.ProducerId && !retryOnOverflow)
            return EndTxnDecision.Refuse(TxnErrorStatus.InvalidProducerIdMapping);

        var validEpoch = txn.State switch
        {
            TransactionState.PrepareCommit or TransactionState.PrepareAbort => retryOnEpochBump,
            TransactionState.CompleteCommit or TransactionState.CompleteAbort => isRetry || producerEpoch == txn.ProducerEpoch,
            _ => producerEpoch == txn.ProducerEpoch,
        };
        if (!validEpoch)
            return EndTxnDecision.Refuse(TxnErrorStatus.ProducerFenced);

        return txn.State switch
        {
            TransactionState.Ongoing => EndTxnDecision.Complete(empty: false),

            TransactionState.CompleteCommit => commit
                ? (isRetry ? EndTxnDecision.AlreadyDone : EndTxnDecision.Complete(empty: true))
                : (isRetry ? InvalidTransition : EndTxnDecision.Complete(empty: true)),

            TransactionState.CompleteAbort => commit
                // The commit raced a coordinator-side abort and never took effect (KAFKA-20785).
                ? (isRetry ? EndTxnDecision.Refuse(TxnErrorStatus.ProducerFenced) : EndTxnDecision.Complete(empty: true))
                : (isRetry ? EndTxnDecision.AlreadyDone : EndTxnDecision.Complete(empty: true)),

            TransactionState.PrepareCommit => commit ? Concurrent : InvalidTransition,
            TransactionState.PrepareAbort => !commit ? Concurrent : InvalidTransition,

            // V2 may end a transaction that added nothing: the epoch is bumped all the same.
            TransactionState.Empty => EndTxnDecision.Complete(empty: true),

            _ => InvalidTransition, // Dead
        };
    }

    private static readonly EndTxnDecision InvalidTransition = EndTxnDecision.Refuse(TxnErrorStatus.InvalidTxnState);
    private static readonly EndTxnDecision Concurrent = EndTxnDecision.Refuse(TxnErrorStatus.ConcurrentTransactions);
}
