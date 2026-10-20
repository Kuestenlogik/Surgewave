namespace Kuestenlogik.Surgewave.Coordination.Transactions;

/// <summary>
/// Protocol-neutral outcome for a transaction-coordinator operation. The adapter maps
/// these onto Kafka wire error codes; the coordinator never references a Kafka
/// <c>ErrorCode</c> for its wire-API surface (#59).
/// </summary>
public enum TxnErrorStatus
{
    /// <summary>Operation succeeded.</summary>
    None,

    /// <summary>The producer epoch in the request is older than the current incarnation (fenced zombie).</summary>
    InvalidProducerEpoch,

    /// <summary>The transaction is in a state that does not permit this operation.</summary>
    InvalidTxnState,

    /// <summary>The transactional id / producer id is unknown to the coordinator.</summary>
    UnknownProducerId,

    /// <summary>A transaction is already in progress for this producer; the requested operation conflicts with it.</summary>
    ConcurrentTransactions,

    /// <summary>A supplied topic id could not be resolved to a topic name.</summary>
    UnknownTopicId,

    /// <summary>
    /// The request carries an epoch other than the transaction's current one: another producer with the
    /// same transactional id took over, or the coordinator aborted the transaction (timeout) and bumped
    /// the epoch. Recoverable for the client, unlike <see cref="InvalidTxnState"/>.
    /// </summary>
    ProducerFenced,

    /// <summary>The producer id does not belong to the transactional id, or the transactional id is unknown.</summary>
    InvalidProducerIdMapping,

    /// <summary>The partition or group is not part of the transaction; the client aborts it and may retry.</summary>
    TransactionAbortable,

    /// <summary>The request is malformed, e.g. a transactional request without a transactional id.</summary>
    InvalidRequest,
}
