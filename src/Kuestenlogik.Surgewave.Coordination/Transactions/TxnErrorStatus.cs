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

    /// <summary>A supplied topic id could not be resolved to a topic name.</summary>
    UnknownTopicId,
}
