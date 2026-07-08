namespace Kuestenlogik.Surgewave.Coordination.Transactions;

/// <summary>
/// Protocol-neutral contract for the transaction coordinator's wire-API surface: the producer /
/// transaction lifecycle (InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, TxnOffsetCommit,
/// EndTxn), the KIP-664 DescribeProducers introspection and the ListTransactions /
/// DescribeTransactions projections. The Kafka DTO conversion + wire envelope live in the
/// <c>TransactionApiHandler</c> adapter, so the coordinator references no Kafka type for these
/// methods (#59). Note the produce-hot-path helpers (ValidateProduceBatch / RecordTransactionalBatch)
/// stay concrete and are intentionally not part of this contract.
/// </summary>
public interface ITransactionCoordinator
{
    Task<InitProducerIdResult> InitProducerIdAsync(InitProducerIdCommand request, CancellationToken cancellationToken);

    AddPartitionsToTxnResult AddPartitionsToTxn(AddPartitionsToTxnCommand request);

    AddOffsetsToTxnResult AddOffsetsToTxn(AddOffsetsToTxnCommand request);

    TxnOffsetCommitResult TxnOffsetCommit(TxnOffsetCommitCommand request);

    Task<EndTxnResult> EndTxnAsync(EndTxnCommand request, CancellationToken cancellationToken);

    DescribeProducersResult DescribeProducers(DescribeProducersCommand request);

    /// <summary>Lists transactions with optional state / producer-id / duration / id-pattern filters.</summary>
    IReadOnlyList<TransactionListing> ListTransactions(
        IEnumerable<string>? statesFilter = null,
        IEnumerable<long>? producerIdFilter = null,
        long minDurationMs = -1,
        string? transactionalIdPattern = null);

    IReadOnlyList<TransactionDescription> DescribeTransactions(IEnumerable<string> transactionalIds);
}
