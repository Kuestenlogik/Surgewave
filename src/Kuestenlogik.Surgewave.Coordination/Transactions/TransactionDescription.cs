namespace Kuestenlogik.Surgewave.Coordination.Transactions;

/// <summary>
/// Protocol-neutral transaction description projection (DescribeTransactions surface). The
/// <see cref="ErrorCode"/> is the numeric Kafka error code (0 = none, 59 = UnknownProducerId)
/// carried through as an int so no Kafka type leaks into the contract (#59).
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
