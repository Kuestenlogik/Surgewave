using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Coordination.Transactions;

/// <summary>
/// Protocol-neutral transaction-marker completion surface. The inter-broker WriteTxnMarkers
/// path (InterBrokerApiHandler, relocatable to the Kafka plugin) commits/aborts a producer's
/// transaction through this seam instead of naming the broker-internal <c>TransactionIndex</c>
/// directly (#59 b5). Distinct from the produce-hot-path <see cref="IProduceTransactionCoordinator"/>.
/// </summary>
public interface ITransactionMarkerSink
{
    /// <summary>Marks the producer's transaction committed across the given partitions.</summary>
    void CommitTransaction(long producerId, IEnumerable<TopicPartition> partitions, long commitOffset);

    /// <summary>Marks the producer's transaction aborted across the given partitions.</summary>
    void AbortTransaction(long producerId, IEnumerable<TopicPartition> partitions, long abortOffset);
}
