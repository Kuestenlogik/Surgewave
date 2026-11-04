using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Coordination.Transactions;

/// <summary>
/// Asks a transactional id's coordinator about a partition of the transaction (KIP-890): whether it was added
/// (<see cref="TransactionVersion.Tv1"/> and older), or to add it (<see cref="TransactionVersion.Tv2"/>). A partition
/// leader asks before it appends a transactional batch, as Kafka's leaders send AddPartitionsToTxn v4+ to the
/// coordinator; in a cluster the coordinator is mostly another broker (#197).
/// </summary>
public interface ITransactionPartitionVerifier
{
    /// <summary>
    /// The coordinator's answer, as <see cref="IProduceTransactionCoordinator.VerifyTransactionalBatchAsync"/> gives
    /// it; <see cref="TxnErrorStatus.NotCoordinator"/> or <see cref="TxnErrorStatus.CoordinatorNotAvailable"/> when the
    /// coordinator could not be asked.
    /// </summary>
    Task<TxnErrorStatus> VerifyPartitionAsync(
        string transactionalId, long producerId, short producerEpoch, TopicPartition partition, TransactionVersion version,
        CancellationToken cancellationToken);
}
