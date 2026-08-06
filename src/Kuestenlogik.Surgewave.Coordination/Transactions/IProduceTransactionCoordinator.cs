using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Coordination.Transactions;

/// <summary>
/// Protocol-neutral contract for the produce/fetch hot-path transaction surface that the
/// data-plane handler needs: idempotent-producer sequence validation, transactional-batch
/// tracking for LSO calculation, and the READ_COMMITTED / READ_UNCOMMITTED fetch filters.
/// Everything crossing the boundary is neutral (<see cref="ProduceSequenceStatus"/> +
/// <see cref="TopicPartition"/> / <c>long</c> / <c>List&lt;byte[]&gt;</c>), so the concrete
/// <c>TransactionIndex</c> stays entirely off the handler's surface (#59 b4-tier2). The two
/// filter members forward to the broker's <c>TransactionIndex</c>; the produce-hot-path
/// helpers are the same neutral b2 signatures already exposed by the coordinators.
/// </summary>
public interface IProduceTransactionCoordinator
{
    /// <summary>
    /// Validates a produce batch for idempotence WITHOUT recording it. Returns the neutral
    /// <see cref="ProduceSequenceCheck"/>; the Kafka Produce handler maps it to a wire error code
    /// at the boundary.
    /// </summary>
    /// <remarks>
    /// Validation is deliberately free of side effects, and <see cref="CommitProduceBatch"/> is what
    /// advances the producer's sequence — because a batch can still be refused after this call
    /// (durability admission, quota, a corrupt payload, a failed append). Advancing the sequence for
    /// a batch that is never written strands the producer: its retry carries the same sequence, is
    /// then a duplicate of something that does not exist, and the error is fatal.
    /// </remarks>
    /// <param name="lastOffsetDelta">
    /// The batch's last-offset delta, i.e. record count minus one. The producer's next batch starts
    /// at <c>baseSequence + lastOffsetDelta + 1</c>, so a broker that assumes one record per batch
    /// rejects every multi-record producer's second batch as out of order.
    /// </param>
    ProduceSequenceCheck ValidateProduceBatch(
        long producerId, short epoch, int baseSequence, int lastOffsetDelta, TopicPartition topicPartition);

    /// <summary>
    /// Records a batch that was actually appended, so a retransmit of it can be recognised and
    /// answered with <paramref name="baseOffset"/>.
    /// </summary>
    void CommitProduceBatch(
        long producerId, short epoch, int baseSequence, int lastOffsetDelta,
        TopicPartition topicPartition, long baseOffset);

    /// <summary>
    /// Records a transactional batch being written (produce path) so the Last Stable Offset
    /// can be tracked for READ_COMMITTED isolation.
    /// </summary>
    void RecordTransactionalBatch(TopicPartition partition, long producerId, long baseOffset);

    /// <summary>Filters record batches for READ_COMMITTED isolation.</summary>
    List<byte[]> FilterForReadCommitted(TopicPartition partition, List<byte[]> batches, long highWatermark);

    /// <summary>Filters record batches for READ_UNCOMMITTED isolation.</summary>
    List<byte[]> FilterForReadUncommitted(List<byte[]> batches);
}
