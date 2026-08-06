namespace Kuestenlogik.Surgewave.Coordination.Transactions;

/// <summary>
/// The verdict on an idempotent producer's batch, plus what the broker owes the producer when the
/// batch turns out to be one it has already written.
/// </summary>
/// <param name="Status">Whether the batch may be appended.</param>
/// <param name="DuplicateBaseOffset">
/// For <see cref="ProduceSequenceStatus.DuplicateSequence"/>, the offset the original batch was
/// written at, or -1 if that is no longer known. A retransmit is the normal, expected outcome of a
/// producer that did not see its acknowledgement, and answering it with the original offset and
/// success is what makes an idempotent retry safe — answering it with an error is not, because
/// duplicate/out-of-order sequence errors are fatal to both the Java producer and librdkafka.
/// </param>
public readonly record struct ProduceSequenceCheck(
    ProduceSequenceStatus Status,
    long DuplicateBaseOffset = -1)
{
    public static ProduceSequenceCheck Ok => new(ProduceSequenceStatus.Ok);

    public static ProduceSequenceCheck Duplicate(long baseOffset)
        => new(ProduceSequenceStatus.DuplicateSequence, baseOffset);

    public static ProduceSequenceCheck Failed(ProduceSequenceStatus status) => new(status);
}
