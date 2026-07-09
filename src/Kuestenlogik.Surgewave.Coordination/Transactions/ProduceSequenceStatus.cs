namespace Kuestenlogik.Surgewave.Coordination.Transactions;

/// <summary>
/// Protocol-neutral outcome of idempotent-producer sequence validation on the produce
/// hot path. Mirrors the <see cref="TxnErrorStatus"/> neutralization: the coordinator
/// surface no longer speaks the Kafka <c>ErrorCode</c>, so the Kafka Produce handler
/// maps this to a wire error code at the boundary and other protocols map their own.
/// </summary>
public enum ProduceSequenceStatus
{
    /// <summary>The batch's sequence is valid (or the producer is non-idempotent).</summary>
    Ok,

    /// <summary>The producer epoch in the request is older than the current incarnation (fenced zombie).</summary>
    InvalidProducerEpoch,

    /// <summary>The producer id is unknown, or a newer epoch was seen for it.</summary>
    UnknownProducerId,

    /// <summary>The batch's base sequence equals the last accepted one — a retransmit.</summary>
    DuplicateSequence,

    /// <summary>The batch's base sequence is neither the expected next one nor a duplicate.</summary>
    OutOfOrderSequence,
}
