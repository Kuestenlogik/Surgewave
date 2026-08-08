using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Core.Replication;

/// <summary>
/// Decides whether a partition can currently honour a write that asks for replicated durability.
/// </summary>
/// <remarks>
/// <para>Lives in Core, and is protocol-neutral on purpose: durability is a property of the log, not
/// of the Kafka wire. Both produce paths ask the same question, so a native client gets the same
/// answer as a Kafka one — a durability guarantee that only existed on the compat layer would be the
/// wrong shape for this broker.</para>
///
/// <para>Two questions, asked at two different moments. <see cref="CanAdmitDurableWrite"/> runs
/// before the append and decides whether the partition is ABLE to commit the write.
/// <see cref="WaitForDurableCommitAsync"/> runs after it and waits until the write actually HAS
/// been committed. Admission alone is not durability: it only says enough replicas were in sync at
/// the moment we looked.</para>
/// </remarks>
public interface IPartitionCommitGate
{
    /// <summary>
    /// Whether <paramref name="partition"/> currently has enough in-sync replicas to accept a
    /// durable write. A refusal must be answered before anything is appended and before any
    /// per-producer state is advanced: the write does not happen at all.
    /// </summary>
    bool CanAdmitDurableWrite(in TopicPartition partition);

    /// <summary>
    /// Waits until every in-sync replica of <paramref name="partition"/> holds the records below
    /// <paramref name="committedThroughOffset"/>, or until <paramref name="timeout"/> elapses.
    /// Returns false on timeout, which means the append happened but replication was not confirmed.
    /// </summary>
    /// <param name="committedThroughOffset">
    /// EXCLUSIVE end of the appended batch, i.e. base offset + record count. The high watermark
    /// counts the next offset to be committed, so a batch ending at offset N is replicated once the
    /// watermark reaches N+1 — passing the batch's last offset instead would release the producer
    /// one record early, which is the same silent under-guarantee this gate exists to remove.
    /// </param>
    /// <remarks>
    /// The default returns true: an implementation that does not track replication has nobody to
    /// wait for, and for it the admission answer is the whole guarantee. Only implementations that
    /// actually observe replica progress should override this.
    /// </remarks>
    ValueTask<bool> WaitForDurableCommitAsync(
        TopicPartition partition,
        long committedThroughOffset,
        TimeSpan timeout,
        CancellationToken cancellationToken) => ValueTask.FromResult(true);
}
