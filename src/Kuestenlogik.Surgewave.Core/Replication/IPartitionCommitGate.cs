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
/// <para>Deliberately admission only, for now. Answering "yes" here means the partition has enough
/// in-sync replicas to be ABLE to commit the write; it does not mean the write has been committed.
/// Waiting for the commit is the next step and needs the produce path to stop mutating idempotence
/// sequence state before a write is admitted — see #122.</para>
/// </remarks>
public interface IPartitionCommitGate
{
    /// <summary>
    /// Whether <paramref name="partition"/> currently has enough in-sync replicas to accept a
    /// durable write. A refusal must be answered before anything is appended and before any
    /// per-producer state is advanced: the write does not happen at all.
    /// </summary>
    bool CanAdmitDurableWrite(in TopicPartition partition);
}
