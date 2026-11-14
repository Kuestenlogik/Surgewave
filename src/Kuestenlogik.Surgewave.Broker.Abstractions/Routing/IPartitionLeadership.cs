using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Broker.Abstractions.Routing;

/// <summary>
/// Answers whether this broker knows that it does not lead a partition (#164): another broker does, or none.
/// </summary>
/// <remarks>
/// <para>
/// The client-facing produce path had no leadership check at all: a produce for a
/// partition this broker hosts was appended whether or not this broker led it. Kafka
/// answers <c>NOT_LEADER_OR_FOLLOWER</c> there, and that answer is what makes a client's
/// stale metadata self-correcting — it refreshes and retries against the real leader.
/// Without it, stale metadata is not corrected but acted on.
/// </para>
/// <para>
/// Deliberately phrased as the POSITIVE knowledge that this broker does not lead, not as "am
/// I the leader". A single-broker or embedded runtime has no cluster state and therefore no
/// partition states at all — they are written only by the clustering paths (the Raft state
/// machine, controller pushes, the replica manager). Asking "am I the leader" would answer
/// no there and refuse every write. Asking "do I know that I do not lead it" answers no, and
/// those deployments are untouched.
/// </para>
/// <para>
/// A partition the cluster knows and that has no leader is not led here either (#253): its
/// last in-sync replica is gone, and a replica that is not in sync appends nothing and serves
/// nothing until a leader is elected. It used to count as "mid-election" and was let through,
/// which was harmless while every election named a leader within the same entry.
/// </para>
/// <para>
/// The asymmetry is also the safe one under a stale view. Refusing when we are in fact the
/// leader costs a client one metadata refresh and a retry; accepting when we are not costs
/// records that the real leader will never have. So the check errs toward refusing, and is
/// silent only when it genuinely does not know.
/// </para>
/// <para>
/// It cannot catch a broker that is wrong about ITSELF — one demoted a moment ago whose
/// view has not caught up will still accept. Kafka has the same limit on this path:
/// <c>ProduceRequest</c> carries no leader epoch (only <c>FetchRequest</c> does, KIP-320),
/// so its check is the equally local <c>leaderLogIfLocal</c>. Closing that needs an epoch
/// on the produce path, which is a protocol change and not this.
/// </para>
/// </remarks>
public interface IPartitionLeadership
{
    /// <summary>
    /// <c>true</c> only when this broker knows the partition and does not lead it: another broker does, or the
    /// partition has no leader. Unknown, or led by us, answers <c>false</c>.
    /// </summary>
    bool IsNotLedHere(TopicPartition partition);

    /// <summary>The partition's leader and its epoch, null when this broker does not know one (#228).</summary>
    PartitionLeader? LeaderOf(TopicPartition partition);
}

/// <summary>A partition's leader as this broker knows it.</summary>
public readonly record struct PartitionLeader(int BrokerId, int LeaderEpoch);
