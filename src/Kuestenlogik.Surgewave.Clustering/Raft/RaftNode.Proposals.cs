namespace Kuestenlogik.Surgewave.Clustering.Raft;

public sealed partial class RaftNode
{
    /// <summary>
    /// Waits until the entry this node proposed at <paramref name="index"/> has committed: the one that carries
    /// <paramref name="data"/>, the very array handed to <see cref="ProposeAsync"/>. False when it did not commit
    /// within <paramref name="timeout"/>, and when another leader's entry stands at the index.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="WaitForCommitAsync"/> says that the commit index reached an index, not whose entry is there. A
    /// leader that loses its office with an entry proposed and not yet on a majority has that entry cut off by its
    /// successor, which writes its own from that index on; the commit index then passes the index all the same, and
    /// the proposer took that for its own entry (#268): the controller answered "created" for a topic that is in no
    /// broker's metadata log, became the leader of its partitions on its own, and the broker that had asked for the
    /// topic waited for it to arrive for as long as it ran.
    /// </para>
    /// <para>
    /// The entry is known by its bytes rather than by its term: the node writes the array it is given into its log
    /// as it is, an entry that survives a change of leader stays that object, and one that replaces it comes off the
    /// wire with an array of its own. The term the entry was written in is not handed to the proposer.
    /// </para>
    /// </remarks>
    public async Task<bool> WaitForCommitOfAsync(long index, byte[] data, TimeSpan timeout, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(data);

        if (!await WaitForCommitAsync(index, timeout, ct).ConfigureAwait(false))
            return false;

        lock (_lock)
        {
            return GetLogEntry(index) is { } entry && ReferenceEquals(entry.Data, data);
        }
    }
}
