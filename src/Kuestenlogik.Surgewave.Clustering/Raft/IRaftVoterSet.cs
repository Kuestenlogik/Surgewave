namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Who votes in this Raft cluster (#166).
/// </summary>
/// <remarks>
/// <para>
/// <see cref="RaftNode"/> used to ask <see cref="IRaftTransport.GetPeerIds"/>, which answers
/// "who can I reach" and was doubling as "who decides". Those are different questions, and
/// conflating them is why the quorum is currently every broker: membership fell out of the
/// transport's view of the cluster rather than being stated anywhere.
/// </para>
/// <para>
/// Deliberately narrower than <see cref="RaftConfiguration"/>, which is the model a
/// controller quorum will be configured as. That type carries a directory id and listener
/// set per voter, and a transport-derived implementation knows neither — filling them with
/// placeholders would put fake data into a type whose whole purpose is to be authoritative.
/// This interface asks for what the node actually needs; the configuration becomes the
/// source behind it when there is one to read.
/// </para>
/// <para>
/// Queried rather than snapshotted, because membership is dynamic today: a broker that
/// registers becomes a voter, and the node re-reads the set as it goes. An implementation
/// backed by a fixed controller quorum simply returns the same answer every time.
/// </para>
/// </remarks>
public interface IRaftVoterSet
{
    /// <summary>Every voter's node id, including this node.</summary>
    IReadOnlyList<int> VoterIds { get; }

    /// <summary>
    /// Votes needed to decide, <c>(count / 2) + 1</c> — the strict majority, counted over
    /// voters rather than over everything the transport can see.
    /// </summary>
    int Majority { get; }
}
