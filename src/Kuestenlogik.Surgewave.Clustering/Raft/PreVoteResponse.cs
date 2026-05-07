namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Raft Pre-Vote response.
///
/// A peer grants a pre-vote if:
/// 1. The proposed term is greater than or equal to the peer's current term
/// 2. The candidate's log is at least as up-to-date as the peer's log
/// 3. The peer hasn't heard from a valid leader recently (prevents disruption)
/// </summary>
/// <param name="Term">The peer's current term</param>
/// <param name="VoteGranted">Whether the peer would vote for this candidate in a real election</param>
public sealed record PreVoteResponse(
    int Term,
    bool VoteGranted
);
