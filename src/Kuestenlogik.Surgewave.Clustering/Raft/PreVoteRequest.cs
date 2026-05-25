namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Raft Pre-Vote request (Raft extension for preventing disruptive elections).
///
/// The Pre-Vote protocol prevents a partitioned node from incrementing its term
/// and disrupting the cluster when it rejoins. Before starting an election,
/// a candidate first asks peers if they would vote for it, without incrementing
/// its term.
/// </summary>
/// <param name="ProposedTerm">The term the candidate would use if it started an election (currentTerm + 1)</param>
/// <param name="CandidateId">The candidate's node ID</param>
/// <param name="LastLogIndex">Index of candidate's last log entry</param>
/// <param name="LastLogTerm">Term of candidate's last log entry</param>
public sealed record PreVoteRequest(
    int ProposedTerm,
    int CandidateId,
    long LastLogIndex,
    int LastLogTerm
);
