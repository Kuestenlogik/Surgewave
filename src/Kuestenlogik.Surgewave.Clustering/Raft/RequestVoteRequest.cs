namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Raft RPC: RequestVote request from candidate.
/// </summary>
public sealed record RequestVoteRequest(
    int Term,
    int CandidateId,
    long LastLogIndex,
    int LastLogTerm
);
