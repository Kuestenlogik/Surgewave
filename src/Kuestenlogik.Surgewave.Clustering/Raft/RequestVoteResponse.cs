namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Raft RPC: RequestVote response.
/// </summary>
public sealed record RequestVoteResponse(
    int Term,
    bool VoteGranted
);
