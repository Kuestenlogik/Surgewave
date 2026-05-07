namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Raft RPC: AppendEntries request from leader.
/// </summary>
public sealed record AppendEntriesRequest(
    int Term,
    int LeaderId,
    long PrevLogIndex,
    int PrevLogTerm,
    RaftLogEntry[] Entries,
    long LeaderCommit
);
