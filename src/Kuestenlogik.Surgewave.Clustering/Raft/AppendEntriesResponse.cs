namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Raft RPC: AppendEntries response.
/// </summary>
public sealed record AppendEntriesResponse(
    int Term,
    bool Success,
    long MatchIndex
);
