namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Transport layer for Raft RPC communication between nodes.
/// </summary>
public interface IRaftTransport
{
    /// <summary>
    /// Get the list of peer node IDs in the cluster.
    /// </summary>
    IReadOnlyList<int> GetPeerIds();

    /// <summary>
    /// Send a PreVote RPC to a peer (Pre-Vote protocol extension).
    /// Pre-Vote prevents disruptive elections by checking if peers would vote
    /// before actually incrementing the term.
    /// </summary>
    Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct);

    /// <summary>
    /// Send a RequestVote RPC to a peer.
    /// </summary>
    Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct);

    /// <summary>
    /// Send an AppendEntries RPC to a peer.
    /// </summary>
    Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct);

    /// <summary>
    /// Check if a peer is reachable (can establish a TCP connection).
    /// Used during startup to wait for peers before enabling elections.
    /// </summary>
    Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct);
}

/// <summary>
/// State machine interface for applying committed log entries.
/// </summary>
public interface IRaftStateMachine
{
    /// <summary>
    /// Apply a committed log entry to the state machine.
    /// This should be idempotent - applying the same entry twice should have no effect.
    /// </summary>
    void Apply(RaftLogEntry entry);

    /// <summary>
    /// Create a snapshot of the current state machine state.
    /// </summary>
    Task<byte[]> CreateSnapshotAsync(CancellationToken ct);

    /// <summary>
    /// Restore state machine from a snapshot.
    /// </summary>
    Task RestoreFromSnapshotAsync(byte[] snapshot, CancellationToken ct);
}
