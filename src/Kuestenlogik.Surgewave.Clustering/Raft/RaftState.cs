namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// The state of a Raft node in the consensus protocol.
/// </summary>
public enum RaftState
{
    /// <summary>
    /// Follower state - accepts log entries from leader.
    /// </summary>
    Follower,

    /// <summary>
    /// Candidate state - requesting votes to become leader.
    /// </summary>
    Candidate,

    /// <summary>
    /// Leader state - replicates log entries to followers.
    /// </summary>
    Leader
}
