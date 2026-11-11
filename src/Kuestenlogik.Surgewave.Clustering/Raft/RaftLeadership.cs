namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Who leads the metadata log, as one Raft node knows it at one moment.
/// </summary>
/// <param name="Term">The node's current term.</param>
/// <param name="LeaderId">The leader the node knows in that term, or <c>null</c> while it knows none.</param>
/// <param name="IsLeader">Whether the node itself leads the term.</param>
public readonly record struct RaftLeadership(int Term, int? LeaderId, bool IsLeader);
