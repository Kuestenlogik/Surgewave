using Kuestenlogik.Surgewave.Clustering.Raft;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// A Raft transport with no peers, so a single voter elects itself and commits on its own — for tests that need
/// a real controller (proposing, committing, applying) without a cluster.
/// </summary>
internal sealed class SingleNodeRaftTransport : IRaftTransport
{
    public IReadOnlyList<int> GetPeerIds() => [];

    public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct) => Task.FromResult(false);

    public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
        => throw new NotSupportedException("no peers in a single-node test");

    public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
        => throw new NotSupportedException("no peers in a single-node test");

    public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
        => throw new NotSupportedException("no peers in a single-node test");
}
