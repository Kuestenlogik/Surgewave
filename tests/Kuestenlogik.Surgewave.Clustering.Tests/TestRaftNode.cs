using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// A single-node metadata log for tests that exercise <see cref="Replication.ClusterController"/>.
/// </summary>
/// <remarks>
/// The controller requires one since #163 step 3: metadata is a replicated log and the controller
/// role is Raft leadership, so a controller without a log is not a configuration that exists. A
/// stand-in that returned "leader" without a log would let these tests pass against a broker that
/// could not actually commit anything.
/// </remarks>
internal static class TestRaftNode
{
    /// <summary>
    /// Builds a Raft node for <paramref name="config"/> over a transport with no peers, so it
    /// elects itself and commits immediately.
    /// </summary>
    public static RaftNode ForSingleNode(ClusteringConfig config, ClusterState state, ClusterMembershipService membership)
    {
        config.RaftDataDirectory = Path.Combine(
            Path.GetTempPath(), "surgewave-test-raft-" + Guid.NewGuid().ToString("N"));

        // Fast enough that a test does not spend its budget waiting for an election it is not
        // about, slow enough to stay a real election rather than an assumption.
        config.RaftElectionTimeoutMinMs = 50;
        config.RaftElectionTimeoutMaxMs = 100;
        config.RaftHeartbeatIntervalMs = 25;
        config.RaftPeerDiscoveryTimeoutSeconds = 0;

        return new RaftNode(
            NullLogger<RaftNode>.Instance,
            config,
            new RaftPersistence(NullLogger<RaftPersistence>.Instance, config),
            new NoPeerTransport(),
            new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, state, membership));
    }

    /// <summary>Builds the membership service these tests do not otherwise care about.</summary>
    public static ClusterMembershipService NewMembership(ClusteringConfig config, ClusterState state)
        => new(
            new ClusterIdManager(config, NullLogger<ClusterIdManager>.Instance),
            state,
            NullLogger<ClusterMembershipService>.Instance);

    /// <summary>
    /// A transport with nobody on it: the node is its own majority, which is the shape a single
    /// broker actually runs in.
    /// </summary>
    private sealed class NoPeerTransport : IRaftTransport
    {
        public IReadOnlyList<int> GetPeerIds() => [];

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct) => Task.FromResult(false);

        public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
            => throw new NotSupportedException();

        public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
            => throw new NotSupportedException();

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
            => throw new NotSupportedException();
    }
}
