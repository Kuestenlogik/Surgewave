using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// Stands in for the metadata log when a test needs a registration to commit (#171).
/// </summary>
/// <remarks>
/// Applies the registration exactly as <c>MetadataStateMachine.ApplyBrokerRegistered</c> does — at
/// the next log index, with a zero replication port read as "none advertised" — so a test exercises
/// the same apply the cluster does, without a Raft node and a temp directory per case.
/// </remarks>
internal sealed class TestBrokerRegistrar : IBrokerRegistrar
{
    private readonly ClusterMembershipService _membership;
    private long _nextIndex = 1;

    public TestBrokerRegistrar(ClusterMembershipService membership, bool isController = true)
    {
        _membership = membership;
        IsController = isController;
    }

    public bool IsController { get; set; }

    /// <summary>Whether a proposal commits — false is a controller that lost the log mid-flight.</summary>
    public bool Commits { get; set; } = true;

    public Task<bool> RegisterBrokerViaRaftAsync(
        int brokerId, string host, int port, string? rack, Guid incarnationId,
        short interBrokerProtocol, int replicationPort, CancellationToken ct)
    {
        if (!IsController || !Commits)
            return Task.FromResult(false);

        _membership.ApplyReplicatedRegistration(
            brokerId, incarnationId, epoch: _nextIndex++, host, port, rack,
            interBrokerProtocol, replicationPort == 0 ? null : replicationPort);

        return Task.FromResult(true);
    }
}
