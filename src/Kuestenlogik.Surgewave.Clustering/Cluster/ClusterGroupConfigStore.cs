using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Coordination.Coordinators;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// The groups' configs of a cluster (#228): read from this broker's copy of the metadata log, written through the
/// controller — this broker when it is the controller, else the controller it asks.
/// </summary>
/// <remarks>
/// A change is answered once it is committed; this broker's own copy has it once the entry reached it, as for every
/// other entry of the metadata log.
/// </remarks>
public sealed class ClusterGroupConfigStore(ClusterState clusterState, IClusterGroupConfigWriter controller) : IGroupConfigStore
{
    /// <inheritdoc />
    public IReadOnlyDictionary<string, string> ConfigsOf(string groupId) => clusterState.GroupConfigsOf(groupId);

    /// <inheritdoc />
    public async Task<GroupConfigWriteStatus> SetAsync(string groupId, string name, string? value, CancellationToken cancellationToken) =>
        await controller.SetGroupConfigAsync(groupId, name, value, cancellationToken).ConfigureAwait(false)
            ? GroupConfigWriteStatus.Ok
            : GroupConfigWriteStatus.NotAvailable;
}
