using System.Text.Json;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

public sealed partial class ClusterController
{
    private static readonly TimeSpan ClusterIdCommitTimeout = TimeSpan.FromSeconds(5);

    private ClusterIdManager? _clusterIdentity;

    /// <summary>
    /// Sets this broker's cluster id, which it writes to the metadata log when it is the first controller of a
    /// cluster (#203). Absent it, the controller writes none, and no other broker can register.
    /// </summary>
    public void SetClusterIdentity(ClusterIdManager clusterIdentity) => _clusterIdentity = clusterIdentity;

    // What a new controller writes first: the cluster's id, so that no registration precedes it in the log, then its
    // own registration.
    private async Task TakeOfficeAsync(CancellationToken ct)
    {
        await EnsureClusterIdAsync(ct).ConfigureAwait(false);
        await RegisterBrokerViaRaftAsync(
            _config.BrokerId, _config.Host, _config.Port, _config.Rack,
            _raftSelfIncarnation, InterBrokerProtocolFeature.LocalMaxSupported, _config.ReplicationPort, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes the cluster's id to the metadata log unless an earlier controller did: the configured id, the one this
    /// broker persisted, or a new one (#203). Kafka writes it once, when the storage is formatted; here the first
    /// controller of a cluster does, and every later one finds it in the log.
    /// </summary>
    /// <remarks>
    /// Whether the log has the id is known only once everything committed before this term is applied, which is when
    /// the log up to the index this controller took office at commits: the new leader's log holds every committed
    /// entry. Apply keeps the first id, so even a second entry could not give the cluster two.
    /// </remarks>
    private async Task EnsureClusterIdAsync(CancellationToken ct)
    {
        if (_clusterIdentity is null || _raftNode is null)
            return;

        try
        {
            while (!ct.IsCancellationRequested && _raftNode.IsLeader)
            {
                if (!await _raftNode.WaitForCommitAsync(_raftNode.LastLogIndex, ClusterIdCommitTimeout, ct).ConfigureAwait(false))
                    continue;

                if (_clusterState.ClusterId is { } committed)
                {
                    if (!_clusterIdentity.IsOwn(committed))
                        LogControllerClusterIdConflict(_clusterIdentity.ClusterId ?? "", committed);
                    return;
                }

                var clusterId = _clusterIdentity.ProposeClusterId();
                var data = JsonSerializer.SerializeToUtf8Bytes(
                    new ClusterIdSetCommand(clusterId), ClusteringJsonContext.Default.ClusterIdSetCommand);
                var index = await ProposeAsync(MetadataCommandType.ClusterIdSet, data, ct).ConfigureAwait(false);
                if (index < 0)
                    return;

                if (await _raftNode.WaitForCommitAsync(index, ClusterIdCommitTimeout, ct).ConfigureAwait(false))
                {
                    LogClusterIdWritten(clusterId);
                    return;
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutting down; the next controller writes it.
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Wrote cluster id {ClusterId} to the metadata log")]
    private partial void LogClusterIdWritten(string clusterId);

    [LoggerMessage(Level = LogLevel.Error,
        Message = "This controller's cluster id {Own} is not {Committed}, the id in the metadata log: it belongs to another cluster")]
    private partial void LogControllerClusterIdConflict(string own, string committed);
}
