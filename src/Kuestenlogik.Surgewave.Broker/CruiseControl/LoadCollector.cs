using Kuestenlogik.Surgewave.Clustering.Cluster;

namespace Kuestenlogik.Surgewave.Broker.CruiseControl;

/// <summary>
/// Collects per-broker load metrics from the cluster state and broker metrics.
/// Produces <see cref="BrokerLoadSnapshot"/> instances for balance analysis.
/// </summary>
public sealed class LoadCollector
{
    private readonly ClusterState _clusterState;

    /// <summary>
    /// Initializes a new instance of <see cref="LoadCollector"/>.
    /// </summary>
    /// <param name="clusterState">The cluster state providing broker and partition information.</param>
    public LoadCollector(ClusterState clusterState)
    {
        _clusterState = clusterState ?? throw new ArgumentNullException(nameof(clusterState));
    }

    /// <summary>
    /// Collect load snapshots for all brokers currently in the cluster.
    /// Derives partition counts, leader counts, and estimated disk usage from the cluster state.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A list of per-broker load snapshots.</returns>
    public Task<IReadOnlyList<BrokerLoadSnapshot>> CollectAsync(CancellationToken ct = default)
    {
        var brokerIds = _clusterState.Brokers.Keys.ToList();
        var partitionStates = _clusterState.PartitionStates;
        var now = DateTimeOffset.UtcNow;

        // Count partitions and leaders per broker
        var partitionCounts = new Dictionary<int, int>();
        var leaderCounts = new Dictionary<int, int>();

        foreach (var brokerId in brokerIds)
        {
            partitionCounts[brokerId] = 0;
            leaderCounts[brokerId] = 0;
        }

        foreach (var (_, state) in partitionStates)
        {
            // Count partition assignments (replicas)
            foreach (var replicaId in state.Replicas)
            {
                if (partitionCounts.TryGetValue(replicaId, out var pCount))
                    partitionCounts[replicaId] = pCount + 1;
            }

            // Count leader assignments
            if (leaderCounts.TryGetValue(state.LeaderBrokerId, out var lCount))
                leaderCounts[state.LeaderBrokerId] = lCount + 1;
        }

        var snapshots = new List<BrokerLoadSnapshot>(brokerIds.Count);

        foreach (var brokerId in brokerIds)
        {
            snapshots.Add(new BrokerLoadSnapshot
            {
                BrokerId = brokerId,
                PartitionCount = partitionCounts.GetValueOrDefault(brokerId),
                LeaderCount = leaderCounts.GetValueOrDefault(brokerId),
                DiskUsageBytes = 0, // Would be populated from storage metrics in a real multi-broker deployment
                ProduceRateBytesPerSec = 0,
                ConsumeRateBytesPerSec = 0,
                CpuPercent = 0,
                NetworkUtilizationPercent = 0,
                Timestamp = now
            });
        }

        return Task.FromResult<IReadOnlyList<BrokerLoadSnapshot>>(snapshots);
    }
}
