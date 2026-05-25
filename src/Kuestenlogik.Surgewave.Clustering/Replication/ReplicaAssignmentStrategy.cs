using Kuestenlogik.Surgewave.Clustering.Cluster;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Strategies for assigning partition replicas to brokers.
/// Supports rack-aware placement for fault tolerance.
/// </summary>
public sealed class ReplicaAssignmentStrategy
{
    private readonly ClusterState _clusterState;

    public ReplicaAssignmentStrategy(ClusterState clusterState)
    {
        _clusterState = clusterState;
    }

    /// <summary>
    /// Assign replicas for a partition using rack-aware algorithm.
    /// Spreads replicas across different racks when possible.
    /// </summary>
    public List<int> AssignReplicas(List<int> brokerIds, int partition, short replicationFactor)
    {
        var count = Math.Min(replicationFactor, brokerIds.Count);

        // Group brokers by rack
        var brokersByRack = new Dictionary<string, List<int>>();
        foreach (var brokerId in brokerIds)
        {
            var broker = _clusterState.GetBroker(brokerId);
            var rack = broker?.Rack ?? "default";

            if (!brokersByRack.TryGetValue(rack, out var brokers))
            {
                brokers = [];
                brokersByRack[rack] = brokers;
            }
            brokers.Add(brokerId);
        }

        // If only one rack or no rack info, fall back to simple round-robin
        if (brokersByRack.Count <= 1)
        {
            return AssignReplicasRoundRobin(brokerIds, partition, count);
        }

        // Sort racks for deterministic assignment
        var racks = brokersByRack.Keys.OrderBy(r => r).ToList();

        var replicas = new List<int>();
        var usedBrokers = new HashSet<int>();

        // Start rack and broker offset based on partition for even distribution
        var rackOffset = partition % racks.Count;

        // Assign replicas by alternating racks
        for (int i = 0; i < count; i++)
        {
            var rackIndex = (rackOffset + i) % racks.Count;
            var rack = racks[rackIndex];
            var brokersInRack = brokersByRack[rack];

            // Find a broker in this rack that hasn't been used yet
            int? selectedBroker = null;
            var brokerOffset = partition % brokersInRack.Count;

            for (int j = 0; j < brokersInRack.Count; j++)
            {
                var candidateIndex = (brokerOffset + j) % brokersInRack.Count;
                var candidate = brokersInRack[candidateIndex];

                if (!usedBrokers.Contains(candidate))
                {
                    selectedBroker = candidate;
                    break;
                }
            }

            // If all brokers in this rack are used, try other racks
            if (selectedBroker == null)
            {
                foreach (var otherRack in racks)
                {
                    if (otherRack == rack) continue;

                    foreach (var candidate in brokersByRack[otherRack])
                    {
                        if (!usedBrokers.Contains(candidate))
                        {
                            selectedBroker = candidate;
                            break;
                        }
                    }
                    if (selectedBroker != null) break;
                }
            }

            // If still no broker found, we've exhausted all options
            if (selectedBroker == null)
                break;

            replicas.Add(selectedBroker.Value);
            usedBrokers.Add(selectedBroker.Value);
        }

        return replicas;
    }

    /// <summary>
    /// Simple round-robin replica assignment (fallback when rack info unavailable).
    /// </summary>
    public static List<int> AssignReplicasRoundRobin(List<int> brokerIds, int partition, int count)
    {
        var replicas = new List<int>();
        var startIndex = partition % brokerIds.Count;

        for (int i = 0; i < count; i++)
        {
            var idx = (startIndex + i) % brokerIds.Count;
            replicas.Add(brokerIds[idx]);
        }

        return replicas;
    }
}
