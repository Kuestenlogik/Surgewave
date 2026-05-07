using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Manages cluster balancing - leader distribution and replica placement.
/// </summary>
public sealed partial class ClusterBalancer
{
    private readonly ILogger<ClusterBalancer> _logger;
    private readonly ClusterState _clusterState;
    private readonly ClusteringConfig _config;
    private readonly HeartbeatManager? _heartbeatManager;

    public ClusterBalancer(
        ILogger<ClusterBalancer> logger,
        ClusterState clusterState,
        ClusteringConfig config,
        HeartbeatManager? heartbeatManager = null)
    {
        _logger = logger;
        _clusterState = clusterState;
        _config = config;
        _heartbeatManager = heartbeatManager;
    }

    /// <summary>
    /// Calculate the current cluster balance status.
    /// </summary>
    public ClusterBalanceStatus GetBalanceStatus()
    {
        var aliveBrokers = GetAliveBrokers();
        var partitionStates = _clusterState.PartitionStates.ToList();

        if (aliveBrokers.Count == 0 || partitionStates.Count == 0)
        {
            return new ClusterBalanceStatus
            {
                State = BalanceState.Balanced,
                BrokerCount = aliveBrokers.Count,
                TotalPartitions = partitionStates.Count,
                TotalReplicas = 0,
                LeaderImbalanceRatio = 0,
                ReplicaImbalanceRatio = 0
            };
        }

        // Calculate leader distribution
        var leaderCounts = new Dictionary<int, int>();
        var replicaCounts = new Dictionary<int, int>();
        foreach (var brokerId in aliveBrokers)
        {
            leaderCounts[brokerId] = 0;
            replicaCounts[brokerId] = 0;
        }

        int notOnPreferredLeader = 0;
        int underReplicated = 0;
        int totalReplicas = 0;

        foreach (var (tp, partState) in partitionStates)
        {
            // Count leaders
            if (leaderCounts.TryGetValue(partState.LeaderBrokerId, out var currentLeaderCount))
            {
                leaderCounts[partState.LeaderBrokerId] = currentLeaderCount + 1;
            }

            // Check preferred leader
            if (partState.LeaderBrokerId != partState.PreferredLeader &&
                aliveBrokers.Contains(partState.PreferredLeader))
            {
                notOnPreferredLeader++;
            }

            // Check under-replication
            if (partState.Isr.Count < partState.Replicas.Count)
            {
                underReplicated++;
            }

            // Count replicas per broker
            foreach (var replica in partState.Replicas)
            {
                totalReplicas++;
                if (replicaCounts.TryGetValue(replica, out var currentReplicaCount))
                {
                    replicaCounts[replica] = currentReplicaCount + 1;
                }
            }
        }

        // Calculate expected counts
        double expectedLeaders = (double)partitionStates.Count / aliveBrokers.Count;
        double expectedReplicas = (double)totalReplicas / aliveBrokers.Count;

        // Build distribution lists
        var leaderDistribution = leaderCounts.Select(kv => new BrokerLeaderCount
        {
            BrokerId = kv.Key,
            LeaderCount = kv.Value,
            ExpectedCount = expectedLeaders
        }).OrderBy(b => b.BrokerId).ToList();

        var replicaDistribution = replicaCounts.Select(kv => new BrokerReplicaCount
        {
            BrokerId = kv.Key,
            ReplicaCount = kv.Value,
            ExpectedCount = expectedReplicas
        }).OrderBy(b => b.BrokerId).ToList();

        // Calculate imbalance ratios
        double leaderImbalance = CalculateImbalanceRatio(leaderCounts.Values, expectedLeaders);
        double replicaImbalance = CalculateImbalanceRatio(replicaCounts.Values, expectedReplicas);

        // Determine overall state
        var balanceState = DetermineBalanceState(leaderImbalance, replicaImbalance,
            notOnPreferredLeader, underReplicated, partitionStates.Count);

        return new ClusterBalanceStatus
        {
            State = balanceState,
            BrokerCount = aliveBrokers.Count,
            TotalPartitions = partitionStates.Count,
            TotalReplicas = totalReplicas,
            LeaderDistribution = leaderDistribution,
            ReplicaDistribution = replicaDistribution,
            LeaderImbalanceRatio = leaderImbalance,
            ReplicaImbalanceRatio = replicaImbalance,
            PartitionsNotOnPreferredLeader = notOnPreferredLeader,
            UnderReplicatedPartitions = underReplicated
        };
    }

    /// <summary>
    /// Generate a rebalance plan to improve cluster balance.
    /// </summary>
    public ClusterBalancePlan GenerateRebalancePlan()
    {
        var currentStatus = GetBalanceStatus();
        var aliveBrokers = GetAliveBrokers();
        var leaderElections = new List<LeaderElectionAction>();
        var reassignmentPlan = new ReassignmentPlan { Version = 1, Partitions = [] };

        // Step 1: Generate preferred leader elections
        foreach (var (tp, state) in _clusterState.PartitionStates)
        {
            if (state.LeaderBrokerId != state.PreferredLeader &&
                state.Isr.Contains(state.PreferredLeader) &&
                aliveBrokers.Contains(state.PreferredLeader))
            {
                leaderElections.Add(new LeaderElectionAction
                {
                    Topic = tp.Topic,
                    Partition = tp.Partition,
                    CurrentLeader = state.LeaderBrokerId,
                    NewLeader = state.PreferredLeader
                });
            }
        }

        // Step 2: Generate replica reassignments for heavily imbalanced brokers
        if (currentStatus.ReplicaImbalanceRatio > _config.RebalanceImbalanceThreshold)
        {
            GenerateReplicaRebalancePlan(aliveBrokers, reassignmentPlan);
        }

        // Estimate status after rebalancing
        var estimatedStatus = EstimateStatusAfterRebalance(currentStatus, leaderElections, reassignmentPlan);

        return new ClusterBalancePlan
        {
            CurrentStatus = currentStatus,
            EstimatedStatusAfter = estimatedStatus,
            LeaderElections = leaderElections,
            ReassignmentPlan = reassignmentPlan.Partitions.Count > 0 ? reassignmentPlan : null
        };
    }

    /// <summary>
    /// Check if rebalancing is needed based on configuration threshold.
    /// </summary>
    public bool IsRebalanceNeeded()
    {
        var status = GetBalanceStatus();
        return status.LeaderImbalanceRatio > _config.RebalanceImbalanceThreshold ||
               status.ReplicaImbalanceRatio > _config.RebalanceImbalanceThreshold ||
               status.PartitionsNotOnPreferredLeader > 0;
    }

    private List<int> GetAliveBrokers()
    {
        if (_heartbeatManager != null)
        {
            return _clusterState.Brokers
                .Select(b => b.Key)
                .Where(id => _heartbeatManager.IsBrokerAlive(id))
                .ToList();
        }

        // Without heartbeat manager, assume all brokers are alive
        return _clusterState.Brokers.Select(b => b.Key).ToList();
    }

    private static double CalculateImbalanceRatio(IEnumerable<int> counts, double expected)
    {
        if (expected == 0) return 0;

        var countList = counts.ToList();
        if (countList.Count == 0) return 0;

        // Standard deviation normalized by expected value
        double sumSquaredDiff = countList.Sum(c => Math.Pow(c - expected, 2));
        double stdDev = Math.Sqrt(sumSquaredDiff / countList.Count);

        // Normalize: 0 = perfect, approaching 1 = heavily imbalanced
        return Math.Min(1.0, stdDev / expected);
    }

    private BalanceState DetermineBalanceState(
        double leaderImbalance,
        double replicaImbalance,
        int notOnPreferred,
        int underReplicated,
        int totalPartitions)
    {
        // Critical if under-replicated
        if (underReplicated > 0)
            return BalanceState.Critical;

        double maxImbalance = Math.Max(leaderImbalance, replicaImbalance);
        double threshold = _config.RebalanceImbalanceThreshold;

        if (maxImbalance <= threshold * 0.5 && notOnPreferred == 0)
            return BalanceState.Balanced;

        if (maxImbalance <= threshold)
            return BalanceState.MinorImbalance;

        if (maxImbalance <= threshold * 2)
            return BalanceState.Imbalanced;

        return BalanceState.Critical;
    }

    private void GenerateReplicaRebalancePlan(List<int> brokers, ReassignmentPlan plan)
    {
        if (brokers.Count < 2) return;

        // Group partitions by topic
        var partitionsByTopic = _clusterState.PartitionStates
            .GroupBy(kv => kv.Key.Topic)
            .ToDictionary(g => g.Key, g => g.ToList());

        foreach (var (topic, partitions) in partitionsByTopic)
        {
            var replicationFactor = partitions.FirstOrDefault().Value?.Replicas.Count ?? 1;

            for (int i = 0; i < partitions.Count; i++)
            {
                var (tp, state) = partitions[i];
                var newReplicas = new List<int>();

                // Round-robin across brokers
                int startBroker = i % brokers.Count;
                for (int r = 0; r < Math.Min(replicationFactor, brokers.Count); r++)
                {
                    int brokerIndex = (startBroker + r) % brokers.Count;
                    newReplicas.Add(brokers[brokerIndex]);
                }

                // Only add if different from current
                if (!state.Replicas.SequenceEqual(newReplicas))
                {
                    plan.Partitions.Add(new PartitionReassignment
                    {
                        Topic = topic,
                        Partition = tp.Partition,
                        Replicas = newReplicas
                    });
                }
            }
        }

        LogRebalancePlanGenerated(plan.Partitions.Count);
    }

    private ClusterBalanceStatus EstimateStatusAfterRebalance(
        ClusterBalanceStatus current,
        List<LeaderElectionAction> elections,
        ReassignmentPlan reassignments)
    {
        // Clone leader distribution with elections applied
        var leaderCounts = current.LeaderDistribution
            .ToDictionary(d => d.BrokerId, d => d.LeaderCount);

        foreach (var election in elections)
        {
            if (leaderCounts.TryGetValue(election.CurrentLeader, out var currentCount))
                leaderCounts[election.CurrentLeader] = currentCount - 1;
            if (leaderCounts.TryGetValue(election.NewLeader, out var newCount))
                leaderCounts[election.NewLeader] = newCount + 1;
        }

        // Estimate imbalance after reassignments
        double expectedLeaders = current.BrokerCount > 0
            ? (double)current.TotalPartitions / current.BrokerCount
            : 0;

        double newLeaderImbalance = expectedLeaders > 0
            ? CalculateImbalanceRatio(leaderCounts.Values, expectedLeaders)
            : 0;

        // With round-robin reassignment, replica balance would be near-perfect
        double newReplicaImbalance = reassignments.Partitions.Count > 0
            ? Math.Min(0.05, current.ReplicaImbalanceRatio * 0.2)
            : current.ReplicaImbalanceRatio;

        return new ClusterBalanceStatus
        {
            State = DetermineBalanceState(newLeaderImbalance, newReplicaImbalance, 0, 0, current.TotalPartitions),
            BrokerCount = current.BrokerCount,
            TotalPartitions = current.TotalPartitions,
            TotalReplicas = current.TotalReplicas,
            LeaderDistribution = leaderCounts.Select(kv => new BrokerLeaderCount
            {
                BrokerId = kv.Key,
                LeaderCount = kv.Value,
                ExpectedCount = expectedLeaders
            }).OrderBy(b => b.BrokerId).ToList(),
            ReplicaDistribution = current.ReplicaDistribution,
            LeaderImbalanceRatio = newLeaderImbalance,
            ReplicaImbalanceRatio = newReplicaImbalance,
            PartitionsNotOnPreferredLeader = 0,
            UnderReplicatedPartitions = current.UnderReplicatedPartitions
        };
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Generated rebalance plan with {Count} partition moves")]
    private partial void LogRebalancePlanGenerated(int count);
}
