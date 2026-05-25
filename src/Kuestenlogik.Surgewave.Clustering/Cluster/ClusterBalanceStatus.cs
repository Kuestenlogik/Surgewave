namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Status of cluster balance across brokers.
/// </summary>
public sealed class ClusterBalanceStatus
{
    /// <summary>
    /// Overall balance state.
    /// </summary>
    public BalanceState State { get; init; }

    /// <summary>
    /// Number of active brokers in the cluster.
    /// </summary>
    public int BrokerCount { get; init; }

    /// <summary>
    /// Total number of partitions across all topics.
    /// </summary>
    public int TotalPartitions { get; init; }

    /// <summary>
    /// Total number of replicas across all partitions.
    /// </summary>
    public int TotalReplicas { get; init; }

    /// <summary>
    /// Leader distribution per broker.
    /// </summary>
    public List<BrokerLeaderCount> LeaderDistribution { get; init; } = [];

    /// <summary>
    /// Replica distribution per broker.
    /// </summary>
    public List<BrokerReplicaCount> ReplicaDistribution { get; init; } = [];

    /// <summary>
    /// Leader imbalance ratio (0 = perfectly balanced, 1 = completely imbalanced).
    /// </summary>
    public double LeaderImbalanceRatio { get; init; }

    /// <summary>
    /// Replica imbalance ratio (0 = perfectly balanced, 1 = completely imbalanced).
    /// </summary>
    public double ReplicaImbalanceRatio { get; init; }

    /// <summary>
    /// Number of partitions not on their preferred leader.
    /// </summary>
    public int PartitionsNotOnPreferredLeader { get; init; }

    /// <summary>
    /// Number of under-replicated partitions.
    /// </summary>
    public int UnderReplicatedPartitions { get; init; }
}

/// <summary>
/// Balance state enumeration.
/// </summary>
public enum BalanceState
{
    /// <summary>
    /// Cluster is balanced within threshold.
    /// </summary>
    Balanced,

    /// <summary>
    /// Cluster has minor imbalance.
    /// </summary>
    MinorImbalance,

    /// <summary>
    /// Cluster has significant imbalance requiring attention.
    /// </summary>
    Imbalanced,

    /// <summary>
    /// Cluster has critical imbalance.
    /// </summary>
    Critical
}

/// <summary>
/// Leader count for a broker.
/// </summary>
public sealed class BrokerLeaderCount
{
    /// <summary>
    /// Broker ID.
    /// </summary>
    public int BrokerId { get; init; }

    /// <summary>
    /// Number of partitions this broker is leading.
    /// </summary>
    public int LeaderCount { get; init; }

    /// <summary>
    /// Expected leader count for perfect balance.
    /// </summary>
    public double ExpectedCount { get; init; }

    /// <summary>
    /// Deviation from expected (positive = overloaded, negative = underloaded).
    /// </summary>
    public int Deviation => LeaderCount - (int)Math.Round(ExpectedCount);
}

/// <summary>
/// Replica count for a broker.
/// </summary>
public sealed class BrokerReplicaCount
{
    /// <summary>
    /// Broker ID.
    /// </summary>
    public int BrokerId { get; init; }

    /// <summary>
    /// Number of replicas on this broker.
    /// </summary>
    public int ReplicaCount { get; init; }

    /// <summary>
    /// Expected replica count for perfect balance.
    /// </summary>
    public double ExpectedCount { get; init; }

    /// <summary>
    /// Deviation from expected (positive = overloaded, negative = underloaded).
    /// </summary>
    public int Deviation => ReplicaCount - (int)Math.Round(ExpectedCount);
}

/// <summary>
/// Balance plan generated for rebalancing.
/// </summary>
public sealed class ClusterBalancePlan
{
    /// <summary>
    /// Current balance status before rebalancing.
    /// </summary>
    public required ClusterBalanceStatus CurrentStatus { get; init; }

    /// <summary>
    /// Estimated status after rebalancing.
    /// </summary>
    public required ClusterBalanceStatus EstimatedStatusAfter { get; init; }

    /// <summary>
    /// Leader elections to perform (for preferred leader elections).
    /// </summary>
    public List<LeaderElectionAction> LeaderElections { get; init; } = [];

    /// <summary>
    /// Partition reassignments to perform.
    /// </summary>
    public ReassignmentPlan? ReassignmentPlan { get; init; }

    /// <summary>
    /// Summary of changes.
    /// </summary>
    public string Summary =>
        $"Leader elections: {LeaderElections.Count}, " +
        $"Partition moves: {ReassignmentPlan?.Partitions.Count ?? 0}";
}

/// <summary>
/// Action to elect a new leader for a partition.
/// </summary>
public sealed class LeaderElectionAction
{
    /// <summary>
    /// Topic name.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Partition number.
    /// </summary>
    public int Partition { get; init; }

    /// <summary>
    /// Current leader broker ID.
    /// </summary>
    public int CurrentLeader { get; init; }

    /// <summary>
    /// New leader broker ID (preferred leader).
    /// </summary>
    public int NewLeader { get; init; }
}
