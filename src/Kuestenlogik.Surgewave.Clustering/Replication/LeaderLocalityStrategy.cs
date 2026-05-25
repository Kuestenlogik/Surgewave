using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Strategies for leader election based on locality.
/// </summary>
public enum LeaderElectionMode
{
    /// <summary>
    /// Prefer the first replica in the assignment list (Kafka default).
    /// </summary>
    PreferredReplica,

    /// <summary>
    /// Prefer a leader in the same rack as the majority of consumers.
    /// </summary>
    RackLocal,

    /// <summary>
    /// Prefer the leader with lowest latency to consumers (requires latency metrics).
    /// </summary>
    LatencyOptimized
}

/// <summary>
/// Determines optimal leader placement based on consumer locality and broker characteristics.
/// </summary>
public sealed partial class LeaderLocalityStrategy
{
    private readonly ClusterState _clusterState;
    private readonly ConsumerRackTracker _rackTracker;
    private readonly FailureDomainHierarchy _domainHierarchy;
    private readonly ILogger<LeaderLocalityStrategy> _logger;
    private readonly LeaderLocalityOptions _options;

    public LeaderLocalityStrategy(
        ClusterState clusterState,
        ConsumerRackTracker rackTracker,
        FailureDomainHierarchy domainHierarchy,
        ILogger<LeaderLocalityStrategy> logger,
        LeaderLocalityOptions? options = null)
    {
        _clusterState = clusterState;
        _rackTracker = rackTracker;
        _domainHierarchy = domainHierarchy;
        _logger = logger;
        _options = options ?? new LeaderLocalityOptions();
    }

    /// <summary>
    /// Selects the optimal leader from the ISR for a partition.
    /// </summary>
    /// <param name="partition">The partition needing a leader.</param>
    /// <param name="state">The current partition state.</param>
    /// <returns>The broker ID to elect as leader, or -1 if none suitable.</returns>
    public int SelectLeader(TopicPartition partition, PartitionState state)
    {
        if (state.Isr.Count == 0)
        {
            LogNoIsrReplicas(partition);
            return -1;
        }

        return _options.Mode switch
        {
            LeaderElectionMode.PreferredReplica => SelectPreferredReplica(partition, state),
            LeaderElectionMode.RackLocal => SelectRackLocal(partition, state),
            LeaderElectionMode.LatencyOptimized => SelectLatencyOptimized(partition, state),
            _ => SelectPreferredReplica(partition, state)
        };
    }

    /// <summary>
    /// Standard Kafka behavior: prefer the first replica in the assignment.
    /// </summary>
    private int SelectPreferredReplica(TopicPartition partition, PartitionState state)
    {
        // Find the preferred (first) replica that's in ISR
        foreach (var replica in state.Replicas)
        {
            if (state.Isr.Contains(replica))
            {
                LogSelectedPreferred(partition, replica);
                return replica;
            }
        }

        // Fall back to first ISR member
        var leader = state.Isr[0];
        LogFallbackToFirstIsr(partition, leader);
        return leader;
    }

    /// <summary>
    /// Prefer a leader in the same rack as the majority of consumers.
    /// </summary>
    private int SelectRackLocal(TopicPartition partition, PartitionState state)
    {
        var dominantRack = _rackTracker.GetDominantRack(partition);

        if (string.IsNullOrEmpty(dominantRack))
        {
            LogNoDominantRack(partition);
            return SelectPreferredReplica(partition, state);
        }

        // Find an ISR replica in the dominant rack
        foreach (var replica in state.Isr)
        {
            var broker = _clusterState.GetBroker(replica);
            if (broker?.Rack == dominantRack)
            {
                LogSelectedRackLocal(partition, replica, dominantRack);
                return replica;
            }

            // Check hierarchical rack (e.g., "us-east/dc1/zone-a/rack-1" contains "rack-1")
            if (broker?.Rack != null && IsRackMatch(broker.Rack, dominantRack))
            {
                LogSelectedRackLocal(partition, replica, dominantRack);
                return replica;
            }
        }

        // No ISR replica in dominant rack, fall back to preferred
        LogNoIsrInDominantRack(partition, dominantRack);
        return SelectPreferredReplica(partition, state);
    }

    /// <summary>
    /// Prefer the leader with lowest latency (based on domain proximity).
    /// </summary>
    private int SelectLatencyOptimized(TopicPartition partition, PartitionState state)
    {
        var rackCounts = _rackTracker.GetRackConsumerCounts(partition);

        if (rackCounts.Count == 0)
        {
            return SelectPreferredReplica(partition, state);
        }

        // Score each ISR replica based on proximity to consumers
        var candidates = new List<(int BrokerId, int Score)>();

        foreach (var replica in state.Isr)
        {
            var broker = _clusterState.GetBroker(replica);
            if (broker == null) continue;

            var score = CalculateProximityScore(broker, rackCounts);
            candidates.Add((replica, score));
        }

        if (candidates.Count == 0)
        {
            return SelectPreferredReplica(partition, state);
        }

        // Select highest scoring replica
        var best = candidates.OrderByDescending(c => c.Score).First();
        LogSelectedLatencyOptimized(partition, best.BrokerId, best.Score);
        return best.BrokerId;
    }

    /// <summary>
    /// Calculates a proximity score for a broker based on consumer distribution.
    /// Higher score = more consumers are close to this broker.
    /// </summary>
    private int CalculateProximityScore(BrokerNode broker, Dictionary<string, int> rackCounts)
    {
        if (string.IsNullOrEmpty(broker.Rack))
            return 0;

        var score = 0;
        foreach (var (rack, count) in rackCounts)
        {
            // Same rack = full score
            if (broker.Rack == rack || IsRackMatch(broker.Rack, rack))
            {
                score += count * 100;
                continue;
            }

            // Check domain hierarchy for partial matches
            var brokerDomain = _domainHierarchy.GetBrokerRack(broker.BrokerId);
            if (brokerDomain == null) continue;

            // Same zone = partial score
            var zoneDomain = brokerDomain.Parent;
            if (zoneDomain != null && rack.StartsWith(zoneDomain.Path, StringComparison.Ordinal))
            {
                score += count * 50;
                continue;
            }

            // Same datacenter = small score
            var dcDomain = zoneDomain?.Parent;
            if (dcDomain != null && rack.StartsWith(dcDomain.Path, StringComparison.Ordinal))
            {
                score += count * 10;
            }
        }

        return score;
    }

    private static bool IsRackMatch(string brokerRack, string targetRack)
    {
        // Handle hierarchical rack format: "region/dc/zone/rack"
        // Check if the broker's rack path ends with or equals the target
        return brokerRack.EndsWith($"/{targetRack}", StringComparison.Ordinal) ||
               brokerRack == targetRack;
    }

    /// <summary>
    /// Evaluates if a leader change would improve locality.
    /// </summary>
    /// <param name="partition">The partition to evaluate.</param>
    /// <param name="currentLeader">The current leader broker ID.</param>
    /// <param name="state">The partition state.</param>
    /// <returns>True if rebalance is recommended.</returns>
    public bool ShouldRebalance(TopicPartition partition, int currentLeader, PartitionState state)
    {
        if (!_options.AutoRebalance)
            return false;

        var optimalLeader = SelectLeader(partition, state);
        if (optimalLeader == currentLeader || optimalLeader < 0)
            return false;

        // Only rebalance if there's significant benefit
        if (_options.Mode == LeaderElectionMode.RackLocal)
        {
            var dominantRack = _rackTracker.GetDominantRack(partition);
            var currentBroker = _clusterState.GetBroker(currentLeader);
            var optimalBroker = _clusterState.GetBroker(optimalLeader);

            // Current leader is already in dominant rack
            if (currentBroker != null && IsRackMatch(currentBroker.Rack ?? "", dominantRack ?? ""))
                return false;

            // Optimal leader is in dominant rack
            if (optimalBroker != null && IsRackMatch(optimalBroker.Rack ?? "", dominantRack ?? ""))
            {
                LogRebalanceRecommended(partition, currentLeader, optimalLeader, dominantRack ?? "");
                return true;
            }
        }

        return false;
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "No ISR replicas for partition {Partition}")]
    private partial void LogNoIsrReplicas(TopicPartition partition);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Selected preferred replica {BrokerId} for partition {Partition}")]
    private partial void LogSelectedPreferred(TopicPartition partition, int brokerId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Fallback to first ISR member {BrokerId} for partition {Partition}")]
    private partial void LogFallbackToFirstIsr(TopicPartition partition, int brokerId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "No dominant rack for partition {Partition}, using preferred replica")]
    private partial void LogNoDominantRack(TopicPartition partition);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Selected rack-local leader {BrokerId} in rack {Rack} for partition {Partition}")]
    private partial void LogSelectedRackLocal(TopicPartition partition, int brokerId, string rack);

    [LoggerMessage(Level = LogLevel.Debug, Message = "No ISR replica in dominant rack {Rack} for partition {Partition}")]
    private partial void LogNoIsrInDominantRack(TopicPartition partition, string rack);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Selected latency-optimized leader {BrokerId} with score {Score} for partition {Partition}")]
    private partial void LogSelectedLatencyOptimized(TopicPartition partition, int brokerId, int score);

    [LoggerMessage(Level = LogLevel.Information, Message = "Rebalance recommended for partition {Partition}: {CurrentLeader} -> {OptimalLeader} (dominant rack: {Rack})")]
    private partial void LogRebalanceRecommended(TopicPartition partition, int currentLeader, int optimalLeader, string rack);
}

/// <summary>
/// Configuration options for leader locality strategy.
/// </summary>
public sealed class LeaderLocalityOptions
{
    /// <summary>
    /// The leader election mode to use.
    /// Default: PreferredReplica
    /// </summary>
    public LeaderElectionMode Mode { get; init; } = LeaderElectionMode.PreferredReplica;

    /// <summary>
    /// Whether to automatically recommend rebalancing for better locality.
    /// Default: false
    /// </summary>
    public bool AutoRebalance { get; init; } = false;

    /// <summary>
    /// Minimum consumer count difference to trigger rebalance recommendation.
    /// Default: 2
    /// </summary>
    public int MinConsumerDifferenceForRebalance { get; init; } = 2;
}
