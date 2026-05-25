using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Assigns replicas using hierarchical failure domain awareness.
/// Supports multi-level placement constraints like spread_across:zone.
/// </summary>
public sealed partial class HierarchicalReplicaAssignment
{
    private readonly ClusterState _clusterState;
    private readonly FailureDomainHierarchy _domainHierarchy;
    private readonly FailureDomainValidator _domainValidator;
    private readonly ILogger<HierarchicalReplicaAssignment> _logger;
    private readonly HierarchicalReplicaAssignmentOptions _options;

    public HierarchicalReplicaAssignment(
        ClusterState clusterState,
        FailureDomainHierarchy domainHierarchy,
        FailureDomainValidator domainValidator,
        ILogger<HierarchicalReplicaAssignment> logger,
        HierarchicalReplicaAssignmentOptions? options = null)
    {
        _clusterState = clusterState;
        _domainHierarchy = domainHierarchy;
        _domainValidator = domainValidator;
        _logger = logger;
        _options = options ?? new HierarchicalReplicaAssignmentOptions();
    }

    /// <summary>
    /// Assigns replicas for a new partition, respecting failure domain constraints.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="partition">The partition number.</param>
    /// <param name="replicationFactor">The desired replication factor.</param>
    /// <returns>List of broker IDs for replicas, empty if assignment failed.</returns>
    public List<int> AssignReplicas(string topic, int partition, short replicationFactor)
    {
        var availableBrokers = _clusterState.Brokers.Values.ToList();

        if (availableBrokers.Count < replicationFactor)
        {
            LogInsufficientBrokers(topic, partition, replicationFactor, availableBrokers.Count);
            // Fall back to assigning what we can
            return availableBrokers.Select(b => b.BrokerId).Take(replicationFactor).ToList();
        }

        // Parse placement constraints
        var constraint = ParseConstraint(_options.PlacementConstraints);

        // Assign based on constraint type
        return constraint.Type switch
        {
            ConstraintType.SpreadAcross => AssignWithSpread(topic, partition, replicationFactor, availableBrokers, constraint.Level),
            ConstraintType.Prefer => AssignWithPreference(topic, partition, replicationFactor, availableBrokers, constraint.Value),
            ConstraintType.None => AssignRoundRobin(topic, partition, replicationFactor, availableBrokers),
            _ => AssignRoundRobin(topic, partition, replicationFactor, availableBrokers)
        };
    }

    /// <summary>
    /// Assigns replicas spread across different failure domains.
    /// </summary>
    private List<int> AssignWithSpread(
        string topic,
        int partition,
        short replicationFactor,
        List<BrokerNode> availableBrokers,
        FailureDomainLevel level)
    {
        var replicas = new List<int>();
        var usedDomains = new HashSet<string>();

        // Group brokers by domain
        var brokersByDomain = availableBrokers
            .GroupBy(b => GetDomainPath(b, level))
            .ToDictionary(g => g.Key, g => g.ToList());

        // Round-robin across domains
        var domainQueue = new Queue<string>(brokersByDomain.Keys.OrderBy(d => d));
        var brokerIndices = brokersByDomain.ToDictionary(kvp => kvp.Key, _ => 0);

        while (replicas.Count < replicationFactor && domainQueue.Count > 0)
        {
            var domain = domainQueue.Dequeue();
            var brokers = brokersByDomain[domain];
            var index = brokerIndices[domain];

            if (index < brokers.Count)
            {
                replicas.Add(brokers[index].BrokerId);
                brokerIndices[domain] = index + 1;

                // Re-queue domain if it has more brokers
                if (index + 1 < brokers.Count)
                {
                    domainQueue.Enqueue(domain);
                }
            }
        }

        // If we still need more replicas, add from any domain
        if (replicas.Count < replicationFactor)
        {
            var remaining = availableBrokers
                .Where(b => !replicas.Contains(b.BrokerId))
                .Select(b => b.BrokerId)
                .Take(replicationFactor - replicas.Count);
            replicas.AddRange(remaining);
        }

        // Validate assignment
        var violations = _domainValidator.ValidateAssignment(replicas, topic, partition);
        if (violations.Count > 0)
        {
            LogAssignmentViolations(topic, partition, violations.Count);
        }

        LogAssignedWithSpread(topic, partition, replicas.Count, level);
        return replicas;
    }

    /// <summary>
    /// Assigns replicas with preference for a specific region/datacenter.
    /// </summary>
    private List<int> AssignWithPreference(
        string topic,
        int partition,
        short replicationFactor,
        List<BrokerNode> availableBrokers,
        string? preferenceValue)
    {
        if (string.IsNullOrEmpty(preferenceValue))
        {
            return AssignRoundRobin(topic, partition, replicationFactor, availableBrokers);
        }

        // Parse preference like "region=us-east"
        var parts = preferenceValue.Split('=', 2);
        if (parts.Length != 2)
        {
            LogInvalidPreference(preferenceValue);
            return AssignRoundRobin(topic, partition, replicationFactor, availableBrokers);
        }

        var preferredValue = parts[1];

        // Separate preferred and other brokers
        var preferredBrokers = availableBrokers
            .Where(b => MatchesPreference(b, parts[0], preferredValue))
            .ToList();
        var otherBrokers = availableBrokers
            .Where(b => !preferredBrokers.Contains(b))
            .ToList();

        var replicas = new List<int>();

        // First, add from preferred brokers
        foreach (var broker in preferredBrokers.Take(replicationFactor))
        {
            replicas.Add(broker.BrokerId);
        }

        // Then fill from others if needed
        foreach (var broker in otherBrokers.Take(replicationFactor - replicas.Count))
        {
            replicas.Add(broker.BrokerId);
        }

        LogAssignedWithPreference(topic, partition, replicas.Count, preferenceValue);
        return replicas;
    }

    /// <summary>
    /// Standard round-robin assignment.
    /// </summary>
    private List<int> AssignRoundRobin(
        string topic,
        int partition,
        short replicationFactor,
        List<BrokerNode> availableBrokers)
    {
        // Simple round-robin based on partition number
        var sortedBrokers = availableBrokers.OrderBy(b => b.BrokerId).ToList();
        var startIndex = partition % sortedBrokers.Count;

        var replicas = new List<int>(replicationFactor);
        for (int i = 0; i < replicationFactor && i < sortedBrokers.Count; i++)
        {
            var index = (startIndex + i) % sortedBrokers.Count;
            replicas.Add(sortedBrokers[index].BrokerId);
        }

        return replicas;
    }

    /// <summary>
    /// Reassigns replicas for an existing partition to improve domain spread.
    /// </summary>
    public List<int>? RebalanceReplicas(TopicPartition partition, List<int> currentReplicas, short replicationFactor)
    {
        var violations = _domainValidator.ValidateAssignment(currentReplicas, partition.Topic, partition.Partition);
        if (violations.Count == 0)
        {
            return null; // No rebalance needed
        }

        // Try to find a better assignment
        var newReplicas = AssignReplicas(partition.Topic, partition.Partition, replicationFactor);

        // Prefer keeping the current leader if possible
        if (currentReplicas.Count > 0 && newReplicas.Contains(currentReplicas[0]))
        {
            // Move current leader to front
            newReplicas.Remove(currentReplicas[0]);
            newReplicas.Insert(0, currentReplicas[0]);
        }

        var newViolations = _domainValidator.ValidateAssignment(newReplicas, partition.Topic, partition.Partition);
        if (newViolations.Count >= violations.Count)
        {
            return null; // New assignment isn't better
        }

        LogRebalanceRecommended(partition, violations.Count, newViolations.Count);
        return newReplicas;
    }

    private string GetDomainPath(BrokerNode broker, FailureDomainLevel level)
    {
        if (string.IsNullOrEmpty(broker.Rack))
            return "default";

        var parts = broker.Rack.Split('/');
        var index = level switch
        {
            FailureDomainLevel.Region => 0,
            FailureDomainLevel.Datacenter => Math.Min(1, parts.Length - 1),
            FailureDomainLevel.Zone => Math.Min(2, parts.Length - 1),
            FailureDomainLevel.Rack => parts.Length - 1,
            _ => parts.Length - 1
        };

        if (index >= parts.Length)
            return broker.Rack;

        return string.Join("/", parts.Take(index + 1));
    }

    private static bool MatchesPreference(BrokerNode broker, string level, string value)
    {
        if (string.IsNullOrEmpty(broker.Rack))
            return false;

        var parts = broker.Rack.Split('/');
        var index = level.ToLowerInvariant() switch
        {
            "region" => 0,
            "datacenter" or "dc" => 1,
            "zone" or "az" => 2,
            "rack" => parts.Length - 1,
            _ => -1
        };

        if (index < 0 || index >= parts.Length)
            return false;

        return parts[index].Equals(value, StringComparison.OrdinalIgnoreCase);
    }

    private static PlacementConstraint ParseConstraint(string? constraint)
    {
        if (string.IsNullOrEmpty(constraint))
            return new PlacementConstraint(ConstraintType.None, FailureDomainLevel.Rack, null);

        var parts = constraint.Split(':', 2);
        if (parts.Length != 2)
            return new PlacementConstraint(ConstraintType.None, FailureDomainLevel.Rack, null);

        var type = parts[0].ToLowerInvariant() switch
        {
            "spread_across" => ConstraintType.SpreadAcross,
            "prefer" => ConstraintType.Prefer,
            _ => ConstraintType.None
        };

        if (type == ConstraintType.SpreadAcross)
        {
            var level = parts[1].ToLowerInvariant() switch
            {
                "region" => FailureDomainLevel.Region,
                "datacenter" or "dc" => FailureDomainLevel.Datacenter,
                "zone" or "az" => FailureDomainLevel.Zone,
                "rack" => FailureDomainLevel.Rack,
                _ => FailureDomainLevel.Rack
            };
            return new PlacementConstraint(type, level, null);
        }

        return new PlacementConstraint(type, FailureDomainLevel.Rack, parts[1]);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Insufficient brokers for {Topic}-{Partition}: need {ReplicationFactor}, have {AvailableCount}")]
    private partial void LogInsufficientBrokers(string topic, int partition, short replicationFactor, int availableCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Assigned {ReplicaCount} replicas for {Topic}-{Partition} with spread_across:{Level}")]
    private partial void LogAssignedWithSpread(string topic, int partition, int replicaCount, FailureDomainLevel level);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Assigned {ReplicaCount} replicas for {Topic}-{Partition} with prefer:{Preference}")]
    private partial void LogAssignedWithPreference(string topic, int partition, int replicaCount, string preference);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Assignment for {Topic}-{Partition} has {ViolationCount} constraint violations")]
    private partial void LogAssignmentViolations(string topic, int partition, int violationCount);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Invalid preference constraint: {Preference}")]
    private partial void LogInvalidPreference(string preference);

    [LoggerMessage(Level = LogLevel.Information, Message = "Rebalance recommended for {Partition}: {OldViolations} -> {NewViolations} violations")]
    private partial void LogRebalanceRecommended(TopicPartition partition, int oldViolations, int newViolations);
}

/// <summary>
/// Configuration options for hierarchical replica assignment.
/// </summary>
public sealed class HierarchicalReplicaAssignmentOptions
{
    /// <summary>
    /// Placement constraints for replica assignment.
    /// Format: "spread_across:zone" or "prefer:region=us-east"
    /// </summary>
    public string? PlacementConstraints { get; init; }

    /// <summary>
    /// Minimum number of distinct failure domains for replicas.
    /// Default: 0 (no minimum)
    /// </summary>
    public int MinDistinctDomains { get; init; } = 0;
}

internal enum ConstraintType
{
    None,
    SpreadAcross,
    Prefer
}

internal readonly record struct PlacementConstraint(
    ConstraintType Type,
    FailureDomainLevel Level,
    string? Value);
