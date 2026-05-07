using Kuestenlogik.Surgewave.Clustering.Cluster;

namespace Kuestenlogik.Surgewave.Clustering.Reassignment;

/// <summary>
/// Generates reassignment plans for various scenarios:
/// balancing partitions across brokers, decommissioning a broker,
/// and changing replication factors.
/// </summary>
public sealed class ReassignmentPlanner
{
    /// <summary>
    /// Generate a plan to balance partitions evenly across the given brokers.
    /// Uses round-robin assignment to distribute partitions.
    /// </summary>
    public OnlineReassignmentPlan GenerateBalancePlan(
        IReadOnlyList<TopicPartitionInfo> currentAssignments,
        IReadOnlyList<int> brokerIds)
    {
        ArgumentNullException.ThrowIfNull(currentAssignments);
        ArgumentNullException.ThrowIfNull(brokerIds);

        var plan = new OnlineReassignmentPlan
        {
            Description = $"Balance partitions across {brokerIds.Count} brokers"
        };

        if (brokerIds.Count == 0)
            return plan;

        var sortedBrokers = brokerIds.OrderBy(id => id).ToList();

        // Group by topic to maintain per-topic round-robin
        var byTopic = currentAssignments
            .GroupBy(a => a.Topic)
            .OrderBy(g => g.Key);

        foreach (var topicGroup in byTopic)
        {
            var partitions = topicGroup.OrderBy(p => p.Partition).ToList();

            foreach (var partInfo in partitions)
            {
                var replicationFactor = partInfo.Replicas.Count;
                var newReplicas = new List<int>();

                // Round-robin starting from partition offset
                var startIndex = partInfo.Partition % sortedBrokers.Count;
                for (int i = 0; i < Math.Min(replicationFactor, sortedBrokers.Count); i++)
                {
                    var idx = (startIndex + i) % sortedBrokers.Count;
                    newReplicas.Add(sortedBrokers[idx]);
                }

                // Only include if the assignment actually changes
                if (!partInfo.Replicas.SequenceEqual(newReplicas))
                {
                    plan.Assignments.Add(new OnlinePartitionReassignment
                    {
                        Topic = partInfo.Topic,
                        Partition = partInfo.Partition,
                        CurrentReplicas = partInfo.Replicas.ToList(),
                        TargetReplicas = newReplicas,
                        TotalBytes = partInfo.SizeBytes
                    });
                }
            }
        }

        return plan;
    }

    /// <summary>
    /// Generate a plan to move all partitions OFF a broker (for decommissioning).
    /// Redistributes partitions to the remaining brokers using round-robin.
    /// </summary>
    public OnlineReassignmentPlan GenerateDecommissionPlan(
        int brokerIdToRemove,
        IReadOnlyList<TopicPartitionInfo> currentAssignments,
        IReadOnlyList<int> remainingBrokerIds)
    {
        ArgumentNullException.ThrowIfNull(currentAssignments);
        ArgumentNullException.ThrowIfNull(remainingBrokerIds);

        var plan = new OnlineReassignmentPlan
        {
            Description = $"Decommission broker {brokerIdToRemove}"
        };

        if (remainingBrokerIds.Count == 0)
            return plan;

        var sortedRemaining = remainingBrokerIds
            .Where(id => id != brokerIdToRemove)
            .OrderBy(id => id)
            .ToList();

        if (sortedRemaining.Count == 0)
            return plan;

        // Only process partitions that have the broker being removed
        var affectedPartitions = currentAssignments
            .Where(a => a.Replicas.Contains(brokerIdToRemove))
            .OrderBy(a => a.Topic)
            .ThenBy(a => a.Partition)
            .ToList();

        int assignmentIndex = 0;

        foreach (var partInfo in affectedPartitions)
        {
            var newReplicas = new List<int>();

            foreach (var replica in partInfo.Replicas)
            {
                if (replica == brokerIdToRemove)
                {
                    // Replace with a remaining broker (round-robin among candidates not already assigned)
                    int replacement = -1;
                    for (int attempt = 0; attempt < sortedRemaining.Count; attempt++)
                    {
                        var candidate = sortedRemaining[(assignmentIndex + attempt) % sortedRemaining.Count];
                        if (!newReplicas.Contains(candidate) &&
                            !partInfo.Replicas.Where(r => r != brokerIdToRemove).Contains(candidate) ||
                            !newReplicas.Contains(candidate))
                        {
                            // Find a candidate not already in newReplicas
                            if (!newReplicas.Contains(candidate))
                            {
                                replacement = candidate;
                                break;
                            }
                        }
                    }

                    if (replacement >= 0)
                    {
                        newReplicas.Add(replacement);
                    }
                    assignmentIndex++;
                }
                else
                {
                    newReplicas.Add(replica);
                }
            }

            plan.Assignments.Add(new OnlinePartitionReassignment
            {
                Topic = partInfo.Topic,
                Partition = partInfo.Partition,
                CurrentReplicas = partInfo.Replicas.ToList(),
                TargetReplicas = newReplicas,
                TotalBytes = partInfo.SizeBytes
            });
        }

        return plan;
    }

    /// <summary>
    /// Generate a plan to increase the replication factor for a topic
    /// by adding replicas on additional brokers.
    /// </summary>
    public OnlineReassignmentPlan GenerateReplicationPlan(
        string topic,
        int targetReplicationFactor,
        IReadOnlyList<TopicPartitionInfo> currentAssignments,
        IReadOnlyList<int> brokerIds)
    {
        ArgumentNullException.ThrowIfNull(topic);
        ArgumentNullException.ThrowIfNull(currentAssignments);
        ArgumentNullException.ThrowIfNull(brokerIds);

        var plan = new OnlineReassignmentPlan
        {
            Description = $"Change replication factor for '{topic}' to {targetReplicationFactor}"
        };

        var sortedBrokers = brokerIds.OrderBy(id => id).ToList();
        var topicPartitions = currentAssignments
            .Where(a => a.Topic == topic)
            .OrderBy(a => a.Partition)
            .ToList();

        if (sortedBrokers.Count == 0 || topicPartitions.Count == 0)
            return plan;

        // Clamp replication factor to available brokers
        var effectiveRf = Math.Min(targetReplicationFactor, sortedBrokers.Count);

        foreach (var partInfo in topicPartitions)
        {
            var currentRf = partInfo.Replicas.Count;

            if (currentRf == effectiveRf)
                continue; // No change needed

            var newReplicas = new List<int>();

            if (effectiveRf > currentRf)
            {
                // Increasing: keep existing replicas, add new ones
                newReplicas.AddRange(partInfo.Replicas);
                var candidateBrokers = sortedBrokers
                    .Where(b => !partInfo.Replicas.Contains(b))
                    .ToList();

                int needed = effectiveRf - currentRf;
                var startIdx = partInfo.Partition % Math.Max(1, candidateBrokers.Count);

                for (int i = 0; i < Math.Min(needed, candidateBrokers.Count); i++)
                {
                    var idx = (startIdx + i) % candidateBrokers.Count;
                    newReplicas.Add(candidateBrokers[idx]);
                }
            }
            else
            {
                // Decreasing: keep first N replicas
                newReplicas.AddRange(partInfo.Replicas.Take(effectiveRf));
            }

            plan.Assignments.Add(new OnlinePartitionReassignment
            {
                Topic = partInfo.Topic,
                Partition = partInfo.Partition,
                CurrentReplicas = partInfo.Replicas.ToList(),
                TargetReplicas = newReplicas,
                TotalBytes = partInfo.SizeBytes
            });
        }

        return plan;
    }

    /// <summary>
    /// Validate a proposed plan against the set of available broker IDs.
    /// Checks for unknown broker IDs, duplicate assignments, and empty replica lists.
    /// </summary>
    public ReassignmentValidation ValidatePlan(
        OnlineReassignmentPlan plan,
        IReadOnlyList<int> availableBrokerIds)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(availableBrokerIds);

        var errors = new List<string>();
        var warnings = new List<string>();
        var brokerSet = new HashSet<int>(availableBrokerIds);
        var seenPartitions = new HashSet<string>();

        if (plan.Assignments.Count == 0)
        {
            warnings.Add("Plan contains no assignments.");
        }

        foreach (var assignment in plan.Assignments)
        {
            var key = $"{assignment.Topic}-{assignment.Partition}";

            // Duplicate partition check
            if (!seenPartitions.Add(key))
            {
                errors.Add($"Duplicate assignment for partition {key}.");
            }

            // Empty target replicas
            if (assignment.TargetReplicas.Count == 0)
            {
                errors.Add($"Partition {key} has an empty target replica list.");
                continue;
            }

            // Unknown broker IDs
            foreach (var brokerId in assignment.TargetReplicas)
            {
                if (!brokerSet.Contains(brokerId))
                {
                    errors.Add($"Partition {key} targets unknown broker {brokerId}.");
                }
            }

            // Duplicate brokers in target
            var targetSet = new HashSet<int>();
            foreach (var brokerId in assignment.TargetReplicas)
            {
                if (!targetSet.Add(brokerId))
                {
                    errors.Add($"Partition {key} has duplicate broker {brokerId} in target replicas.");
                }
            }

            // Same assignment warning
            if (assignment.CurrentReplicas.SequenceEqual(assignment.TargetReplicas))
            {
                warnings.Add($"Partition {key} target is identical to current assignment (no-op).");
            }
        }

        return new ReassignmentValidation(
            IsValid: errors.Count == 0,
            Errors: errors,
            Warnings: warnings);
    }
}
