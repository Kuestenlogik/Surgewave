namespace Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;

/// <summary>
/// Implements the KIP-848 "revoke before assign" reconciliation step. When the server
/// recomputes a group's target assignment the broker cannot hand a partition to its new
/// owner until the previous owner has acknowledged that it has stopped consuming. This
/// reconciler computes the subset of a member's target that is safe to advertise on the
/// next heartbeat — partitions still owned by another member are withheld until that
/// member's next heartbeat carries an updated <c>OwnedTopicPartitions</c> list without
/// them.
/// </summary>
internal static class ConsumerGroupV2Reconciler
{
    /// <summary>
    /// Returns the assignment that should be communicated to <paramref name="member"/> on
    /// the next heartbeat. Partitions that appear in another member's
    /// <c>OwnedTopicPartitions</c> are filtered out so the new owner waits for the
    /// previous owner to revoke before it picks them up.
    /// </summary>
    /// <returns>A reduced copy of <c>member.TargetAssignment</c> safe to consume now.</returns>
    public static List<TopicPartitionAssignment> ComputeSafeAssignment(
        ConsumerGroupV2State group,
        ConsumerGroupV2Member member)
    {
        if (member.TargetAssignment.Count == 0)
        {
            return [];
        }

        var ownedByOthers = CollectPartitionsOwnedByOtherMembers(group, member.MemberId);

        var safe = new List<TopicPartitionAssignment>(member.TargetAssignment.Count);
        foreach (var topicTarget in member.TargetAssignment)
        {
            List<int>? safePartitions = null;
            foreach (var partition in topicTarget.Partitions)
            {
                if (ownedByOthers.Contains((topicTarget.TopicId, partition)))
                {
                    continue;
                }

                safePartitions ??= [];
                safePartitions.Add(partition);
            }

            if (safePartitions != null)
            {
                safe.Add(new TopicPartitionAssignment
                {
                    TopicId = topicTarget.TopicId,
                    Partitions = safePartitions,
                });
            }
        }

        return safe;
    }

    /// <summary>
    /// True when every member of the group has converged to its target assignment —
    /// i.e. the group is in the Stable state.
    /// </summary>
    public static bool IsStable(ConsumerGroupV2State group)
    {
        foreach (var member in group.Members.Values)
        {
            if (!HasReachedTarget(member))
            {
                return false;
            }
        }
        return true;
    }

    private static bool HasReachedTarget(ConsumerGroupV2Member member)
    {
        if (member.Assignment.Count != member.TargetAssignment.Count)
        {
            return false;
        }

        foreach (var target in member.TargetAssignment)
        {
            var actual = FindByTopicId(member.Assignment, target.TopicId);
            if (actual == null || !PartitionsEqual(actual.Partitions, target.Partitions))
            {
                return false;
            }
        }
        return true;
    }

    private static HashSet<(Guid TopicId, int Partition)> CollectPartitionsOwnedByOtherMembers(
        ConsumerGroupV2State group,
        string excludeMemberId)
    {
        var owned = new HashSet<(Guid, int)>();
        foreach (var other in group.Members.Values)
        {
            if (other.MemberId == excludeMemberId)
            {
                continue;
            }

            foreach (var topic in other.OwnedTopicPartitions)
            {
                foreach (var partition in topic.Partitions)
                {
                    owned.Add((topic.TopicId, partition));
                }
            }
        }
        return owned;
    }

    private static TopicPartitionAssignment? FindByTopicId(
        List<TopicPartitionAssignment> assignments,
        Guid topicId)
    {
        foreach (var a in assignments)
        {
            if (a.TopicId == topicId) return a;
        }
        return null;
    }

    private static bool PartitionsEqual(List<int> left, List<int> right)
    {
        if (left.Count != right.Count) return false;
        for (int i = 0; i < left.Count; i++)
        {
            if (left[i] != right[i]) return false;
        }
        return true;
    }
}
