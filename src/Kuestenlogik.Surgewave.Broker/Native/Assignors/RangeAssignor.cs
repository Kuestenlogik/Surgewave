namespace Kuestenlogik.Surgewave.Broker.Native.Assignors;

/// <summary>
/// Range partition assignor - assigns partitions in ranges to each member.
/// Partitions are sorted and divided into contiguous ranges.
/// </summary>
public sealed class RangeAssignor : IPartitionAssignor
{
    public string Name => "range";

    public Dictionary<string, List<AssignedPartition>> Assign(
        List<string> topics,
        Dictionary<string, int> topicPartitionCounts,
        List<MemberSubscription> members)
    {
        var assignment = new Dictionary<string, List<AssignedPartition>>();

        // Initialize empty assignments for all members
        foreach (var member in members)
        {
            assignment[member.MemberId] = new List<AssignedPartition>();
        }

        if (members.Count == 0)
            return assignment;

        // Process each topic separately
        foreach (var topic in topics)
        {
            if (!topicPartitionCounts.TryGetValue(topic, out var partitionCount))
                continue;

            // Get members subscribed to this topic
            var subscribedMembers = members
                .Where(m => m.Topics.Contains(topic))
                .OrderBy(m => m.MemberId)
                .ToList();

            if (subscribedMembers.Count == 0)
                continue;

            // Calculate range assignment
            var partitionsPerMember = partitionCount / subscribedMembers.Count;
            var remainder = partitionCount % subscribedMembers.Count;

            var partitionIndex = 0;
            for (int i = 0; i < subscribedMembers.Count; i++)
            {
                var member = subscribedMembers[i];
                // Members at the beginning get an extra partition if there's a remainder
                var assignmentSize = partitionsPerMember + (i < remainder ? 1 : 0);

                for (int j = 0; j < assignmentSize; j++)
                {
                    assignment[member.MemberId].Add(new AssignedPartition(topic, partitionIndex++));
                }
            }
        }

        return assignment;
    }
}
