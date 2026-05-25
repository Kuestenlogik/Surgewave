namespace Kuestenlogik.Surgewave.Broker.Native.Assignors;

/// <summary>
/// Round-robin partition assignor - assigns partitions in a round-robin fashion.
/// More even distribution when members subscribe to different topics.
/// </summary>
public sealed class RoundRobinAssignor : IPartitionAssignor
{
    public string Name => "roundrobin";

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

        // Collect all topic-partitions
        var allPartitions = new List<AssignedPartition>();
        foreach (var topic in topics.OrderBy(t => t))
        {
            if (!topicPartitionCounts.TryGetValue(topic, out var partitionCount))
                continue;

            for (int p = 0; p < partitionCount; p++)
            {
                allPartitions.Add(new AssignedPartition(topic, p));
            }
        }

        // Sort members for consistent ordering
        var sortedMembers = members.OrderBy(m => m.MemberId).ToList();

        // Round-robin assignment
        var memberIndex = 0;
        foreach (var partition in allPartitions)
        {
            // Find next member subscribed to this topic
            var startIndex = memberIndex;
            do
            {
                var member = sortedMembers[memberIndex];
                if (member.Topics.Contains(partition.Topic))
                {
                    assignment[member.MemberId].Add(partition);
                    memberIndex = (memberIndex + 1) % sortedMembers.Count;
                    break;
                }
                memberIndex = (memberIndex + 1) % sortedMembers.Count;
            } while (memberIndex != startIndex);
        }

        return assignment;
    }
}
