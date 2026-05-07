using System.Buffers.Binary;
using System.Text;

namespace Kuestenlogik.Surgewave.Broker.Native.Assignors;

/// <summary>
/// Sticky partition assignor - tries to maintain existing assignments while rebalancing.
/// Minimizes partition movement during rebalances for better performance.
/// </summary>
public sealed class StickyAssignor : IPartitionAssignor
{
    public string Name => "sticky";

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

        // Parse previous assignments from member metadata (if available)
        var previousAssignments = ParsePreviousAssignments(members);

        // Collect all partitions
        var allPartitions = new List<AssignedPartition>();
        foreach (var topic in topics)
        {
            if (!topicPartitionCounts.TryGetValue(topic, out var partitionCount))
                continue;

            for (int p = 0; p < partitionCount; p++)
            {
                allPartitions.Add(new AssignedPartition(topic, p));
            }
        }

        var assignedPartitions = new HashSet<AssignedPartition>();
        var memberSubscriptions = members.ToDictionary(m => m.MemberId, m => m.Topics.ToHashSet());

        // First pass: Keep existing assignments if member is still subscribed
        foreach (var (memberId, previousPartitions) in previousAssignments)
        {
            if (!assignment.ContainsKey(memberId) || !memberSubscriptions.ContainsKey(memberId))
                continue;

            var subscriptions = memberSubscriptions[memberId];
            foreach (var partition in previousPartitions)
            {
                if (allPartitions.Contains(partition) &&
                    subscriptions.Contains(partition.Topic) &&
                    !assignedPartitions.Contains(partition))
                {
                    assignment[memberId].Add(partition);
                    assignedPartitions.Add(partition);
                }
            }
        }

        // Second pass: Distribute unassigned partitions evenly
        var unassignedPartitions = allPartitions.Where(p => !assignedPartitions.Contains(p)).ToList();
        var sortedMembers = members.OrderBy(m => m.MemberId).ToList();

        foreach (var partition in unassignedPartitions)
        {
            // Find member with least partitions that is subscribed to this topic
            var targetMember = sortedMembers
                .Where(m => m.Topics.Contains(partition.Topic))
                .OrderBy(m => assignment[m.MemberId].Count)
                .FirstOrDefault();

            if (targetMember != null)
            {
                assignment[targetMember.MemberId].Add(partition);
            }
        }

        // Sort partitions within each member for consistency
        foreach (var memberId in assignment.Keys.ToList())
        {
            assignment[memberId] = assignment[memberId]
                .OrderBy(p => p.Topic)
                .ThenBy(p => p.Partition)
                .ToList();
        }

        return assignment;
    }

    private static Dictionary<string, List<AssignedPartition>> ParsePreviousAssignments(List<MemberSubscription> members)
    {
        var result = new Dictionary<string, List<AssignedPartition>>();

        foreach (var member in members)
        {
            if (member.UserData.Length == 0)
            {
                result[member.MemberId] = new List<AssignedPartition>();
                continue;
            }

            try
            {
                var partitions = new List<AssignedPartition>();
                var span = member.UserData.AsSpan();
                var pos = 0;

                // Format: [topicCount(2)][topic(len+string)][partitionCount(4)][partition(4)...]...
                if (span.Length >= 2)
                {
                    var topicCount = BinaryPrimitives.ReadInt16BigEndian(span[pos..]);
                    pos += 2;

                    for (int t = 0; t < topicCount && pos < span.Length; t++)
                    {
                        var topicLen = BinaryPrimitives.ReadInt16BigEndian(span[pos..]);
                        pos += 2;
                        var topic = Encoding.UTF8.GetString(span.Slice(pos, topicLen));
                        pos += topicLen;

                        var partitionCount = BinaryPrimitives.ReadInt32BigEndian(span[pos..]);
                        pos += 4;

                        for (int p = 0; p < partitionCount && pos + 4 <= span.Length; p++)
                        {
                            var partition = BinaryPrimitives.ReadInt32BigEndian(span[pos..]);
                            pos += 4;
                            partitions.Add(new AssignedPartition(topic, partition));
                        }
                    }
                }

                result[member.MemberId] = partitions;
            }
            catch
            {
                result[member.MemberId] = new List<AssignedPartition>();
            }
        }

        return result;
    }
}
