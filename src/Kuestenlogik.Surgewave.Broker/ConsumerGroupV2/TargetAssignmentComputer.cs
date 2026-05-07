using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Broker.Native.Assignors;
using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;

/// <summary>
/// Bridges the KIP-848 group state (which uses topic IDs) to the broker-internal
/// <see cref="IPartitionAssignor"/> abstraction (which uses topic names) and applies
/// the resulting assignments back onto the group members' <c>TargetAssignment</c>.
/// </summary>
internal sealed class TargetAssignmentComputer(LogManager logManager)
{
    /// <summary>
    /// Recomputes the target assignment for every member of <paramref name="group"/>
    /// using the assignor selected by <c>group.AssignorName</c>. Members keep their
    /// previous target as user-data so sticky/cooperative-sticky can preserve it.
    /// </summary>
    public void Compute(ConsumerGroupV2State group)
    {
        if (group.Members.Count == 0)
        {
            group.AssignmentEpoch = group.GroupEpoch;
            return;
        }

        var subscribedTopics = CollectSubscribedTopics(group);
        var (topicPartitionCounts, topicNameToId, topicIdToName) =
            ResolveTopicMetadata(subscribedTopics);

        var subscriptions = BuildMemberSubscriptions(group, topicIdToName);

        var assignor = PartitionAssignorFactory.GetAssignor(group.AssignorName);
        var assignedByMember = assignor.Assign(
            [.. topicPartitionCounts.Keys],
            topicPartitionCounts,
            subscriptions);

        ApplyAssignment(group, assignedByMember, topicNameToId);
        group.AssignmentEpoch = group.GroupEpoch;
    }

    private static HashSet<string> CollectSubscribedTopics(ConsumerGroupV2State group)
    {
        var topics = new HashSet<string>(StringComparer.Ordinal);
        foreach (var member in group.Members.Values)
        {
            foreach (var topic in member.SubscribedTopicNames)
            {
                topics.Add(topic);
            }
        }
        return topics;
    }

    private (Dictionary<string, int> Counts, Dictionary<string, Guid> NameToId, Dictionary<Guid, string> IdToName)
        ResolveTopicMetadata(HashSet<string> subscribedTopics)
    {
        var counts = new Dictionary<string, int>(subscribedTopics.Count, StringComparer.Ordinal);
        var nameToId = new Dictionary<string, Guid>(subscribedTopics.Count, StringComparer.Ordinal);
        var idToName = new Dictionary<Guid, string>(subscribedTopics.Count);

        foreach (var topic in subscribedTopics)
        {
            var metadata = logManager.GetTopicMetadata(topic);
            if (metadata == null) continue;

            counts[topic] = metadata.PartitionCount;
            nameToId[topic] = metadata.TopicId;
            idToName[metadata.TopicId] = topic;
        }

        return (counts, nameToId, idToName);
    }

    private static List<MemberSubscription> BuildMemberSubscriptions(
        ConsumerGroupV2State group,
        Dictionary<Guid, string> topicIdToName)
    {
        var subscriptions = new List<MemberSubscription>(group.Members.Count);
        foreach (var member in group.Members.Values)
        {
            var userData = SerializePreviousAssignment(member.TargetAssignment, topicIdToName);
            subscriptions.Add(new MemberSubscription(
                member.MemberId,
                [.. member.SubscribedTopicNames],
                userData));
        }
        return subscriptions;
    }

    private static void ApplyAssignment(
        ConsumerGroupV2State group,
        Dictionary<string, List<AssignedPartition>> assignedByMember,
        Dictionary<string, Guid> topicNameToId)
    {
        foreach (var member in group.Members.Values)
        {
            if (!assignedByMember.TryGetValue(member.MemberId, out var partitions))
            {
                member.TargetAssignment = [];
                continue;
            }

            member.TargetAssignment = GroupByTopic(partitions, topicNameToId);
        }
    }

    private static List<TopicPartitionAssignment> GroupByTopic(
        List<AssignedPartition> partitions,
        Dictionary<string, Guid> topicNameToId)
    {
        if (partitions.Count == 0) return [];

        var byTopic = new Dictionary<string, List<int>>(StringComparer.Ordinal);
        foreach (var p in partitions)
        {
            if (!byTopic.TryGetValue(p.Topic, out var list))
            {
                list = [];
                byTopic[p.Topic] = list;
            }
            list.Add(p.Partition);
        }

        var result = new List<TopicPartitionAssignment>(byTopic.Count);
        foreach (var (topic, parts) in byTopic)
        {
            if (!topicNameToId.TryGetValue(topic, out var id)) continue;
            parts.Sort();
            result.Add(new TopicPartitionAssignment { TopicId = id, Partitions = parts });
        }
        return result;
    }

    /// <summary>
    /// Serialises the previous target assignment into the format
    /// <see cref="StickyAssignor"/> reads from <c>UserData</c>:
    /// <c>[topicCount(2)][topicLen(2)][topic][partitionCount(4)][partition(4)…]</c>.
    /// Only topics that resolve to a known name are emitted.
    /// </summary>
    internal static byte[] SerializePreviousAssignment(
        List<TopicPartitionAssignment> previous,
        Dictionary<Guid, string> topicIdToName)
    {
        if (previous.Count == 0) return [];

        var resolved = new List<(string Topic, List<int> Partitions)>(previous.Count);
        foreach (var topic in previous)
        {
            if (!topicIdToName.TryGetValue(topic.TopicId, out var name)) continue;
            resolved.Add((name, topic.Partitions));
        }
        if (resolved.Count == 0) return [];

        var size = 2;
        foreach (var (topic, partitions) in resolved)
        {
            size += 2 + Encoding.UTF8.GetByteCount(topic) + 4 + partitions.Count * 4;
        }

        var buffer = new byte[size];
        var span = buffer.AsSpan();
        var pos = 0;

        BinaryPrimitives.WriteInt16BigEndian(span[pos..], (short)resolved.Count);
        pos += 2;

        foreach (var (topic, partitions) in resolved)
        {
            var topicLen = Encoding.UTF8.GetByteCount(topic);
            BinaryPrimitives.WriteInt16BigEndian(span[pos..], (short)topicLen);
            pos += 2;
            Encoding.UTF8.GetBytes(topic, span[pos..]);
            pos += topicLen;

            BinaryPrimitives.WriteInt32BigEndian(span[pos..], partitions.Count);
            pos += 4;
            foreach (var p in partitions)
            {
                BinaryPrimitives.WriteInt32BigEndian(span[pos..], p);
                pos += 4;
            }
        }

        return buffer;
    }
}
