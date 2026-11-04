using Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;

namespace Kuestenlogik.Surgewave.Broker.GroupOffsets;

/// <summary>
/// A KIP-848 consumer group's state as Kafka's ConsumerGroup* records of <c>__consumer_offsets</c> (#219): the group's
/// epoch (key version 3), each member's metadata (5), the target assignment's epoch (6), each member's target assignment
/// (7) and each member's current assignment (8), in Kafka's encoding (<see cref="FlexibleRecordCodec"/>). A record
/// without a value is a tombstone.
/// </summary>
/// <remarks>
/// <para>
/// Kafka keeps the server assignor and the rebalance timeout with each member; this broker has them per group, so each
/// member's record carries the group's, and the group takes them back from its members.
/// </para>
/// <para>
/// Not kept: what a member reports it owns (it reports it again with its next heartbeat), the time of its last heartbeat
/// (a coordinator that takes the group over gives every member a session from then) and the per-partition assignment
/// epochs of KIP-1251, for which the member epoch stands in.
/// </para>
/// </remarks>
internal static class ConsumerGroupRecords
{
    public const short GroupMetadataKeyVersion = 3;
    public const short MemberMetadataKeyVersion = 5;
    public const short TargetAssignmentMetadataKeyVersion = 6;
    public const short TargetAssignmentMemberKeyVersion = 7;
    public const short CurrentMemberAssignmentKeyVersion = 8;

    private const sbyte StableState = 0;

    /// <summary>Whether a key of <c>__consumer_offsets</c> is one of a KIP-848 consumer group's records.</summary>
    public static bool IsConsumerGroupKey(ReadOnlySpan<byte> key) =>
        key.Length >= 2 && FlexibleRecordCodec.KeyVersion(key) is >= GroupMetadataKeyVersion and <= CurrentMemberAssignmentKeyVersion;

    /// <summary>Every record of the group's state as it is now.</summary>
    public static List<(byte[] Key, byte[] Value)> Of(ConsumerGroupV2State group)
    {
        var records = new List<(byte[], byte[])>
        {
            (FlexibleRecordCodec.Key(GroupMetadataKeyVersion, group.GroupId), FlexibleRecordCodec.Value(w => w.Int32(group.GroupEpoch))),
            (FlexibleRecordCodec.Key(TargetAssignmentMetadataKeyVersion, group.GroupId), FlexibleRecordCodec.Value(w => w.Int32(group.AssignmentEpoch))),
        };
        foreach (var member in group.Members.Values.OrderBy(m => m.MemberId, StringComparer.Ordinal))
        {
            records.Add((FlexibleRecordCodec.Key(MemberMetadataKeyVersion, group.GroupId, member.MemberId), FlexibleRecordCodec.Value(w =>
            {
                w.NullableString(member.InstanceId);
                w.NullableString(member.RackId);
                w.String(member.ClientId ?? string.Empty);
                w.String(member.ClientHost ?? string.Empty);
                w.Array(member.SubscribedTopicNames, (w2, topic) => w2.String(topic));
                w.NullableString(member.SubscribedTopicRegex);
                w.Int32(group.RebalanceTimeoutMs);
                w.NullableString(group.AssignorName);
            })));
            records.Add((FlexibleRecordCodec.Key(TargetAssignmentMemberKeyVersion, group.GroupId, member.MemberId),
                FlexibleRecordCodec.Value(w => Partitions(w, member.TargetAssignment))));
            records.Add((FlexibleRecordCodec.Key(CurrentMemberAssignmentKeyVersion, group.GroupId, member.MemberId), FlexibleRecordCodec.Value(w =>
            {
                w.Int32(member.MemberEpoch);
                w.Int32(member.MemberEpoch); // previous member epoch: not tracked
                w.Int8(StableState);
                Partitions(w, member.Assignment);
                Partitions(w, []); // partitions pending revocation: the reconciler derives them
            })));
        }
        return records;
    }

    /// <summary>
    /// The group the records describe, null when they name no group epoch — every record of it was a tombstone, or the
    /// group never wrote one.
    /// </summary>
    public static ConsumerGroupV2State? Read(string groupId, IEnumerable<(byte[] Key, byte[] Value)> records)
    {
        ConsumerGroupV2State? group = null;
        var members = new Dictionary<string, ConsumerGroupV2Member>(StringComparer.Ordinal);
        var settings = new Dictionary<string, (int RebalanceTimeoutMs, string? Assignor)>(StringComparer.Ordinal);
        int? assignmentEpoch = null;
        ConsumerGroupV2Member Member(string id) =>
            members.TryGetValue(id, out var m) ? m : members[id] = new ConsumerGroupV2Member { MemberId = id, LastHeartbeat = DateTime.UtcNow };

        foreach (var (key, value) in records)
        {
            if (value.Length == 0 || !IsConsumerGroupKey(key))
                continue;
            var memberId = FlexibleRecordCodec.SecondKeyString(key) ?? string.Empty;
            var r = new FlexibleReader(value);
            switch (FlexibleRecordCodec.KeyVersion(key))
            {
                case GroupMetadataKeyVersion:
                    group = new ConsumerGroupV2State { GroupId = groupId, GroupEpoch = r.Int32(), Initialized = true };
                    break;
                case TargetAssignmentMetadataKeyVersion:
                    assignmentEpoch = r.Int32();
                    break;
                case MemberMetadataKeyVersion:
                {
                    var m = Member(memberId);
                    m.InstanceId = r.NullableString();
                    m.RackId = r.NullableString();
                    m.ClientId = r.String();
                    m.ClientHost = r.String();
                    m.SubscribedTopicNames = r.StringArray();
                    m.SubscribedTopicRegex = r.NullableString();
                    settings[memberId] = (r.Int32(), r.NullableString());
                    break;
                }
                case TargetAssignmentMemberKeyVersion:
                    Member(memberId).TargetAssignment = ReadPartitions(ref r);
                    break;
                case CurrentMemberAssignmentKeyVersion:
                {
                    var m = Member(memberId);
                    m.MemberEpoch = r.Int32();
                    _ = r.Int32(); // previous member epoch
                    _ = r.Int8(); // state
                    m.Assignment = ReadPartitions(ref r);
                    break;
                }
            }
        }

        if (group is null)
            return null;
        if (assignmentEpoch is { } epoch)
            group.AssignmentEpoch = epoch;
        foreach (var member in members.Values.Where(m => m.ClientId is not null))
        {
            group.Members[member.MemberId] = member;
            if (settings.TryGetValue(member.MemberId, out var s))
            {
                if (s.Assignor is { } assignor)
                    group.AssignorName = assignor;
                group.RebalanceTimeoutMs = s.RebalanceTimeoutMs;
            }
        }
        return group;
    }

    // Kafka's TopicPartitions: a topic id and its partitions, each entry with its tagged fields.
    private static void Partitions(FlexibleWriter w, IReadOnlyCollection<TopicPartitionAssignment> assignment) => w.Array(assignment, (w2, topic) =>
    {
        w2.Uuid(topic.TopicId);
        w2.Array(topic.Partitions, (w3, p) => w3.Int32(p));
        w2.TaggedFields([]);
    });

    private static List<TopicPartitionAssignment> ReadPartitions(ref FlexibleReader r)
    {
        var count = r.ArrayCount();
        var topics = new List<TopicPartitionAssignment>(count);
        for (var i = 0; i < count; i++)
        {
            var topicId = r.Uuid();
            var partitions = r.ArrayCount();
            var list = new List<int>(partitions);
            for (var p = 0; p < partitions; p++)
                list.Add(r.Int32());
            _ = r.TaggedFields();
            topics.Add(new TopicPartitionAssignment { TopicId = topicId, Partitions = list });
        }
        return topics;
    }
}
