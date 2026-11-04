using Kuestenlogik.Surgewave.Broker.ShareGroups;

namespace Kuestenlogik.Surgewave.Broker.GroupOffsets;

/// <summary>
/// A share group's state as Kafka's ShareGroup* records of <c>__consumer_offsets</c> (#219): each member's metadata (key
/// version 10), the group's epoch (11) and each member's epoch (14), in Kafka's encoding
/// (<see cref="FlexibleRecordCodec"/>). A record without a value is a tombstone.
/// </summary>
/// <remarks>
/// As in Kafka, the share partitions' state, their start offsets among it, is in <c>__share_group_state</c>, and the
/// group's configs are in the metadata log (#228).
/// </remarks>
internal static class ShareGroupRecords
{
    public const short MemberMetadataKeyVersion = 10;
    public const short GroupMetadataKeyVersion = 11;
    public const short CurrentMemberAssignmentKeyVersion = 14;

    private const sbyte StableState = 0;

    /// <summary>Whether a key of <c>__consumer_offsets</c> is one of a share group's records.</summary>
    public static bool IsShareGroupKey(ReadOnlySpan<byte> key) =>
        key.Length >= 2 && FlexibleRecordCodec.KeyVersion(key) is MemberMetadataKeyVersion or GroupMetadataKeyVersion or CurrentMemberAssignmentKeyVersion;

    /// <summary>Every record of the group's state as it is now.</summary>
    public static List<(byte[] Key, byte[] Value)> Of(ShareGroupState group)
    {
        var records = new List<(byte[], byte[])>
        {
            (FlexibleRecordCodec.Key(GroupMetadataKeyVersion, group.GroupId),
             FlexibleRecordCodec.Value(w => w.Int32(group.GroupEpoch))),
        };
        foreach (var member in group.Members.Values.OrderBy(m => m.MemberId, StringComparer.Ordinal))
        {
            records.Add((FlexibleRecordCodec.Key(MemberMetadataKeyVersion, group.GroupId, member.MemberId), FlexibleRecordCodec.Value(w =>
            {
                w.NullableString(member.RackId);
                w.String(member.ClientId ?? string.Empty);
                w.String(member.ClientHost ?? string.Empty);
                w.Array(member.SubscribedTopicNames, (w2, topic) => w2.String(topic));
            })));
            records.Add((FlexibleRecordCodec.Key(CurrentMemberAssignmentKeyVersion, group.GroupId, member.MemberId), FlexibleRecordCodec.Value(w =>
            {
                w.Int32(member.MemberEpoch);
                w.Int32(member.MemberEpoch); // previous member epoch: not tracked
                w.Int8(StableState);
                w.Array(Array.Empty<int>(), (_, _) => { }); // assigned partitions: every member is given all of them
            })));
        }
        return records;
    }

    /// <summary>The group the records describe, null when they name no group epoch.</summary>
    public static ShareGroupState? Read(string groupId, IEnumerable<(byte[] Key, byte[] Value)> records)
    {
        ShareGroupState? group = null;
        var members = new Dictionary<string, ShareGroupMember>(StringComparer.Ordinal);
        ShareGroupMember Member(string id) =>
            members.TryGetValue(id, out var m) ? m : members[id] = new ShareGroupMember { MemberId = id, LastHeartbeat = DateTime.UtcNow };

        foreach (var (key, value) in records)
        {
            if (value.Length == 0 || !IsShareGroupKey(key))
                continue;
            var memberId = FlexibleRecordCodec.SecondKeyString(key) ?? string.Empty;
            var r = new FlexibleReader(value);
            switch (FlexibleRecordCodec.KeyVersion(key))
            {
                case GroupMetadataKeyVersion:
                {
                    group = new ShareGroupState { GroupId = groupId, GroupEpoch = r.Int32() };
                    break;
                }
                case MemberMetadataKeyVersion:
                {
                    var m = Member(memberId);
                    m.RackId = r.NullableString();
                    m.ClientId = r.String();
                    m.ClientHost = r.String();
                    m.SubscribedTopicNames = r.StringArray();
                    break;
                }
                case CurrentMemberAssignmentKeyVersion:
                    Member(memberId).MemberEpoch = r.Int32();
                    break;
            }
        }

        if (group is null)
            return null;
        foreach (var member in members.Values.Where(m => m.ClientId is not null))
        {
            group.Members[member.MemberId] = member;
            foreach (var topic in member.SubscribedTopicNames)
                group.SubscribedTopics.Add(topic);
        }
        return group;
    }
}
