using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;

namespace Kuestenlogik.Surgewave.Broker.GroupOffsets;

/// <summary>
/// A KIP-848 consumer group's state as Kafka's ConsumerGroup* records of <c>__consumer_offsets</c> (#219): the group's
/// epoch (key version 3), each member's metadata (5), the target assignment's epoch (6), each member's target assignment
/// (7) and each member's current assignment (8). Keys as Kafka writes them — the int16 key version, then the group and
/// member ids as int16-length strings; values led by their int16 version and encoded flexibly (compact strings and
/// arrays, an empty tagged-field section). A record without a value is a tombstone.
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

    private const short ValueVersion = 0;
    private const sbyte StableState = 0;

    /// <summary>Whether a key of <c>__consumer_offsets</c> is one of a KIP-848 consumer group's records.</summary>
    public static bool IsConsumerGroupKey(ReadOnlySpan<byte> key) =>
        key.Length >= 2 && BinaryPrimitives.ReadInt16BigEndian(key) is >= GroupMetadataKeyVersion and <= CurrentMemberAssignmentKeyVersion;

    /// <summary>The group a consumer-group record's key names.</summary>
    public static string GroupOf(ReadOnlySpan<byte> key)
    {
        var at = 2;
        return ReadKeyString(key, ref at) ?? string.Empty;
    }

    /// <summary>Every record of the group's state as it is now.</summary>
    public static List<(byte[] Key, byte[] Value)> Of(ConsumerGroupV2State group)
    {
        var records = new List<(byte[], byte[])>
        {
            (Key(GroupMetadataKeyVersion, group.GroupId, null), Value(w => w.Int32(group.GroupEpoch))),
            (Key(TargetAssignmentMetadataKeyVersion, group.GroupId, null), Value(w => w.Int32(group.AssignmentEpoch))),
        };
        foreach (var member in group.Members.Values.OrderBy(m => m.MemberId, StringComparer.Ordinal))
        {
            records.Add((Key(MemberMetadataKeyVersion, group.GroupId, member.MemberId), Value(w =>
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
            records.Add((Key(TargetAssignmentMemberKeyVersion, group.GroupId, member.MemberId),
                Value(w => w.Partitions(member.TargetAssignment))));
            records.Add((Key(CurrentMemberAssignmentKeyVersion, group.GroupId, member.MemberId), Value(w =>
            {
                w.Int32(member.MemberEpoch);
                w.Int32(member.MemberEpoch); // previous member epoch: not tracked
                w.Int8(StableState);
                w.Partitions(member.Assignment);
                w.Partitions([]); // partitions pending revocation: the reconciler derives them
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
        int? assignmentEpoch = null;
        var groupSettings = new Dictionary<string, (int RebalanceTimeoutMs, string? Assignor)>(StringComparer.Ordinal);
        ConsumerGroupV2Member Member(string id) =>
            members.TryGetValue(id, out var m) ? m : members[id] = new ConsumerGroupV2Member { MemberId = id, LastHeartbeat = DateTime.UtcNow };

        foreach (var (key, value) in records)
        {
            if (value.Length == 0)
                continue;
            var version = BinaryPrimitives.ReadInt16BigEndian(key);
            var at = 2;
            _ = ReadKeyString(key, ref at); // group
            var memberId = version is MemberMetadataKeyVersion or TargetAssignmentMemberKeyVersion or CurrentMemberAssignmentKeyVersion
                ? ReadKeyString(key, ref at) ?? string.Empty
                : null;
            var r = new Reader(value);
            switch (version)
            {
                case GroupMetadataKeyVersion:
                    group = new ConsumerGroupV2State { GroupId = groupId, GroupEpoch = r.Int32(), Initialized = true };
                    break;
                case TargetAssignmentMetadataKeyVersion:
                    assignmentEpoch = r.Int32();
                    break;
                case MemberMetadataKeyVersion:
                {
                    var m = Member(memberId!);
                    m.InstanceId = r.NullableString();
                    m.RackId = r.NullableString();
                    m.ClientId = r.String();
                    m.ClientHost = r.String();
                    m.SubscribedTopicNames = r.StringArray();
                    m.SubscribedTopicRegex = r.NullableString();
                    groupSettings[m.MemberId] = (r.Int32(), r.NullableString());
                    break;
                }
                case TargetAssignmentMemberKeyVersion:
                    Member(memberId!).TargetAssignment = r.Partitions();
                    break;
                case CurrentMemberAssignmentKeyVersion:
                {
                    var m = Member(memberId!);
                    m.MemberEpoch = r.Int32();
                    _ = r.Int32(); // previous member epoch
                    _ = r.Int8(); // state
                    m.Assignment = r.Partitions();
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
            if (groupSettings.TryGetValue(member.MemberId, out var settings))
            {
                if (settings.Assignor is { } assignor)
                    group.AssignorName = assignor;
                group.RebalanceTimeoutMs = settings.RebalanceTimeoutMs;
            }
        }
        return group;
    }

    // Keys are not flexible: int16 version, int16-length strings.
    private static byte[] Key(short version, string groupId, string? memberId)
    {
        var size = 2 + 2 + Encoding.UTF8.GetByteCount(groupId) + (memberId is null ? 0 : 2 + Encoding.UTF8.GetByteCount(memberId));
        var buffer = new byte[size];
        BinaryPrimitives.WriteInt16BigEndian(buffer, version);
        var at = 2;
        at += WriteKeyString(buffer.AsSpan(at), groupId);
        if (memberId is not null)
            WriteKeyString(buffer.AsSpan(at), memberId);
        return buffer;
    }

    private static int WriteKeyString(Span<byte> span, string value)
    {
        var length = Encoding.UTF8.GetBytes(value, span[2..]);
        BinaryPrimitives.WriteInt16BigEndian(span, (short)length);
        return 2 + length;
    }

    private static string? ReadKeyString(ReadOnlySpan<byte> span, ref int at)
    {
        var length = BinaryPrimitives.ReadInt16BigEndian(span[at..]);
        at += 2;
        if (length < 0)
            return null;
        var value = Encoding.UTF8.GetString(span.Slice(at, length));
        at += length;
        return value;
    }

    private static byte[] Value(Action<Writer> write)
    {
        var writer = new Writer();
        writer.Int16(ValueVersion);
        write(writer);
        writer.UnsignedVarint(0); // tagged fields
        return writer.ToArray();
    }

    private sealed class Writer
    {
        private readonly List<byte> _bytes = [];

        public byte[] ToArray() => [.. _bytes];

        public void Int8(sbyte value) => _bytes.Add((byte)value);

        public void Int16(short value)
        {
            Span<byte> b = stackalloc byte[2];
            BinaryPrimitives.WriteInt16BigEndian(b, value);
            _bytes.AddRange(b);
        }

        public void Int32(int value)
        {
            Span<byte> b = stackalloc byte[4];
            BinaryPrimitives.WriteInt32BigEndian(b, value);
            _bytes.AddRange(b);
        }

        public void UnsignedVarint(uint value)
        {
            while ((value & ~0x7Fu) != 0)
            {
                _bytes.Add((byte)((value & 0x7F) | 0x80));
                value >>= 7;
            }
            _bytes.Add((byte)value);
        }

        public void String(string value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            UnsignedVarint((uint)bytes.Length + 1);
            _bytes.AddRange(bytes);
        }

        public void NullableString(string? value)
        {
            if (value is null)
                UnsignedVarint(0);
            else
                String(value);
        }

        public void Array<T>(IReadOnlyCollection<T> items, Action<Writer, T> write)
        {
            UnsignedVarint((uint)items.Count + 1);
            foreach (var item in items)
                write(this, item);
        }

        // Kafka's TopicPartitions: a topic id and its partitions, each entry with its tagged fields.
        public void Partitions(IReadOnlyCollection<TopicPartitionAssignment> assignment) => Array(assignment, (w, topic) =>
        {
            Span<byte> uuid = stackalloc byte[16];
            topic.TopicId.TryWriteBytes(uuid, bigEndian: true, out _);
            w._bytes.AddRange(uuid);
            w.Array(topic.Partitions, (w2, p) => w2.Int32(p));
            w.UnsignedVarint(0);
        });
    }

    private ref struct Reader(ReadOnlySpan<byte> value)
    {
        private readonly ReadOnlySpan<byte> _value = value;
        private int _at = 2; // value version

        public sbyte Int8() => (sbyte)_value[_at++];

        public int Int32()
        {
            var v = BinaryPrimitives.ReadInt32BigEndian(_value[_at..]);
            _at += 4;
            return v;
        }

        public uint UnsignedVarint()
        {
            uint result = 0;
            var shift = 0;
            byte b;
            do
            {
                b = _value[_at++];
                result |= (uint)(b & 0x7F) << shift;
                shift += 7;
            }
            while ((b & 0x80) != 0);
            return result;
        }

        public string String() => NullableString() ?? string.Empty;

        public string? NullableString()
        {
            var length = (int)UnsignedVarint() - 1;
            if (length < 0)
                return null;
            var s = Encoding.UTF8.GetString(_value.Slice(_at, length));
            _at += length;
            return s;
        }

        public List<string> StringArray()
        {
            var count = (int)UnsignedVarint() - 1;
            var items = new List<string>(Math.Max(count, 0));
            for (var i = 0; i < count; i++)
                items.Add(String());
            return items;
        }

        public List<TopicPartitionAssignment> Partitions()
        {
            var count = (int)UnsignedVarint() - 1;
            var topics = new List<TopicPartitionAssignment>(Math.Max(count, 0));
            for (var i = 0; i < count; i++)
            {
                var topicId = new Guid(_value.Slice(_at, 16), bigEndian: true);
                _at += 16;
                var partitions = (int)UnsignedVarint() - 1;
                var list = new List<int>(Math.Max(partitions, 0));
                for (var p = 0; p < partitions; p++)
                    list.Add(Int32());
                SkipTaggedFields();
                topics.Add(new TopicPartitionAssignment { TopicId = topicId, Partitions = list });
            }
            return topics;
        }

        private void SkipTaggedFields()
        {
            var fields = UnsignedVarint();
            for (var f = 0; f < fields; f++)
            {
                _ = UnsignedVarint(); // tag
                _at += (int)UnsignedVarint(); // size
            }
        }
    }
}
