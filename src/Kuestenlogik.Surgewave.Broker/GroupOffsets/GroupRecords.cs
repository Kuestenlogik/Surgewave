using System.Buffers.Binary;
using System.Text;

namespace Kuestenlogik.Surgewave.Broker.GroupOffsets;

/// <summary>
/// The records of <c>__consumer_offsets</c> in Kafka's shapes (#205): an offset commit keyed by group, topic and
/// partition, and a group's metadata keyed by group. Big-endian, strings as int16 length and UTF-8, every key and value
/// led by its int16 schema version — so Kafka tooling that dumps the topic reads what it expects. A record without a
/// value is a tombstone: the offset or the group is gone, and compaction drops it.
/// </summary>
internal static class GroupRecords
{
    // Key versions 0 and 1 are offset commits, 2 is group metadata (Kafka's GroupCoordinatorRecordSerde).
    private const short OffsetCommitKeyVersion = 1;
    private const short GroupMetadataKeyVersion = 2;
    private const short OffsetCommitValueVersion = 3;
    private const short GroupMetadataValueVersion = 3;

    /// <summary>The key of a group's committed offset for one partition.</summary>
    public static byte[] OffsetCommitKey(string group, string topic, int partition)
    {
        var buffer = new byte[2 + Size(group) + Size(topic) + 4];
        var span = buffer.AsSpan();
        BinaryPrimitives.WriteInt16BigEndian(span, OffsetCommitKeyVersion);
        var at = 2;
        at += WriteString(span[at..], group);
        at += WriteString(span[at..], topic);
        BinaryPrimitives.WriteInt32BigEndian(span[at..], partition);
        return buffer;
    }

    /// <summary>The value of a committed offset: no leader epoch, no metadata, the commit time.</summary>
    public static byte[] OffsetCommitValue(long offset, long commitTimestampMs)
    {
        var buffer = new byte[2 + 8 + 4 + 2 + 8];
        var span = buffer.AsSpan();
        BinaryPrimitives.WriteInt16BigEndian(span, OffsetCommitValueVersion);
        BinaryPrimitives.WriteInt64BigEndian(span[2..], offset);
        BinaryPrimitives.WriteInt32BigEndian(span[10..], -1); // leader epoch: not tracked
        BinaryPrimitives.WriteInt16BigEndian(span[14..], 0);  // metadata: empty
        BinaryPrimitives.WriteInt64BigEndian(span[16..], commitTimestampMs);
        return buffer;
    }

    /// <summary>The key of a group's metadata.</summary>
    public static byte[] GroupMetadataKey(string group)
    {
        var buffer = new byte[2 + Size(group)];
        BinaryPrimitives.WriteInt16BigEndian(buffer, GroupMetadataKeyVersion);
        WriteString(buffer.AsSpan(2), group);
        return buffer;
    }

    /// <summary>
    /// A group's metadata: protocol type, generation, protocol and leader. The member list is written empty — a new
    /// coordinator has members rejoin, and what it keeps of the group is the generation the next one counts on from.
    /// </summary>
    public static byte[] GroupMetadataValue(string protocolType, int generation, string? protocol, string? leader, long timestampMs)
    {
        var buffer = new byte[2 + Size(protocolType) + 4 + Size(protocol) + Size(leader) + 8 + 4];
        var span = buffer.AsSpan();
        BinaryPrimitives.WriteInt16BigEndian(span, GroupMetadataValueVersion);
        var at = 2;
        at += WriteString(span[at..], protocolType);
        BinaryPrimitives.WriteInt32BigEndian(span[at..], generation);
        at += 4;
        at += WriteString(span[at..], protocol);
        at += WriteString(span[at..], leader);
        BinaryPrimitives.WriteInt64BigEndian(span[at..], timestampMs);
        at += 8;
        BinaryPrimitives.WriteInt32BigEndian(span[at..], 0); // members
        return buffer;
    }

    /// <summary>What a record of the topic says; <see cref="GroupRecord.Unknown"/> for a key version this broker does not write.</summary>
    public static GroupRecord Read(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (key.Length < 2)
            return GroupRecord.Unknown;

        var version = BinaryPrimitives.ReadInt16BigEndian(key);
        var at = 2;
        switch (version)
        {
            case 0 or 1:
            {
                var group = ReadString(key, ref at)!;
                var topic = ReadString(key, ref at)!;
                var partition = BinaryPrimitives.ReadInt32BigEndian(key[at..]);
                if (value.IsEmpty)
                    return new GroupRecord.OffsetRemoved(group, topic, partition);
                // Every value version starts with the offset after its version.
                return new GroupRecord.OffsetCommitted(group, topic, partition, BinaryPrimitives.ReadInt64BigEndian(value[2..]));
            }
            case 2:
            {
                var group = ReadString(key, ref at)!;
                if (value.IsEmpty)
                    return new GroupRecord.GroupRemoved(group);
                var v = 2;
                var protocolType = ReadString(value, ref v);
                return new GroupRecord.GroupMetadata(group, BinaryPrimitives.ReadInt32BigEndian(value[v..]), protocolType);
            }
            default:
                return GroupRecord.Unknown;
        }
    }

    private static int Size(string? value) => 2 + (value is null ? 0 : Encoding.UTF8.GetByteCount(value));

    // A null string is length -1, as Kafka writes it.
    private static int WriteString(Span<byte> span, string? value)
    {
        if (value is null)
        {
            BinaryPrimitives.WriteInt16BigEndian(span, -1);
            return 2;
        }
        var length = Encoding.UTF8.GetBytes(value, span[2..]);
        BinaryPrimitives.WriteInt16BigEndian(span, (short)length);
        return 2 + length;
    }

    private static string? ReadString(ReadOnlySpan<byte> span, ref int at)
    {
        var length = BinaryPrimitives.ReadInt16BigEndian(span[at..]);
        at += 2;
        if (length < 0)
            return null;
        var value = Encoding.UTF8.GetString(span.Slice(at, length));
        at += length;
        return value;
    }
}

/// <summary>One record of <c>__consumer_offsets</c>, as the store applies it.</summary>
internal abstract record GroupRecord
{
    public static readonly GroupRecord Unknown = new UnknownRecord();

    public sealed record OffsetCommitted(string Group, string Topic, int Partition, long Offset) : GroupRecord;

    public sealed record OffsetRemoved(string Group, string Topic, int Partition) : GroupRecord;

    public sealed record GroupMetadata(string Group, int Generation, string? ProtocolType) : GroupRecord;

    public sealed record GroupRemoved(string Group) : GroupRecord;

    private sealed record UnknownRecord : GroupRecord;
}
