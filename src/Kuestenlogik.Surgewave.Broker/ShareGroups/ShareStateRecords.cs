using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Broker.GroupOffsets;
using Kuestenlogik.Surgewave.Coordination.ShareGroups;

namespace Kuestenlogik.Surgewave.Broker.ShareGroups;

/// <summary>
/// The records of <c>__share_group_state</c> (#228) as Kafka's share coordinator writes them: a ShareSnapshot keyed by
/// group, topic id and partition (key version 0), its value the snapshot epoch, state epoch, leader epoch, start offset,
/// create and write timestamps and the state batches. This coordinator writes snapshots only, no ShareUpdate deltas. A
/// record without a value is a tombstone.
/// </summary>
internal static class ShareStateRecords
{
    private const short SnapshotKeyVersion = 0;

    public static byte[] Key(ShareStateKey key)
    {
        var group = Encoding.UTF8.GetBytes(key.GroupId);
        var buffer = new byte[2 + 2 + group.Length + 16 + 4];
        var span = buffer.AsSpan();
        BinaryPrimitives.WriteInt16BigEndian(span, SnapshotKeyVersion);
        BinaryPrimitives.WriteInt16BigEndian(span[2..], (short)group.Length);
        group.CopyTo(span[4..]);
        key.TopicId.TryWriteBytes(span.Slice(4 + group.Length, 16), bigEndian: true, out _);
        BinaryPrimitives.WriteInt32BigEndian(span[(4 + group.Length + 16)..], key.Partition);
        return buffer;
    }

    /// <summary>The key a snapshot record names; null for a key version this coordinator does not write.</summary>
    public static ShareStateKey? ReadKey(ReadOnlySpan<byte> key)
    {
        if (key.Length < 4 || BinaryPrimitives.ReadInt16BigEndian(key) != SnapshotKeyVersion)
            return null;
        var length = BinaryPrimitives.ReadInt16BigEndian(key[2..]);
        var group = Encoding.UTF8.GetString(key.Slice(4, length));
        var topicId = new Guid(key.Slice(4 + length, 16), bigEndian: true);
        var partition = BinaryPrimitives.ReadInt32BigEndian(key[(4 + length + 16)..]);
        return new ShareStateKey(group, topicId, partition);
    }

    public static byte[] Value(ShareStateSnapshot state, int snapshotEpoch, long timestampMs) => FlexibleRecordCodec.Value(w =>
    {
        w.Int32(snapshotEpoch);
        w.Int32(state.StateEpoch);
        w.Int32(state.LeaderEpoch);
        w.Int64(state.StartOffset);
        w.Int64(timestampMs); // create timestamp
        w.Int64(timestampMs); // write timestamp
        w.Array(state.Batches, (w2, b) =>
        {
            w2.Int64(b.FirstOffset);
            w2.Int64(b.LastOffset);
            w2.Int8((sbyte)b.State);
            w2.Int16(b.DeliveryCount);
            w2.TaggedFields([]);
        });
    });

    public static (int SnapshotEpoch, ShareStateSnapshot State) ReadValue(ReadOnlySpan<byte> value)
    {
        var r = new FlexibleReader(value);
        var snapshotEpoch = r.Int32();
        var stateEpoch = r.Int32();
        var leaderEpoch = r.Int32();
        var startOffset = r.Int64();
        _ = r.Int64();
        _ = r.Int64();
        var count = r.ArrayCount();
        var batches = new List<ShareStateBatch>(count);
        for (var i = 0; i < count; i++)
        {
            var first = r.Int64();
            var last = r.Int64();
            var state = (ShareRecordState)r.Int8();
            var deliveryCount = r.Int16();
            _ = r.TaggedFields();
            batches.Add(new ShareStateBatch(first, last, state, deliveryCount));
        }
        return (snapshotEpoch, new ShareStateSnapshot(stateEpoch, leaderEpoch, startOffset, batches));
    }
}
