using Kuestenlogik.Surgewave.Coordination.ShareGroups;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;

/// <summary>What a share-state request asks of the key's share coordinator — Kafka's Read/Write/Initialize/DeleteShareGroupState.</summary>
public enum ShareStateOperation : short
{
    Read = 0,
    Write = 1,
    Initialize = 2,
    Delete = 3,
}

/// <summary>
/// A partition leader or a group coordinator asking a key's share coordinator about a share partition's state (#228).
/// <see cref="State"/> is set for a write, <see cref="StartOffset"/> for an initialization.
/// </summary>
public readonly record struct ShareStatePayload(ShareStateOperation Operation, ShareStateKey Key, long StartOffset, ShareStateSnapshot? State)
    : ISerializablePayload<ShareStatePayload>
{
    public static ShareStatePayload Read(ref SurgewavePayloadReader reader)
    {
        var operation = (ShareStateOperation)reader.ReadInt16();
        var key = ShareStateWire.ReadKey(ref reader);
        var startOffset = reader.ReadInt64();
        var state = reader.ReadBoolean() ? ShareStateWire.ReadState(ref reader) : null;
        return new(operation, key, startOffset, state);
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt16((short)Operation);
        ShareStateWire.WriteKey(ref writer, Key);
        writer.WriteInt64(StartOffset);
        writer.WriteBoolean(State is not null);
        if (State is not null)
            ShareStateWire.WriteState(ref writer, State);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt16((short)Operation);
        ShareStateWire.WriteKey(writer, Key);
        writer.WriteInt64(StartOffset);
        writer.WriteBoolean(State is not null);
        if (State is not null)
            ShareStateWire.WriteState(writer, State);
    }

    public int EstimateSize() => 2 + ShareStateWire.SizeOf(Key) + 8 + 1 + (State is null ? 0 : ShareStateWire.SizeOf(State));
}

/// <summary>The share coordinator's answer: a status, the state epoch of an initialization, the state of a read.</summary>
public readonly record struct ShareStateResponsePayload(ShareStateStatus Status, int StateEpoch, ShareStateSnapshot? State)
    : ISerializablePayload<ShareStateResponsePayload>
{
    public static ShareStateResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var status = (ShareStateStatus)reader.ReadInt16();
        var stateEpoch = reader.ReadInt32();
        var state = reader.ReadBoolean() ? ShareStateWire.ReadState(ref reader) : null;
        return new(status, stateEpoch, state);
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt16((short)Status);
        writer.WriteInt32(StateEpoch);
        writer.WriteBoolean(State is not null);
        if (State is not null)
            ShareStateWire.WriteState(ref writer, State);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt16((short)Status);
        writer.WriteInt32(StateEpoch);
        writer.WriteBoolean(State is not null);
        if (State is not null)
            ShareStateWire.WriteState(writer, State);
    }

    public int EstimateSize() => 2 + 4 + 1 + (State is null ? 0 : ShareStateWire.SizeOf(State));
}

internal static class ShareStateWire
{
    public static ShareStateKey ReadKey(ref SurgewavePayloadReader r)
    {
        var group = r.ReadString() ?? string.Empty;
        var topicId = Guid.Parse(r.ReadString() ?? Guid.Empty.ToString());
        return new ShareStateKey(group, topicId, r.ReadInt32());
    }

    public static void WriteKey(ref SurgewavePayloadWriter w, ShareStateKey key)
    {
        w.WriteString(key.GroupId);
        w.WriteString(key.TopicId.ToString());
        w.WriteInt32(key.Partition);
    }

    public static void WriteKey(IPayloadWriter w, ShareStateKey key)
    {
        w.WriteString(key.GroupId);
        w.WriteString(key.TopicId.ToString());
        w.WriteInt32(key.Partition);
    }

    public static int SizeOf(ShareStateKey key) => 2 + key.GroupId.Length * 3 + 2 + 36 * 3 + 4;

    public static ShareStateSnapshot ReadState(ref SurgewavePayloadReader r)
    {
        var stateEpoch = r.ReadInt32();
        var leaderEpoch = r.ReadInt32();
        var startOffset = r.ReadInt64();
        var count = r.ReadInt32();
        if (count < 0 || count > r.Remaining / 20)
            throw new InvalidDataException($"Corrupt share-state payload: {count} batches in {r.Remaining} bytes");
        var batches = new List<ShareStateBatch>(count);
        for (var i = 0; i < count; i++)
            batches.Add(new ShareStateBatch(r.ReadInt64(), r.ReadInt64(), (ShareRecordState)r.ReadInt16(), r.ReadInt16()));
        return new ShareStateSnapshot(stateEpoch, leaderEpoch, startOffset, batches);
    }

    public static void WriteState(ref SurgewavePayloadWriter w, ShareStateSnapshot state)
    {
        w.WriteInt32(state.StateEpoch);
        w.WriteInt32(state.LeaderEpoch);
        w.WriteInt64(state.StartOffset);
        w.WriteInt32(state.Batches.Count);
        foreach (var b in state.Batches)
        {
            w.WriteInt64(b.FirstOffset);
            w.WriteInt64(b.LastOffset);
            w.WriteInt16((short)b.State);
            w.WriteInt16(b.DeliveryCount);
        }
    }

    public static void WriteState(IPayloadWriter w, ShareStateSnapshot state)
    {
        w.WriteInt32(state.StateEpoch);
        w.WriteInt32(state.LeaderEpoch);
        w.WriteInt64(state.StartOffset);
        w.WriteInt32(state.Batches.Count);
        foreach (var b in state.Batches)
        {
            w.WriteInt64(b.FirstOffset);
            w.WriteInt64(b.LastOffset);
            w.WriteInt16((short)b.State);
            w.WriteInt16(b.DeliveryCount);
        }
    }

    public static int SizeOf(ShareStateSnapshot state) => 4 + 4 + 8 + 4 + state.Batches.Count * 20;
}
