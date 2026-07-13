using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;

/// <summary>
/// #60 Inc2 — shared read/write helpers for the composite fields that recur across the native
/// inter-broker payloads (TopicPartition, int lists, PartitionState). Each write has a ref-struct
/// (client, pre-sized) and an <see cref="IPayloadWriter"/> (broker-side) form so the two paths stay
/// byte-identical.
/// </summary>
internal static class InterBrokerWire
{
    // 2-byte length prefix + worst-case UTF-8 (<=3 bytes per UTF-16 char).
    private static int StrSize(string s) => 2 + s.Length * 3;

    // --- TopicPartition ---
    public static void Write(ref SurgewavePayloadWriter w, TopicPartition tp)
    {
        w.WriteString(tp.Topic);
        w.WriteInt32(tp.Partition);
    }

    public static void Write(IPayloadWriter w, TopicPartition tp)
    {
        w.WriteString(tp.Topic);
        w.WriteInt32(tp.Partition);
    }

    public static TopicPartition ReadTopicPartition(ref SurgewavePayloadReader r)
        => new() { Topic = r.ReadString() ?? string.Empty, Partition = r.ReadInt32() };

    public static int SizeOf(TopicPartition tp) => StrSize(tp.Topic) + 4;

    // --- int list ---
    public static void Write(ref SurgewavePayloadWriter w, List<int> list)
    {
        w.WriteInt32(list.Count);
        foreach (var v in list) w.WriteInt32(v);
    }

    public static void Write(IPayloadWriter w, List<int> list)
    {
        w.WriteInt32(list.Count);
        foreach (var v in list) w.WriteInt32(v);
    }

    public static List<int> ReadIntList(ref SurgewavePayloadReader r)
    {
        var n = r.ReadInt32();
        // Reject a bogus/hostile count before pre-allocating: each element is a 4-byte int, so the count
        // can never exceed the remaining bytes / 4 (#60 Inc4 puts this decoder on the wire).
        if (n < 0 || n > r.Remaining / sizeof(int))
            throw new InvalidDataException($"Corrupt inter-broker payload: int-list count {n} exceeds {r.Remaining} remaining bytes");

        var list = new List<int>(n);
        for (var i = 0; i < n; i++) list.Add(r.ReadInt32());
        return list;
    }

    public static int SizeOf(List<int> list) => 4 + list.Count * 4;

    // --- PartitionState (its TopicPartition is serialized by the caller and passed back on read) ---
    public static void WriteState(ref SurgewavePayloadWriter w, PartitionState s)
    {
        w.WriteInt32(s.LeaderBrokerId);
        w.WriteInt32(s.LeaderEpoch);
        Write(ref w, s.Replicas);
        Write(ref w, s.Isr);
        Write(ref w, s.OfflineReplicas);
        w.WriteInt32(s.MinInSyncReplicas);
        w.WriteInt64(s.HighWatermark);
        w.WriteInt64(s.LogStartOffset);
    }

    public static void WriteState(IPayloadWriter w, PartitionState s)
    {
        w.WriteInt32(s.LeaderBrokerId);
        w.WriteInt32(s.LeaderEpoch);
        Write(w, s.Replicas);
        Write(w, s.Isr);
        Write(w, s.OfflineReplicas);
        w.WriteInt32(s.MinInSyncReplicas);
        w.WriteInt64(s.HighWatermark);
        w.WriteInt64(s.LogStartOffset);
    }

    public static PartitionState ReadState(ref SurgewavePayloadReader r, TopicPartition tp)
        => new()
        {
            TopicPartition = tp,
            LeaderBrokerId = r.ReadInt32(),
            LeaderEpoch = r.ReadInt32(),
            Replicas = ReadIntList(ref r),
            Isr = ReadIntList(ref r),
            OfflineReplicas = ReadIntList(ref r),
            MinInSyncReplicas = r.ReadInt32(),
            HighWatermark = r.ReadInt64(),
            LogStartOffset = r.ReadInt64(),
        };

    public static int SizeOfState(PartitionState s)
        => 4 + 4 + SizeOf(s.Replicas) + SizeOf(s.Isr) + SizeOf(s.OfflineReplicas) + 4 + 8 + 8;
}
