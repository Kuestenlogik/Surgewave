using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;

/// <summary>
/// #60 Inc2 — native SRWV payload for InterBrokerStopReplica: the target broker id plus the
/// partitions to stop, each with its leader epoch and a delete flag. Fire-and-forget (bare Task on
/// IControllerReplicaRpc), no response payload.
/// </summary>
public readonly record struct StopReplicaPayload(
    int BrokerId,
    IReadOnlyList<(TopicPartition Tp, int LeaderEpoch, bool DeletePartition)> Partitions)
    : ISerializablePayload<StopReplicaPayload>
{
    public static StopReplicaPayload Read(ref SurgewavePayloadReader reader)
    {
        var brokerId = reader.ReadInt32();
        var count = reader.ReadInt32();
        var partitions = new List<(TopicPartition, int, bool)>(count);
        for (var i = 0; i < count; i++)
        {
            var tp = InterBrokerWire.ReadTopicPartition(ref reader);
            var leaderEpoch = reader.ReadInt32();
            var deletePartition = reader.ReadBoolean();
            partitions.Add((tp, leaderEpoch, deletePartition));
        }
        return new(brokerId, partitions);
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(BrokerId);
        writer.WriteInt32(Partitions.Count);
        foreach (var (tp, leaderEpoch, deletePartition) in Partitions)
        {
            InterBrokerWire.Write(ref writer, tp);
            writer.WriteInt32(leaderEpoch);
            writer.WriteBoolean(deletePartition);
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(BrokerId);
        writer.WriteInt32(Partitions.Count);
        foreach (var (tp, leaderEpoch, deletePartition) in Partitions)
        {
            InterBrokerWire.Write(writer, tp);
            writer.WriteInt32(leaderEpoch);
            writer.WriteBoolean(deletePartition);
        }
    }

    public int EstimateSize()
    {
        var size = 4 + 4;
        foreach (var (tp, _, _) in Partitions)
            size += InterBrokerWire.SizeOf(tp) + 4 + 1;
        return size;
    }
}
