using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;

/// <summary>
/// #60 Inc2/Inc5 — native SRWV payload for InterBrokerStopReplica: the sending controller's
/// identity/epoch, the target broker id, plus the partitions to stop, each with its leader epoch and
/// a delete flag. <see cref="ControllerId"/>/<see cref="ControllerEpoch"/> let the receiver fence a
/// stale push from a demoted controller (mirroring the Kafka-wire StopReplica handler); the target
/// <see cref="BrokerId"/> lets it refuse a misrouted stop — vital because a stop can delete data.
/// </summary>
public readonly record struct StopReplicaPayload(
    int ControllerId,
    int ControllerEpoch,
    int BrokerId,
    IReadOnlyList<(TopicPartition Tp, int LeaderEpoch, bool DeletePartition)> Partitions)
    : ISerializablePayload<StopReplicaPayload>
{
    // Lower bound per partition entry: TopicPartition (2-byte string length + 4-byte partition) +
    // 4-byte leader epoch + 1-byte delete flag. Guards the pre-allocation against a hostile count.
    private const int MinEntryBytes = 11;

    public static StopReplicaPayload Read(ref SurgewavePayloadReader reader)
    {
        var controllerId = reader.ReadInt32();
        var controllerEpoch = reader.ReadInt32();
        var brokerId = reader.ReadInt32();
        var count = reader.ReadInt32();
        if (count < 0 || count > reader.Remaining / MinEntryBytes)
            throw new InvalidDataException($"Corrupt StopReplica payload: entry count {count} exceeds {reader.Remaining} remaining bytes");

        var partitions = new List<(TopicPartition, int, bool)>(count);
        for (var i = 0; i < count; i++)
        {
            var tp = InterBrokerWire.ReadTopicPartition(ref reader);
            var leaderEpoch = reader.ReadInt32();
            var deletePartition = reader.ReadBoolean();
            partitions.Add((tp, leaderEpoch, deletePartition));
        }
        return new(controllerId, controllerEpoch, brokerId, partitions);
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(ControllerId);
        writer.WriteInt32(ControllerEpoch);
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
        writer.WriteInt32(ControllerId);
        writer.WriteInt32(ControllerEpoch);
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
        var size = 4 + 4 + 4 + 4;
        foreach (var (tp, _, _) in Partitions)
            size += InterBrokerWire.SizeOf(tp) + 4 + 1;
        return size;
    }
}
