using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;

/// <summary>
/// #60 Inc2 — native SRWV payload carrying a list of (TopicPartition, PartitionState). Serves both
/// the InterBrokerLeaderAndIsr and InterBrokerUpdateMetadata opcodes (they carry the same shape;
/// the opcode distinguishes intent). Fire-and-forget on the wire — IControllerReplicaRpc exposes
/// these as bare Tasks, so there is no response payload.
/// </summary>
public readonly record struct PartitionStatesPayload(IReadOnlyList<(TopicPartition Tp, PartitionState State)> Entries)
    : ISerializablePayload<PartitionStatesPayload>
{
    public static PartitionStatesPayload Read(ref SurgewavePayloadReader reader)
    {
        var count = reader.ReadInt32();
        var entries = new List<(TopicPartition, PartitionState)>(count);
        for (var i = 0; i < count; i++)
        {
            var tp = InterBrokerWire.ReadTopicPartition(ref reader);
            var state = InterBrokerWire.ReadState(ref reader, tp);
            entries.Add((tp, state));
        }
        return new(entries);
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Entries.Count);
        foreach (var (tp, state) in Entries)
        {
            InterBrokerWire.Write(ref writer, tp);
            InterBrokerWire.WriteState(ref writer, state);
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(Entries.Count);
        foreach (var (tp, state) in Entries)
        {
            InterBrokerWire.Write(writer, tp);
            InterBrokerWire.WriteState(writer, state);
        }
    }

    public int EstimateSize()
    {
        var size = 4;
        foreach (var (tp, state) in Entries)
            size += InterBrokerWire.SizeOf(tp) + InterBrokerWire.SizeOfState(state);
        return size;
    }
}
