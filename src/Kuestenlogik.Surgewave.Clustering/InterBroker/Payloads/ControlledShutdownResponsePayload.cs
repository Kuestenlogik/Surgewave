using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;

/// <summary>
/// The controller's answer to a <see cref="ControlledShutdownPayload"/>: which partitions the
/// departing broker still leads.
/// </summary>
/// <remarks>
/// An empty list means every leadership moved and the broker may stop serving. A non-empty one is
/// not an error — a partition whose ISR holds nobody else has no successor to move to, and the
/// caller has to decide between waiting and leaving anyway. Naming them is what lets it decide;
/// a bare status could not tell "nothing to do" from "nothing possible".
/// </remarks>
public readonly record struct ControlledShutdownResponsePayload(
    ClusterRpcStatus Status,
    IReadOnlyList<TopicPartition> RemainingPartitions)
    : ISerializablePayload<ControlledShutdownResponsePayload>
{
    public static ControlledShutdownResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var status = (ClusterRpcStatus)reader.ReadInt16();
        var count = reader.ReadInt32();
        var remaining = new List<TopicPartition>(count);
        for (var i = 0; i < count; i++)
        {
            remaining.Add(InterBrokerWire.ReadTopicPartition(ref reader));
        }

        return new(status, remaining);
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt16((short)Status);
        writer.WriteInt32(RemainingPartitions.Count);
        foreach (var tp in RemainingPartitions) InterBrokerWire.Write(ref writer, tp);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt16((short)Status);
        writer.WriteInt32(RemainingPartitions.Count);
        foreach (var tp in RemainingPartitions) InterBrokerWire.Write(writer, tp);
    }

    public int EstimateSize()
    {
        var size = 2 + 4;
        foreach (var tp in RemainingPartitions) size += InterBrokerWire.SizeOf(tp);
        return size;
    }
}
