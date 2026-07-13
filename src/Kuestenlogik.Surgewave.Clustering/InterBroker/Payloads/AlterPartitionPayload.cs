using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;

/// <summary>
/// #60 Inc5 — native SRWV payload for InterBrokerAlterPartition: the reverse ISR-propagation path
/// (#69). A partition leader reports its new in-sync replica set to the controller, which applies it
/// via <see cref="Replication.IIsrUpdateApplier"/>. One partition per frame — the notifier surface
/// (<see cref="Replication.IIsrChangeNotifier"/>) reports single-partition changes, so no batching
/// shape is needed. No controller epoch here: the SENDER is a leader, not the controller; staleness
/// is fenced by the leader epoch inside the applier.
/// </summary>
public readonly record struct AlterPartitionPayload(
    int LeaderId,
    int LeaderEpoch,
    TopicPartition Tp,
    IReadOnlyList<int> NewIsr)
    : ISerializablePayload<AlterPartitionPayload>
{
    public static AlterPartitionPayload Read(ref SurgewavePayloadReader reader)
    {
        var leaderId = reader.ReadInt32();
        var leaderEpoch = reader.ReadInt32();
        var tp = InterBrokerWire.ReadTopicPartition(ref reader);
        var newIsr = InterBrokerWire.ReadIntList(ref reader);
        return new(leaderId, leaderEpoch, tp, newIsr);
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(LeaderId);
        writer.WriteInt32(LeaderEpoch);
        InterBrokerWire.Write(ref writer, Tp);
        writer.WriteInt32(NewIsr.Count);
        foreach (var id in NewIsr) writer.WriteInt32(id);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(LeaderId);
        writer.WriteInt32(LeaderEpoch);
        InterBrokerWire.Write(writer, Tp);
        writer.WriteInt32(NewIsr.Count);
        foreach (var id in NewIsr) writer.WriteInt32(id);
    }

    public int EstimateSize()
        => 4 + 4 + InterBrokerWire.SizeOf(Tp) + 4 + NewIsr.Count * 4;
}
