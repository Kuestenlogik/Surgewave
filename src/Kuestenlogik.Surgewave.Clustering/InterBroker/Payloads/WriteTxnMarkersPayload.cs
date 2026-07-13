using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;

/// <summary>
/// #60 Inc2 — native SRWV payload for InterBrokerWriteTxnMarkers: replicate a commit/abort marker
/// for a producer to a follower hosting the involved partitions. Carries the fields the neutral
/// <c>ITransactionMarkerReplicator.ReplicateMarkersAsync</c> is built from.
/// </summary>
public readonly record struct WriteTxnMarkersRequestPayload(
    string TransactionalId,
    long ProducerId,
    short ProducerEpoch,
    IReadOnlyList<TopicPartition> Partitions,
    bool Commit,
    int CoordinatorEpoch)
    : ISerializablePayload<WriteTxnMarkersRequestPayload>
{
    public static WriteTxnMarkersRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var transactionalId = reader.ReadString() ?? string.Empty;
        var producerId = reader.ReadInt64();
        var producerEpoch = reader.ReadInt16();
        var count = reader.ReadInt32();
        var partitions = new List<TopicPartition>(count);
        for (var i = 0; i < count; i++)
            partitions.Add(InterBrokerWire.ReadTopicPartition(ref reader));
        var commit = reader.ReadBoolean();
        var coordinatorEpoch = reader.ReadInt32();
        return new(transactionalId, producerId, producerEpoch, partitions, commit, coordinatorEpoch);
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(TransactionalId);
        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);
        writer.WriteInt32(Partitions.Count);
        foreach (var tp in Partitions)
            InterBrokerWire.Write(ref writer, tp);
        writer.WriteBoolean(Commit);
        writer.WriteInt32(CoordinatorEpoch);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(TransactionalId);
        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);
        writer.WriteInt32(Partitions.Count);
        foreach (var tp in Partitions)
            InterBrokerWire.Write(writer, tp);
        writer.WriteBoolean(Commit);
        writer.WriteInt32(CoordinatorEpoch);
    }

    public int EstimateSize()
    {
        var size = 2 + TransactionalId.Length * 3 + 8 + 2 + 4 + 1 + 4;
        foreach (var tp in Partitions)
            size += InterBrokerWire.SizeOf(tp);
        return size;
    }
}

/// <summary>
/// #60 Inc2 — native SRWV response for InterBrokerWriteTxnMarkers: a single status for the marker
/// write on the receiving follower. The coordinator aggregates these into a
/// <c>MarkerReplicationResult</c> across followers (Inc7).
/// </summary>
public readonly record struct WriteTxnMarkersResponsePayload(ClusterRpcStatus Status)
    : ISerializablePayload<WriteTxnMarkersResponsePayload>
{
    public static WriteTxnMarkersResponsePayload Read(ref SurgewavePayloadReader reader)
        => new((ClusterRpcStatus)reader.ReadInt16());

    public void Write(ref SurgewavePayloadWriter writer) => writer.WriteInt16((short)Status);

    public void WriteTo(IPayloadWriter writer) => writer.WriteInt16((short)Status);

    public int EstimateSize() => 2;
}
