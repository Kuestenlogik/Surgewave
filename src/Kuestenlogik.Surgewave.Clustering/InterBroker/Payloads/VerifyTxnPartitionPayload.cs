using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;

/// <summary>
/// A partition leader asking a transactional id's coordinator about a partition of the transaction (#197): whether
/// it was added, or under transactions V2 to add it — Kafka's AddPartitionsToTxn v4+ between brokers, verifyOnly for
/// V1. The answer is a <see cref="VerifyTxnPartitionResponsePayload"/>.
/// </summary>
public readonly record struct VerifyTxnPartitionPayload(
    string TransactionalId, long ProducerId, short ProducerEpoch, TopicPartition Partition, TransactionVersion Version)
    : ISerializablePayload<VerifyTxnPartitionPayload>
{
    public static VerifyTxnPartitionPayload Read(ref SurgewavePayloadReader reader)
    {
        var transactionalId = reader.ReadString() ?? string.Empty;
        var producerId = reader.ReadInt64();
        var producerEpoch = reader.ReadInt16();
        var partition = InterBrokerWire.ReadTopicPartition(ref reader);
        var version = (TransactionVersion)reader.ReadInt16();
        return new(transactionalId, producerId, producerEpoch, partition, version);
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(TransactionalId);
        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);
        InterBrokerWire.Write(ref writer, Partition);
        writer.WriteInt16((short)Version);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(TransactionalId);
        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);
        InterBrokerWire.Write(writer, Partition);
        writer.WriteInt16((short)Version);
    }

    public int EstimateSize() => 2 + TransactionalId.Length * 3 + 8 + 2 + InterBrokerWire.SizeOf(Partition) + 2;
}

/// <summary>The coordinator's answer to a <see cref="VerifyTxnPartitionPayload"/>.</summary>
public readonly record struct VerifyTxnPartitionResponsePayload(TxnErrorStatus Status)
    : ISerializablePayload<VerifyTxnPartitionResponsePayload>
{
    public static VerifyTxnPartitionResponsePayload Read(ref SurgewavePayloadReader reader)
        => new((TxnErrorStatus)reader.ReadInt16());

    public void Write(ref SurgewavePayloadWriter writer) => writer.WriteInt16((short)Status);

    public void WriteTo(IPayloadWriter writer) => writer.WriteInt16((short)Status);

    public int EstimateSize() => 2;
}
