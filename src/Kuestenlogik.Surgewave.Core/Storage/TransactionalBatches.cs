using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// The transaction fields of a record batch: marking a batch a broker writes itself as part of a producer's
/// transaction — the transactional offset commits a group coordinator writes into <c>__consumer_offsets</c> (#197) —
/// and reading back which producer a batch belongs to and which outcome a marker carries.
/// </summary>
public static class TransactionalBatches
{
    /// <summary>
    /// Marks <paramref name="batch"/> as written in the transaction of <paramref name="producerId"/> at
    /// <paramref name="producerEpoch"/>, and seals it with a new checksum.
    /// </summary>
    public static void MarkTransactional(Span<byte> batch, long producerId, short producerEpoch)
    {
        var attributesAt = KafkaConstants.RecordBatch.AttributesOffset;
        var attributes = BinaryPrimitives.ReadInt16BigEndian(batch[attributesAt..]);
        BinaryPrimitives.WriteInt16BigEndian(batch[attributesAt..], (short)(attributes | KafkaConstants.Attributes.IsTransactionalBit));
        BinaryPrimitives.WriteInt64BigEndian(batch[KafkaConstants.RecordBatch.ProducerIdOffset..], producerId);
        BinaryPrimitives.WriteInt16BigEndian(batch[KafkaConstants.RecordBatch.ProducerEpochOffset..], producerEpoch);
        BinaryPrimitives.WriteUInt32BigEndian(batch[KafkaConstants.RecordBatch.CrcOffset..], Crc32C.Compute(batch[attributesAt..]));
    }

    /// <summary>The producer a transactional batch belongs to; -1 for a batch outside any transaction.</summary>
    public static long TransactionalProducerOf(ReadOnlySpan<byte> batch) =>
        CompressionCodec.IsTransactional(batch)
            ? BinaryPrimitives.ReadInt64BigEndian(batch[KafkaConstants.RecordBatch.ProducerIdOffset..])
            : KafkaConstants.Producer.NoProducerId;

    /// <summary>
    /// Whether the control record of a transaction marker commits, read from its key as Kafka's
    /// <c>ControlRecordType.parse</c> reads it (version, type); null for a control record that is no commit or abort
    /// marker.
    /// </summary>
    public static bool? IsCommitMarker(ReadOnlySpan<byte> controlRecordKey)
    {
        if (controlRecordKey.Length < 4)
            return null;
        return BinaryPrimitives.ReadInt16BigEndian(controlRecordKey[2..]) switch
        {
            KafkaConstants.ControlRecordType.Commit => true,
            KafkaConstants.ControlRecordType.Abort => false,
            _ => null,
        };
    }
}
