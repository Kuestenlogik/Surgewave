using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// #60 Inc7 — builds a transaction-marker control batch (a Kafka v2 RecordBatch carrying a single
/// commit/abort control record). This is the on-disk record format written to a partition log when a
/// transaction is committed/aborted, so it lives in Core and is shared by BOTH inter-broker wires —
/// the Kafka-wire <c>InterBrokerApiHandler</c> and the native inter-broker receive path — guaranteeing
/// byte-identical markers regardless of which wire replicated them.
/// </summary>
public static class ControlBatchBuilder
{
    /// <summary>
    /// Build a control batch for one transaction marker. <paramref name="controlType"/> is
    /// <see cref="KafkaConstants.ControlRecordType.Commit"/> or
    /// <see cref="KafkaConstants.ControlRecordType.Abort"/>. <paramref name="baseTimestampMs"/>
    /// defaults to the current time; pass a fixed value for deterministic output.
    /// </summary>
    public static byte[] BuildTransactionMarker(long producerId, short producerEpoch, short controlType, long? baseTimestampMs = null)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        var baseOffset = 0L;
        var baseTimestamp = baseTimestampMs ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Control record: key = version (int16 = 0), value = {version: int16, type: int16}
        Span<byte> controlKey = stackalloc byte[2];
        controlKey[0] = 0;
        controlKey[1] = 0; // Version 0

        Span<byte> controlValue = stackalloc byte[4];
        controlValue[0] = 0;
        controlValue[1] = 0; // Version
        controlValue[2] = (byte)(controlType >> 8);
        controlValue[3] = (byte)(controlType & 0xFF);

        // Build the record
        using var recordStream = new MemoryStream();

        recordStream.WriteByte(0); // attributes (int8) = 0
        recordStream.WriteByte(0); // timestampDelta (varlong zigzag) = 0
        recordStream.WriteByte(0); // offsetDelta (varint zigzag) = 0

        // keyLength (varint zigzag)
        WriteVarInt(recordStream, (int)ZigZag.Encode(controlKey.Length));
        recordStream.Write(controlKey);

        // valueLength (varint zigzag)
        WriteVarInt(recordStream, (int)ZigZag.Encode(controlValue.Length));
        recordStream.Write(controlValue);

        recordStream.WriteByte(0); // headers count (varint) = 0

        var recordBytes = recordStream.ToArray();

        short attributes = (short)(KafkaConstants.Attributes.IsTransactionalBit |
                                   KafkaConstants.Attributes.IsControlBatchBit);

        var recordsLength = 1 + recordBytes.Length;
        var batchLength = KafkaConstants.RecordBatch.HeaderSize -
                          KafkaConstants.RecordBatch.BaseOffsetSize -
                          KafkaConstants.RecordBatch.LengthSize +
                          recordsLength;

        // Write batch header
        WriteBigEndianInt64(writer, baseOffset);
        WriteBigEndianInt32(writer, batchLength);
        WriteBigEndianInt32(writer, 0); // Partition Leader Epoch
        writer.Write(KafkaConstants.Magic.V2);

        // Prepare CRC data
        using var crcStream = new MemoryStream();
        using var crcWriter = new BinaryWriter(crcStream);

        WriteBigEndianInt16(crcWriter, attributes);
        WriteBigEndianInt32(crcWriter, 0); // Last Offset Delta
        WriteBigEndianInt64(crcWriter, baseTimestamp);
        WriteBigEndianInt64(crcWriter, baseTimestamp); // Max Timestamp
        WriteBigEndianInt64(crcWriter, producerId);
        WriteBigEndianInt16(crcWriter, producerEpoch);
        WriteBigEndianInt32(crcWriter, 0); // Base Sequence
        WriteBigEndianInt32(crcWriter, 1); // Record Count

        WriteVarInt(crcStream, (int)ZigZag.Encode(recordBytes.Length)); // record length (zigzag varint)
        crcStream.Write(recordBytes, 0, recordBytes.Length);

        var crcData = crcStream.ToArray();
        var crc = Crc32C.Compute(crcData);

        WriteBigEndianUInt32(writer, crc);
        writer.Write(crcData);

        return stream.ToArray();
    }

    private static void WriteVarInt(Stream stream, int value)
    {
        while ((value & ~0x7F) != 0)
        {
            stream.WriteByte((byte)((value & 0x7F) | 0x80));
            value = (int)((uint)value >> 7);
        }
        stream.WriteByte((byte)value);
    }

    private static void WriteBigEndianInt16(BinaryWriter writer, short value)
    {
        Span<byte> buffer = stackalloc byte[2];
        BinaryPrimitives.WriteInt16BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteBigEndianInt32(BinaryWriter writer, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteBigEndianInt64(BinaryWriter writer, long value)
    {
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteBigEndianUInt32(BinaryWriter writer, uint value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(buffer, value);
        writer.Write(buffer);
    }
}
