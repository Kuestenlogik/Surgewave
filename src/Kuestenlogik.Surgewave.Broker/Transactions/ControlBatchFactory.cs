using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// Factory for creating Kafka control batches (transaction commit/abort markers).
/// </summary>
public static class ControlBatchFactory
{
    /// <summary>
    /// Creates a control batch (transaction marker).
    /// </summary>
    public static byte[] CreateControlBatch(long producerId, short producerEpoch, short controlType)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        // We'll create a minimal control batch
        // The key is the version (int16) and the value is the control type info

        var baseOffset = 0L;
        var baseTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Control record: key = version (int16 = 0), value = {version: int16, type: int16}
        var controlKey = new byte[2];
        controlKey[0] = 0;
        controlKey[1] = 0; // Version 0

        var controlValue = new byte[4];
        controlValue[0] = 0;
        controlValue[1] = 0; // Version
        controlValue[2] = (byte)(controlType >> 8);
        controlValue[3] = (byte)(controlType & 0xFF);

        // Build the record
        using var recordStream = new MemoryStream();

        // attributes (int8) = 0
        recordStream.WriteByte(0);

        // timestampDelta (varlong zigzag) = 0
        recordStream.WriteByte(0);

        // offsetDelta (varint zigzag) = 0
        recordStream.WriteByte(0);

        // keyLength (varint zigzag)
        var keyLengthEncoded = ZigZag.Encode(controlKey.Length);
        WriteVarInt(recordStream, (int)keyLengthEncoded);
        recordStream.Write(controlKey, 0, controlKey.Length);

        // valueLength (varint zigzag)
        var valueLengthEncoded = ZigZag.Encode(controlValue.Length);
        WriteVarInt(recordStream, (int)valueLengthEncoded);
        recordStream.Write(controlValue, 0, controlValue.Length);

        // headers count (varint) = 0
        recordStream.WriteByte(0);

        var recordBytes = recordStream.ToArray();

        // Build the full batch
        // Attributes: compression=0, timestamp=CreateTime, transactional=true, control=true
        short attributes = (short)(KafkaConstants.Attributes.IsTransactionalBit | KafkaConstants.Attributes.IsControlBatchBit);

        // Calculate batch length (everything after length field)
        var recordsLength = 1 + recordBytes.Length; // length varint + record bytes
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

        // Record length (zigzag varint)
        var recordLengthEncoded = ZigZag.Encode(recordBytes.Length);
        WriteVarInt(crcStream, (int)recordLengthEncoded);
        crcStream.Write(recordBytes, 0, recordBytes.Length);

        var crcData = crcStream.ToArray();
        var crc = Kuestenlogik.Surgewave.Core.Util.Crc32C.Compute(crcData);

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
        System.Buffers.Binary.BinaryPrimitives.WriteInt16BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteBigEndianInt32(BinaryWriter writer, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteBigEndianInt64(BinaryWriter writer, long value)
    {
        Span<byte> buffer = stackalloc byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteBigEndianUInt32(BinaryWriter writer, uint value)
    {
        Span<byte> buffer = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(buffer, value);
        writer.Write(buffer);
    }
}
