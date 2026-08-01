namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// Builds well-formed v2 RecordBatch bytes for storage tests.
/// </summary>
internal static class TestRecordBatch
{
    public static byte[] Create(long baseOffset, int recordCount, int valueSize = 100)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var valueData = new byte[valueSize];
        System.Security.Cryptography.RandomNumberGenerator.Fill(valueData);

        WriteBigEndian(writer, baseOffset);

        var batchLengthPos = stream.Position;
        WriteBigEndian(writer, 0); // placeholder
        var batchDataStart = stream.Position;

        WriteBigEndian(writer, 0);                  // partition leader epoch
        writer.Write((byte)2);                      // magic
        WriteBigEndian(writer, 0u);                 // CRC placeholder
        WriteBigEndian(writer, (short)0);           // attributes
        WriteBigEndian(writer, recordCount - 1);    // last offset delta
        WriteBigEndian(writer, timestamp);          // base timestamp
        WriteBigEndian(writer, timestamp);          // max timestamp
        WriteBigEndian(writer, -1L);                // producer id
        WriteBigEndian(writer, (short)-1);          // producer epoch
        WriteBigEndian(writer, -1);                 // base sequence
        WriteBigEndian(writer, recordCount);        // record count

        for (int i = 0; i < recordCount; i++)
        {
            WriteRecord(writer, valueData, i);
        }

        var batchLength = (int)(stream.Position - batchDataStart);
        var endPos = stream.Position;
        stream.Position = batchLengthPos;
        WriteBigEndian(writer, batchLength);
        stream.Position = endPos;

        return stream.ToArray();
    }

    private static void WriteRecord(BinaryWriter writer, byte[] value, int offsetDelta)
    {
        using var recordStream = new MemoryStream();
        using var recordWriter = new BinaryWriter(recordStream);

        recordWriter.Write((byte)0);            // attributes
        WriteVarInt(recordWriter, 0);           // timestamp delta
        WriteVarInt(recordWriter, offsetDelta); // offset delta
        WriteVarInt(recordWriter, -1);          // key length (null)
        WriteVarInt(recordWriter, value.Length);
        recordWriter.Write(value);
        WriteVarInt(recordWriter, 0);           // header count

        var recordBytes = recordStream.ToArray();
        WriteVarInt(writer, recordBytes.Length);
        writer.Write(recordBytes);
    }

    private static void WriteBigEndian(BinaryWriter writer, short value)
    {
        writer.Write((byte)(value >> 8));
        writer.Write((byte)value);
    }

    private static void WriteBigEndian(BinaryWriter writer, int value)
    {
        writer.Write((byte)(value >> 24));
        writer.Write((byte)(value >> 16));
        writer.Write((byte)(value >> 8));
        writer.Write((byte)value);
    }

    private static void WriteBigEndian(BinaryWriter writer, long value)
    {
        writer.Write((byte)(value >> 56));
        writer.Write((byte)(value >> 48));
        writer.Write((byte)(value >> 40));
        writer.Write((byte)(value >> 32));
        writer.Write((byte)(value >> 24));
        writer.Write((byte)(value >> 16));
        writer.Write((byte)(value >> 8));
        writer.Write((byte)value);
    }

    private static void WriteBigEndian(BinaryWriter writer, uint value)
    {
        writer.Write((byte)(value >> 24));
        writer.Write((byte)(value >> 16));
        writer.Write((byte)(value >> 8));
        writer.Write((byte)value);
    }

    private static void WriteVarInt(BinaryWriter writer, int value)
    {
        var v = (uint)((value << 1) ^ (value >> 31));
        while ((v & ~0x7F) != 0)
        {
            writer.Write((byte)((v & 0x7F) | 0x80));
            v >>= 7;
        }
        writer.Write((byte)v);
    }
}
