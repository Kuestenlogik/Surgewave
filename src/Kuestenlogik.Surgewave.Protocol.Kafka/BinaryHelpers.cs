using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Static helper methods for big-endian binary operations.
/// Used by protocol handlers where extension method syntax isn't appropriate.
/// </summary>
public static class BinaryHelpers
{
    /// <summary>
    /// Read a big-endian Int16 from the reader
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static short ReadInt16BigEndian(BinaryReader reader)
    {
        Span<byte> buffer = stackalloc byte[2];
        int bytesRead = reader.Read(buffer);
        if (bytesRead < 2)
            throw new InvalidDataException($"Expected 2 bytes but read {bytesRead}");
        return BinaryPrimitives.ReadInt16BigEndian(buffer);
    }

    /// <summary>
    /// Read a big-endian Int32 from the reader
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadInt32BigEndian(BinaryReader reader)
    {
        Span<byte> buffer = stackalloc byte[4];
        int bytesRead = reader.Read(buffer);
        if (bytesRead < 4)
            throw new InvalidDataException($"Expected 4 bytes but read {bytesRead}");
        return BinaryPrimitives.ReadInt32BigEndian(buffer);
    }

    /// <summary>
    /// Read a big-endian Int64 from the reader
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ReadInt64BigEndian(BinaryReader reader)
    {
        Span<byte> buffer = stackalloc byte[8];
        reader.Read(buffer);
        return BinaryPrimitives.ReadInt64BigEndian(buffer);
    }

    /// <summary>
    /// Read a Kafka-style string (2-byte length prefix + UTF-8 bytes)
    /// </summary>
    public static string ReadString(BinaryReader reader)
    {
        var length = ReadInt16BigEndian(reader);
        if (length <= 0) return string.Empty;

        return ReadUtf8Interned(reader, length);
    }

    /// <summary>
    /// Read a Kafka-style nullable string (2-byte length prefix + UTF-8 bytes, -1 for null)
    /// </summary>
    public static string? ReadNullableString(BinaryReader reader)
    {
        var length = ReadInt16BigEndian(reader);
        if (length < 0) return null;
        if (length == 0) return string.Empty;

        return ReadUtf8Interned(reader, length);
    }

    /// <summary>
    /// Reads <paramref name="length"/> UTF-8 bytes into stack space and interns them through the
    /// shared wire-string cache — replaces the byte[] + string pair the old ReadBytes/GetString
    /// combination allocated per string. Strings longer than the stack buffer fall back to a
    /// plain decode (they are past the cache's length limit anyway).
    /// A short read at end-of-stream yields the shorter string, exactly as ReadBytes did.
    /// </summary>
    private static string ReadUtf8Interned(BinaryReader reader, int length)
    {
        if (length <= 256)
        {
            Span<byte> buffer = stackalloc byte[256];
            var span = buffer[..length];
            var total = 0;
            while (total < length)
            {
                var read = reader.Read(span[total..]);
                if (read == 0) break;
                total += read;
            }

            return KafkaProtocolReader.InternUtf8(span[..total]);
        }

        var bytes = reader.ReadBytes(length);
        return Encoding.UTF8.GetString(bytes);
    }

    /// <summary>
    /// Write a big-endian Int32 to a byte array
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte[] WriteInt32BigEndian(int value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        return bytes;
    }
}

/// <summary>
/// Extension methods for reading big-endian binary data from BinaryReader.
/// Kafka protocol uses big-endian byte order for all numeric values.
/// </summary>
public static class BinaryReaderExtensions
{
    /// <summary>
    /// Read a big-endian Int16 from the reader
    /// </summary>
    public static short ReadInt16BigEndian(this BinaryReader reader)
        => BinaryHelpers.ReadInt16BigEndian(reader);

    /// <summary>
    /// Read a big-endian Int32 from the reader
    /// </summary>
    public static int ReadInt32BigEndian(this BinaryReader reader)
        => BinaryHelpers.ReadInt32BigEndian(reader);

    /// <summary>
    /// Read a big-endian Int64 from the reader
    /// </summary>
    public static long ReadInt64BigEndian(this BinaryReader reader)
        => BinaryHelpers.ReadInt64BigEndian(reader);

    /// <summary>
    /// Read a Kafka-style string (2-byte length prefix + UTF-8 bytes)
    /// </summary>
    public static string ReadKafkaString(this BinaryReader reader)
        => BinaryHelpers.ReadString(reader);
}

/// <summary>
/// Extension methods for writing big-endian binary data.
/// </summary>
public static class BinaryWriterExtensions
{
    /// <summary>
    /// Write a big-endian Int32 and return the bytes
    /// </summary>
    public static byte[] ToBytesBigEndian(this int value)
        => BinaryHelpers.WriteInt32BigEndian(value);
}
