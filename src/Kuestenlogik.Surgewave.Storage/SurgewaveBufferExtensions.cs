using System.Buffers.Binary;

namespace Kuestenlogik.Surgewave.Storage;

/// <summary>
/// Extension methods for ISurgewaveBuffer.
/// </summary>
public static class SurgewaveBufferExtensions
{
    /// <summary>
    /// Read a big-endian Int32 from the buffer at the specified offset.
    /// </summary>
    public static int ReadInt32BigEndian(this ISurgewaveBuffer buffer, int offset)
    {
        var span = buffer.Span;
        return BinaryPrimitives.ReadInt32BigEndian(span.Slice(offset, 4));
    }

    /// <summary>
    /// Read a big-endian Int64 from the buffer at the specified offset.
    /// </summary>
    public static long ReadInt64BigEndian(this ISurgewaveBuffer buffer, int offset)
    {
        var span = buffer.Span;
        return BinaryPrimitives.ReadInt64BigEndian(span.Slice(offset, 8));
    }

    /// <summary>
    /// Write a big-endian Int32 to the buffer at the specified offset.
    /// </summary>
    public static void WriteInt32BigEndian(this ISurgewaveWritableBuffer buffer, int offset, int value)
    {
        var span = buffer.Span;
        BinaryPrimitives.WriteInt32BigEndian(span.Slice(offset, 4), value);
    }

    /// <summary>
    /// Write a big-endian Int64 to the buffer at the specified offset.
    /// </summary>
    public static void WriteInt64BigEndian(this ISurgewaveWritableBuffer buffer, int offset, long value)
    {
        var span = buffer.Span;
        BinaryPrimitives.WriteInt64BigEndian(span.Slice(offset, 8), value);
    }
}
