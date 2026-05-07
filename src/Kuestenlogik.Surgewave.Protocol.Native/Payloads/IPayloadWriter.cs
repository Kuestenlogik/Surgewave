namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads;

/// <summary>
/// Interface for payload writers. Implemented by both SurgewavePayloadWriter (via adapter)
/// and BigEndianWriter to enable shared payload serialization.
/// </summary>
public interface IPayloadWriter
{
    void WriteInt8(sbyte value);
    void WriteUInt8(byte value);
    void WriteInt16(short value);
    void WriteUInt16(ushort value);
    void WriteInt32(int value);
    void WriteUInt32(uint value);
    void WriteInt64(long value);
    void WriteUInt64(ulong value);
    void WriteString(string? value);
    void WriteBytes(ReadOnlySpan<byte> value);
}

/// <summary>
/// Extension methods to write payloads using IPayloadWriter interface.
/// </summary>
public static class PayloadWriterExtensions
{
    /// <summary>
    /// Write a boolean as a single byte.
    /// </summary>
    public static void WriteBool(this IPayloadWriter writer, bool value)
        => writer.WriteUInt8(value ? (byte)1 : (byte)0);

    /// <summary>
    /// Write a boolean value (1 byte: 0=false, 1=true)
    /// </summary>
    public static void WriteBoolean(this IPayloadWriter writer, bool value)
        => writer.WriteUInt8(value ? (byte)1 : (byte)0);

    /// <summary>
    /// Write a nullable string with explicit null marker (1 byte prefix)
    /// </summary>
    public static void WriteNullableString(this IPayloadWriter writer, string? value)
    {
        writer.WriteBoolean(value != null);
        if (value != null)
        {
            writer.WriteString(value);
        }
    }
}
