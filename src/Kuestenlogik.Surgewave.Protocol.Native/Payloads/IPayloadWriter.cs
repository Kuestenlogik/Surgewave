namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads;

/// <summary>
/// Interface for payload writers. Implemented by BigEndianWriter (the
/// broker-side response writer) so payloads can share a single
/// <c>WriteTo</c> body across the broker's IPayloadWriter sink and the
/// client's <see cref="SurgewavePayloadWriter"/> (the latter is a
/// <c>ref struct</c> and cannot implement interfaces, so payloads
/// duplicate-write via a parallel <c>Write(ref SurgewavePayloadWriter)</c>
/// method).
///
/// <para><b>Method contract — read this before adding implementations.</b></para>
///
/// All multi-byte numerics are big-endian. The two methods worth calling
/// out:
///
/// <list type="bullet">
///   <item>
///     <see cref="WriteString"/> — emits a Kafka-style length-prefixed
///     string: <c>int16 length (-1 = null)</c> + UTF-8 bytes.
///   </item>
///   <item>
///     <see cref="WriteBytes"/> — emits <b>raw bytes, NO length prefix</b>.
///     Callers MUST emit the count themselves (typically via
///     <see cref="WriteInt32"/>) ahead of the call. This matches
///     <c>BigEndianWriter.WriteBytes</c> (raw) and contrasts with
///     <c>SurgewavePayloadWriter.WriteBytes</c> which adds an int32
///     prefix; payloads that want the same wire shape across both
///     paths use <c>SurgewavePayloadWriter.WriteRaw</c> in the
///     <c>Write(ref ...)</c> path. Reading the bytes back uses
///     <c>reader.ReadRaw(len)</c>.
///   </item>
/// </list>
/// </summary>
public interface IPayloadWriter
{
    /// <summary>Writes a single signed byte.</summary>
    void WriteInt8(sbyte value);

    /// <summary>Writes a single unsigned byte.</summary>
    void WriteUInt8(byte value);

    /// <summary>Writes a big-endian int16.</summary>
    void WriteInt16(short value);

    /// <summary>Writes a big-endian uint16.</summary>
    void WriteUInt16(ushort value);

    /// <summary>Writes a big-endian int32.</summary>
    void WriteInt32(int value);

    /// <summary>Writes a big-endian uint32.</summary>
    void WriteUInt32(uint value);

    /// <summary>Writes a big-endian int64.</summary>
    void WriteInt64(long value);

    /// <summary>Writes a big-endian uint64.</summary>
    void WriteUInt64(ulong value);

    /// <summary>
    /// Writes a Kafka-style length-prefixed string: <c>int16 length
    /// (-1 = null)</c> followed by UTF-8 bytes.
    /// </summary>
    void WriteString(string? value);

    /// <summary>
    /// Writes raw bytes with <b>NO length prefix</b>. Callers MUST emit
    /// the count themselves (e.g. <c>WriteInt32(span.Length)</c>) before
    /// this call. See the interface doc-comment for the rationale and
    /// for the wire-shape parity with <c>SurgewavePayloadWriter.WriteRaw</c>.
    /// </summary>
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
