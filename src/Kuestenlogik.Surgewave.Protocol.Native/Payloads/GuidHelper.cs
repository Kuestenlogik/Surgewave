namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads;

/// <summary>
/// Shared helper for reading/writing .NET Guids as big-endian UUIDs (Kafka wire format).
/// .NET Guid stores the first three components in native (little-endian) byte order,
/// while Kafka/UUID uses big-endian throughout. These methods handle the byte-swapping.
/// </summary>
internal static class GuidHelper
{
    /// <summary>
    /// Read a big-endian UUID (16 bytes) from the reader and return a .NET Guid.
    /// </summary>
    internal static Guid ReadGuid(ref SurgewavePayloadReader reader)
    {
        var span = reader.ReadRaw(16);
        // Convert from big-endian UUID to .NET mixed-endian Guid
        Span<byte> guidBytes = stackalloc byte[16];
        guidBytes[0] = span[3];
        guidBytes[1] = span[2];
        guidBytes[2] = span[1];
        guidBytes[3] = span[0];
        guidBytes[4] = span[5];
        guidBytes[5] = span[4];
        guidBytes[6] = span[7];
        guidBytes[7] = span[6];
        span.Slice(8, 8).CopyTo(guidBytes.Slice(8));
        return new Guid(guidBytes);
    }

    /// <summary>
    /// Write a .NET Guid as a big-endian UUID (16 bytes) to SurgewavePayloadWriter.
    /// Uses ToByteArray() + individual WriteUInt8 to avoid ref-struct scoping issues with stackalloc.
    /// </summary>
    internal static void WriteGuid(ref SurgewavePayloadWriter writer, Guid value)
    {
        var guidBytes = value.ToByteArray();
        // Convert from .NET mixed-endian to big-endian UUID format
        writer.WriteUInt8(guidBytes[3]);
        writer.WriteUInt8(guidBytes[2]);
        writer.WriteUInt8(guidBytes[1]);
        writer.WriteUInt8(guidBytes[0]);
        writer.WriteUInt8(guidBytes[5]);
        writer.WriteUInt8(guidBytes[4]);
        writer.WriteUInt8(guidBytes[7]);
        writer.WriteUInt8(guidBytes[6]);
        for (var i = 8; i < 16; i++)
            writer.WriteUInt8(guidBytes[i]);
    }

    /// <summary>
    /// Write a .NET Guid as a big-endian UUID (16 bytes) to IPayloadWriter.
    /// </summary>
    internal static void WriteGuid(IPayloadWriter writer, Guid value)
    {
        var guidBytes = value.ToByteArray();
        // Convert from .NET mixed-endian to big-endian UUID format
        byte[] beBytes =
        [
            guidBytes[3], guidBytes[2], guidBytes[1], guidBytes[0],
            guidBytes[5], guidBytes[4],
            guidBytes[7], guidBytes[6],
            guidBytes[8], guidBytes[9], guidBytes[10], guidBytes[11],
            guidBytes[12], guidBytes[13], guidBytes[14], guidBytes[15]
        ];
        writer.WriteBytes(beBytes);
    }
}
