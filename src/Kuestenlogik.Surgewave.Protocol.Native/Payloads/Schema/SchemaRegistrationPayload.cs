using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Schema;

/// <summary>
/// Wire format for RegisterSchema response.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct SchemaRegistrationPayload
{
    public int SchemaId { get; init; }
    public int Version { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static SchemaRegistrationPayload Read(ref SurgewavePayloadReader reader)
    {
        return new SchemaRegistrationPayload
        {
            SchemaId = reader.ReadInt32(),
            Version = reader.ReadInt32()
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(SchemaId);
        writer.WriteInt32(Version);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(SchemaId);
        writer.WriteInt32(Version);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        4 +     // SchemaId
        4;      // Version
}
