using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

/// <summary>
/// Wire format for SyncGroup response.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct SyncGroupResponsePayload
{
    public ushort ErrorCode { get; init; }
    public byte[] Assignment { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static SyncGroupResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var errorCode = reader.ReadUInt16();
        var assignmentLength = reader.ReadInt32();
        var assignment = assignmentLength > 0 ? reader.ReadRaw(assignmentLength).ToArray() : Array.Empty<byte>();

        return new SyncGroupResponsePayload
        {
            ErrorCode = errorCode,
            Assignment = assignment
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt32(Assignment.Length);
        writer.WriteRaw(Assignment);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt32(Assignment.Length);
        writer.WriteBytes(Assignment);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + // ErrorCode
        4 + // Assignment length
        Assignment.Length;
}
