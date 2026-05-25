using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

/// <summary>
/// Wire format for LeaveGroup response.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct LeaveGroupResponsePayload
{
    public ushort ErrorCode { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static LeaveGroupResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        return new LeaveGroupResponsePayload
        {
            ErrorCode = reader.ReadUInt16()
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() => 2; // ErrorCode
}
