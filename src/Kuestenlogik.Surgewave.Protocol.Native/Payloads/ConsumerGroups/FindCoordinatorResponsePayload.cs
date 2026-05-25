using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

/// <summary>
/// Wire format for FindCoordinator response.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct FindCoordinatorResponsePayload
{
    public ushort ErrorCode { get; init; }
    public int CoordinatorId { get; init; }
    public string Host { get; init; }
    public int Port { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static FindCoordinatorResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        return new FindCoordinatorResponsePayload
        {
            ErrorCode = reader.ReadUInt16(),
            CoordinatorId = reader.ReadInt32(),
            Host = reader.ReadString() ?? "",
            Port = reader.ReadInt32()
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt32(CoordinatorId);
        writer.WriteString(Host);
        writer.WriteInt32(Port);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt32(CoordinatorId);
        writer.WriteString(Host);
        writer.WriteInt32(Port);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + // ErrorCode
        4 + // CoordinatorId
        2 + System.Text.Encoding.UTF8.GetByteCount(Host ?? "") +
        4;  // Port
}
