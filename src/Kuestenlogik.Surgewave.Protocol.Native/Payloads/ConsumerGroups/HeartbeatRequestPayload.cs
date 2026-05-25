using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

/// <summary>
/// Wire format for Heartbeat request.
/// Shared between client (write) and broker (read) to ensure consistency.
/// </summary>
public readonly record struct HeartbeatRequestPayload
{
    public string GroupId { get; init; }
    public string MemberId { get; init; }
    public int GenerationId { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static HeartbeatRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new HeartbeatRequestPayload
        {
            GroupId = reader.ReadString() ?? "",
            MemberId = reader.ReadString() ?? "",
            GenerationId = reader.ReadInt32()
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteString(MemberId);
        writer.WriteInt32(GenerationId);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteString(MemberId);
        writer.WriteInt32(GenerationId);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(GroupId ?? "") +
        2 + System.Text.Encoding.UTF8.GetByteCount(MemberId ?? "") +
        4; // GenerationId
}
