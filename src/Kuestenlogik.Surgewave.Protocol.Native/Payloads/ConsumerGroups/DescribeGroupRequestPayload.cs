using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

/// <summary>
/// Wire format for DescribeGroup request.
/// Shared between client (write) and broker (read) to ensure consistency.
/// </summary>
public readonly record struct DescribeGroupRequestPayload
{
    public string GroupId { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static DescribeGroupRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new DescribeGroupRequestPayload
        {
            GroupId = reader.ReadString() ?? ""
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(GroupId);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(GroupId);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(GroupId ?? "");
}
