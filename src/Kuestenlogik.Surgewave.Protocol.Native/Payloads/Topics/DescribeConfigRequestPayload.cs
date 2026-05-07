using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

/// <summary>
/// Wire format for DescribeConfig request.
/// Shared between broker (read) and client (write) to ensure consistency.
/// </summary>
public readonly record struct DescribeConfigRequestPayload
{
    public string TopicName { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static DescribeConfigRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new DescribeConfigRequestPayload
        {
            TopicName = reader.ReadString() ?? string.Empty
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(TopicName);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(TopicName);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(TopicName ?? ""); // TopicName (length prefix + bytes)
}
