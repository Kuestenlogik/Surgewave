using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

/// <summary>
/// Wire format for a single topic configuration key-value pair.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct TopicConfigPayload
{
    public string Key { get; init; }
    public string Value { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static TopicConfigPayload Read(ref SurgewavePayloadReader reader)
    {
        return new TopicConfigPayload
        {
            Key = reader.ReadString() ?? string.Empty,
            Value = reader.ReadString() ?? string.Empty
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Key);
        writer.WriteString(Value);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(Key);
        writer.WriteString(Value);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(Key ?? "") +   // Key (length prefix + bytes)
        2 + System.Text.Encoding.UTF8.GetByteCount(Value ?? "");  // Value (length prefix + bytes)
}
