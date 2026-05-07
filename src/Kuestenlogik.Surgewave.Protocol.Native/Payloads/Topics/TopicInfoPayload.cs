using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

/// <summary>
/// Wire format for topic information (name and partition count).
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct TopicInfoPayload
{
    public string Name { get; init; }
    public int PartitionCount { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static TopicInfoPayload Read(ref SurgewavePayloadReader reader)
    {
        return new TopicInfoPayload
        {
            Name = reader.ReadString() ?? string.Empty,
            PartitionCount = reader.ReadInt32()
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteInt32(PartitionCount);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteInt32(PartitionCount);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(Name ?? "") + // Name (length prefix + bytes)
        4;                                                        // PartitionCount
}
