using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

/// <summary>
/// Wire format for topic information (name, partition count, and the
/// per-topic produce-strategy hint from ADR-014). Shared between
/// broker (write) and client (read) to ensure consistency.
///
/// <para>
/// Wire layout: <c>name (string) | partitionCount (int32) | strategy (byte)</c>.
/// The strategy byte was added pre-launch with G21/P4; older v1
/// snapshots without that byte do not exist in the wild because the
/// protocol was not yet ratified.
/// </para>
/// </summary>
public readonly record struct TopicInfoPayload
{
    public string Name { get; init; }
    public int PartitionCount { get; init; }

    /// <summary>
    /// Broker-side hint that tells the client which write path the
    /// topic uses. <see cref="ProduceStrategy.Replicated"/> when unset.
    /// </summary>
    public ProduceStrategy Strategy { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static TopicInfoPayload Read(ref SurgewavePayloadReader reader)
    {
        return new TopicInfoPayload
        {
            Name = reader.ReadString() ?? string.Empty,
            PartitionCount = reader.ReadInt32(),
            Strategy = (ProduceStrategy)reader.ReadUInt8(),
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteInt32(PartitionCount);
        writer.WriteUInt8((byte)Strategy);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteInt32(PartitionCount);
        writer.WriteUInt8((byte)Strategy);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(Name ?? "") + // Name (length prefix + bytes)
        4 +                                                        // PartitionCount
        1;                                                         // Strategy
}
