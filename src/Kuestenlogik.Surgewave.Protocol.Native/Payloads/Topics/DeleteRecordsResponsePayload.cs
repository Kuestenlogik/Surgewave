namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

/// <summary>
/// Wire format for DeleteRecords response.
/// </summary>
public readonly record struct DeleteRecordsResponsePayload
{
    public string TopicName { get; init; }
    public int Partition { get; init; }
    public long LowWatermark { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static DeleteRecordsResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        return new DeleteRecordsResponsePayload
        {
            TopicName = reader.ReadString() ?? string.Empty,
            Partition = reader.ReadInt32(),
            LowWatermark = reader.ReadInt64()
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(TopicName);
        writer.WriteInt32(Partition);
        writer.WriteInt64(LowWatermark);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(TopicName);
        writer.WriteInt32(Partition);
        writer.WriteInt64(LowWatermark);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(TopicName ?? "") + // TopicName (length prefix + bytes)
        4 +                                                             // Partition
        8;                                                              // LowWatermark
}
