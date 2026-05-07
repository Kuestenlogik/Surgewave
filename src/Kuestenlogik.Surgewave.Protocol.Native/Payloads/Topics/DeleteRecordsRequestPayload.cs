namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

/// <summary>
/// Wire format for DeleteRecords request.
/// </summary>
public readonly record struct DeleteRecordsRequestPayload
{
    public string TopicName { get; init; }
    public int Partition { get; init; }
    public long BeforeOffset { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static DeleteRecordsRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new DeleteRecordsRequestPayload
        {
            TopicName = reader.ReadString() ?? string.Empty,
            Partition = reader.ReadInt32(),
            BeforeOffset = reader.ReadInt64()
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(TopicName);
        writer.WriteInt32(Partition);
        writer.WriteInt64(BeforeOffset);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(TopicName);
        writer.WriteInt32(Partition);
        writer.WriteInt64(BeforeOffset);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(TopicName ?? "") + // TopicName (length prefix + bytes)
        4 +                                                             // Partition
        8;                                                              // BeforeOffset
}
