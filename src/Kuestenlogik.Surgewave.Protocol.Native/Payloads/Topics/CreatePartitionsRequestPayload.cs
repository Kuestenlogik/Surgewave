namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

/// <summary>
/// Wire format for CreatePartitions request.
/// </summary>
public readonly record struct CreatePartitionsRequestPayload
{
    public string TopicName { get; init; }
    public int TotalPartitions { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static CreatePartitionsRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new CreatePartitionsRequestPayload
        {
            TopicName = reader.ReadString() ?? string.Empty,
            TotalPartitions = reader.ReadInt32()
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(TopicName);
        writer.WriteInt32(TotalPartitions);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(TopicName);
        writer.WriteInt32(TotalPartitions);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(TopicName ?? "") + // TopicName (length prefix + bytes)
        4;                                                             // TotalPartitions
}
