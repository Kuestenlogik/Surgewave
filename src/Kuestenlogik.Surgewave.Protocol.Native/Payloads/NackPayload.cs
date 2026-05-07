namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads;

/// <summary>
/// Wire format for Nack request.
/// Sent by clients to indicate a message could not be processed,
/// triggering broker-level retry or DLQ routing.
/// </summary>
public readonly record struct NackRequestPayload
{
    /// <summary>
    /// Topic of the message being nacked.
    /// </summary>
    public string Topic { get; init; }

    /// <summary>
    /// Partition of the message being nacked.
    /// </summary>
    public int Partition { get; init; }

    /// <summary>
    /// Offset of the message being nacked.
    /// </summary>
    public long Offset { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static NackRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new NackRequestPayload
        {
            Topic = reader.ReadString() ?? "",
            Partition = reader.ReadInt32(),
            Offset = reader.ReadInt64()
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(Partition);
        writer.WriteInt64(Offset);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(Partition);
        writer.WriteInt64(Offset);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(Topic ?? "") + 4 + 8;
}

/// <summary>
/// Wire format for Nack response.
/// Indicates whether the nack was processed and if the message was routed to DLQ.
/// </summary>
public readonly record struct NackResponsePayload
{
    /// <summary>
    /// Whether the message was routed to DLQ (true) or scheduled for retry (false).
    /// </summary>
    public bool RoutedToDlq { get; init; }

    /// <summary>
    /// Current retry count for this message.
    /// </summary>
    public int RetryCount { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static NackResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        return new NackResponsePayload
        {
            RoutedToDlq = reader.ReadUInt8() != 0,
            RetryCount = reader.ReadInt32()
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt8(RoutedToDlq ? (byte)1 : (byte)0);
        writer.WriteInt32(RetryCount);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt8(RoutedToDlq ? (byte)1 : (byte)0);
        writer.WriteInt32(RetryCount);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() => 1 + 4;
}
