namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Streaming;

/// <summary>
/// Wire format for StreamAck request.
/// Sent periodically by the client to acknowledge received bytes and
/// allow the broker to advance its flow-control window.
/// </summary>
public readonly record struct StreamAckPayload
{
    /// <summary>
    /// Subscription identifier this acknowledgment applies to.
    /// </summary>
    public string SubscriptionId { get; init; }

    /// <summary>
    /// Total number of payload bytes acknowledged by the client.
    /// </summary>
    public long AcknowledgedBytes { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static StreamAckPayload Read(ref SurgewavePayloadReader reader)
    {
        return new StreamAckPayload
        {
            SubscriptionId = reader.ReadString() ?? "",
            AcknowledgedBytes = reader.ReadInt64()
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(SubscriptionId);
        writer.WriteInt64(AcknowledgedBytes);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(SubscriptionId);
        writer.WriteInt64(AcknowledgedBytes);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(SubscriptionId ?? "") +
        8; // AcknowledgedBytes
}
