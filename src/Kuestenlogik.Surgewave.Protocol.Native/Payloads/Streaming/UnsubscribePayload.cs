namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Streaming;

/// <summary>
/// Wire format for Unsubscribe request.
/// Sent by clients to terminate an active push subscription.
/// </summary>
public readonly record struct UnsubscribePayload
{
    /// <summary>
    /// Subscription identifier to cancel.
    /// </summary>
    public string SubscriptionId { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static UnsubscribePayload Read(ref SurgewavePayloadReader reader)
    {
        return new UnsubscribePayload
        {
            SubscriptionId = reader.ReadString() ?? ""
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(SubscriptionId);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(SubscriptionId);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(SubscriptionId ?? "");
}
