namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Telemetry;

/// <summary>
/// Wire format for GetTelemetrySubscriptions request (KIP-714).
/// Sent by clients to discover which metrics the broker wants them to push.
///
/// Wire layout:
///   ClientInstanceId  Guid (16 bytes, big-endian UUID)
/// </summary>
public readonly record struct GetTelemetrySubscriptionsRequestPayload
{
    public Guid ClientInstanceId { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static GetTelemetrySubscriptionsRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var clientInstanceId = GuidHelper.ReadGuid(ref reader);

        return new GetTelemetrySubscriptionsRequestPayload
        {
            ClientInstanceId = clientInstanceId
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        GuidHelper.WriteGuid(ref writer, ClientInstanceId);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        GuidHelper.WriteGuid(writer, ClientInstanceId);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() => 16; // Guid
}
