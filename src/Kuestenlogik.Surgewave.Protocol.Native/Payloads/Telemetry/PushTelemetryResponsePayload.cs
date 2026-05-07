namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Telemetry;

/// <summary>
/// Wire format for PushTelemetry response (KIP-714).
/// Acknowledgement from the broker after receiving pushed metrics.
///
/// Wire layout:
///   ThrottleTimeMs  int32
///   ErrorCode       int16
/// </summary>
public readonly record struct PushTelemetryResponsePayload
{
    public int ThrottleTimeMs { get; init; }
    public short ErrorCode { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static PushTelemetryResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        return new PushTelemetryResponsePayload
        {
            ThrottleTimeMs = reader.ReadInt32(),
            ErrorCode = reader.ReadInt16()
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16(ErrorCode);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16(ErrorCode);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        4 + // ThrottleTimeMs
        2;  // ErrorCode
}
