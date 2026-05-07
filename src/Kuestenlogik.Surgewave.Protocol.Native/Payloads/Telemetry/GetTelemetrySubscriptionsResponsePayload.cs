using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Telemetry;

/// <summary>
/// Wire format for GetTelemetrySubscriptions response (KIP-714).
/// Tells the client which metrics to push, how often, and in what format.
///
/// Wire layout:
///   ThrottleTimeMs            int32
///   ErrorCode                 int16
///   ClientInstanceId          Guid (16 bytes, big-endian UUID)
///   SubscriptionId            int32
///   AcceptedCompressionTypes  byte[] (int32 length prefix + raw bytes)
///   PushIntervalMs            int32
///   TelemetryMaxBytes         int32
///   DeltaTemporality          bool (1 byte)
///   RequestedMetrics          string[] (int32 count + strings)
/// </summary>
public readonly record struct GetTelemetrySubscriptionsResponsePayload
{
    public int ThrottleTimeMs { get; init; }
    public short ErrorCode { get; init; }
    public Guid ClientInstanceId { get; init; }
    public int SubscriptionId { get; init; }
    public byte[] AcceptedCompressionTypes { get; init; }
    public int PushIntervalMs { get; init; }
    public int TelemetryMaxBytes { get; init; }
    public bool DeltaTemporality { get; init; }
    public string[] RequestedMetrics { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static GetTelemetrySubscriptionsResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var throttleTimeMs = reader.ReadInt32();
        var errorCode = reader.ReadInt16();
        var clientInstanceId = GuidHelper.ReadGuid(ref reader);
        var subscriptionId = reader.ReadInt32();

        var compressionLength = reader.ReadInt32();
        var acceptedCompressionTypes = compressionLength > 0
            ? reader.ReadRaw(compressionLength).ToArray()
            : [];

        var pushIntervalMs = reader.ReadInt32();
        var telemetryMaxBytes = reader.ReadInt32();
        var deltaTemporality = reader.ReadBoolean();

        var metricCount = reader.ReadInt32();
        var requestedMetrics = new string[metricCount];
        for (var i = 0; i < metricCount; i++)
            requestedMetrics[i] = reader.ReadString() ?? "";

        return new GetTelemetrySubscriptionsResponsePayload
        {
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ClientInstanceId = clientInstanceId,
            SubscriptionId = subscriptionId,
            AcceptedCompressionTypes = acceptedCompressionTypes,
            PushIntervalMs = pushIntervalMs,
            TelemetryMaxBytes = telemetryMaxBytes,
            DeltaTemporality = deltaTemporality,
            RequestedMetrics = requestedMetrics
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16(ErrorCode);
        GuidHelper.WriteGuid(ref writer, ClientInstanceId);
        writer.WriteInt32(SubscriptionId);

        var compressionTypes = AcceptedCompressionTypes ?? [];
        writer.WriteInt32(compressionTypes.Length);
        writer.WriteRaw(compressionTypes);

        writer.WriteInt32(PushIntervalMs);
        writer.WriteInt32(TelemetryMaxBytes);
        writer.WriteBoolean(DeltaTemporality);

        var metrics = RequestedMetrics ?? [];
        writer.WriteInt32(metrics.Length);
        foreach (var metric in metrics)
            writer.WriteString(metric);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16(ErrorCode);
        GuidHelper.WriteGuid(writer, ClientInstanceId);
        writer.WriteInt32(SubscriptionId);

        var compressionTypes = AcceptedCompressionTypes ?? [];
        writer.WriteInt32(compressionTypes.Length);
        writer.WriteBytes(compressionTypes);

        writer.WriteInt32(PushIntervalMs);
        writer.WriteInt32(TelemetryMaxBytes);
        writer.WriteBoolean(DeltaTemporality);

        var metrics = RequestedMetrics ?? [];
        writer.WriteInt32(metrics.Length);
        foreach (var metric in metrics)
            writer.WriteString(metric);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var compressionTypes = AcceptedCompressionTypes ?? [];
        var metrics = RequestedMetrics ?? [];

        var size =
            4 + // ThrottleTimeMs
            2 + // ErrorCode
            16 + // ClientInstanceId
            4 + // SubscriptionId
            4 + compressionTypes.Length + // AcceptedCompressionTypes
            4 + // PushIntervalMs
            4 + // TelemetryMaxBytes
            1 + // DeltaTemporality
            4;  // metric count

        foreach (var metric in metrics)
            size += 2 + Encoding.UTF8.GetByteCount(metric ?? "");

        return size;
    }
}
