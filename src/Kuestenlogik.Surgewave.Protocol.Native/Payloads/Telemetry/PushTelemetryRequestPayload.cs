namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Telemetry;

/// <summary>
/// Wire format for PushTelemetry request (KIP-714).
/// Sent by clients to push collected metrics to the broker.
///
/// Wire layout:
///   ClientInstanceId  Guid (16 bytes, big-endian UUID)
///   SubscriptionId    int32
///   Terminating       bool (1 byte)
///   CompressionType   byte (1 byte)
///   Metrics           byte[] (int32 length prefix + raw bytes)
/// </summary>
public readonly record struct PushTelemetryRequestPayload
{
    public Guid ClientInstanceId { get; init; }
    public int SubscriptionId { get; init; }
    public bool Terminating { get; init; }
    public byte CompressionType { get; init; }
    public byte[] Metrics { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static PushTelemetryRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var clientInstanceId = GuidHelper.ReadGuid(ref reader);
        var subscriptionId = reader.ReadInt32();
        var terminating = reader.ReadBoolean();
        var compressionType = reader.ReadUInt8();

        var metricsLength = reader.ReadInt32();
        var metrics = metricsLength > 0
            ? reader.ReadRaw(metricsLength).ToArray()
            : [];

        return new PushTelemetryRequestPayload
        {
            ClientInstanceId = clientInstanceId,
            SubscriptionId = subscriptionId,
            Terminating = terminating,
            CompressionType = compressionType,
            Metrics = metrics
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        GuidHelper.WriteGuid(ref writer, ClientInstanceId);
        writer.WriteInt32(SubscriptionId);
        writer.WriteBoolean(Terminating);
        writer.WriteUInt8(CompressionType);

        var metrics = Metrics ?? [];
        writer.WriteInt32(metrics.Length);
        writer.WriteRaw(metrics);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        GuidHelper.WriteGuid(writer, ClientInstanceId);
        writer.WriteInt32(SubscriptionId);
        writer.WriteBoolean(Terminating);
        writer.WriteUInt8(CompressionType);

        var metrics = Metrics ?? [];
        writer.WriteInt32(metrics.Length);
        writer.WriteBytes(metrics);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var metrics = Metrics ?? [];
        return 16 + // ClientInstanceId
               4 +  // SubscriptionId
               1 +  // Terminating
               1 +  // CompressionType
               4 + metrics.Length; // Metrics
    }
}
