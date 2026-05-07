namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka GetTelemetrySubscriptions request (API Key 71, v0-0).
/// KIP-714: Client telemetry - get subscriptions.
/// </summary>
public sealed class GetTelemetrySubscriptionsRequest : KafkaRequest
{
    /// <summary>
    /// Unique ID for this client instance, used to match successive requests.
    /// On the first request, the client generates a new random UUID.
    /// </summary>
    public required Guid ClientInstanceId { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteUuid(ClientInstanceId);

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static GetTelemetrySubscriptionsRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var clientInstanceId = reader.ReadUuid();
        reader.SkipTaggedFields();

        return new GetTelemetrySubscriptionsRequest
        {
            ApiKey = ApiKey.GetTelemetrySubscriptions,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            ClientInstanceId = clientInstanceId
        };
    }
}

/// <summary>
/// Kafka GetTelemetrySubscriptions response (API Key 71, v0-0).
/// </summary>
public sealed class GetTelemetrySubscriptionsResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>
    /// Assigned client instance ID for this client for stateless clients.
    /// For stateful clients, this is the same as the one sent in the request.
    /// </summary>
    public Guid ClientInstanceId { get; init; }

    /// <summary>
    /// Unique identifier for the current subscription set for this client instance.
    /// </summary>
    public int SubscriptionId { get; init; }

    /// <summary>
    /// Compression types that may be used for this subscription.
    /// </summary>
    public required List<sbyte> AcceptedCompressionTypes { get; init; }

    /// <summary>
    /// Configured push interval, which is the lowest configured interval in the current subscription set.
    /// </summary>
    public int PushIntervalMs { get; init; }

    /// <summary>
    /// The maximum bytes of binary data the broker accepts in a push request.
    /// </summary>
    public int TelemetryMaxBytes { get; init; }

    /// <summary>
    /// Flag to indicate whether to push in a delta or full version for the next push.
    /// </summary>
    public bool DeltaTemporality { get; init; }

    /// <summary>
    /// Requested metrics prefix string match. Empty string means all metrics.
    /// </summary>
    public required List<string> RequestedMetrics { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);
        writer.WriteUuid(ClientInstanceId);
        writer.WriteInt32(SubscriptionId);

        writer.WriteVarInt(AcceptedCompressionTypes.Count + 1);
        foreach (var compression in AcceptedCompressionTypes)
        {
            writer.WriteInt8(compression);
        }

        writer.WriteInt32(PushIntervalMs);
        writer.WriteInt32(TelemetryMaxBytes);
        writer.WriteBoolean(DeltaTemporality);

        writer.WriteVarInt(RequestedMetrics.Count + 1);
        foreach (var metric in RequestedMetrics)
        {
            writer.WriteCompactString(metric);
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static GetTelemetrySubscriptionsResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var clientInstanceId = reader.ReadUuid();
        var subscriptionId = reader.ReadInt32();

        var compressionCount = reader.ReadVarInt() - 1;
        var acceptedCompressionTypes = new List<sbyte>(compressionCount);
        for (int i = 0; i < compressionCount; i++)
        {
            acceptedCompressionTypes.Add(reader.ReadInt8());
        }

        var pushIntervalMs = reader.ReadInt32();
        var telemetryMaxBytes = reader.ReadInt32();
        var deltaTemporality = reader.ReadBoolean();

        var metricCount = reader.ReadVarInt() - 1;
        var requestedMetrics = new List<string>(metricCount);
        for (int i = 0; i < metricCount; i++)
        {
            requestedMetrics.Add(reader.ReadCompactString() ?? "");
        }

        reader.SkipTaggedFields();

        return new GetTelemetrySubscriptionsResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
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
}

/// <summary>
/// Kafka PushTelemetry request (API Key 72, v0-0).
/// KIP-714: Client telemetry - push metrics.
/// </summary>
public sealed class PushTelemetryRequest : KafkaRequest
{
    /// <summary>
    /// Unique ID for this client instance.
    /// </summary>
    public required Guid ClientInstanceId { get; init; }

    /// <summary>
    /// Unique identifier for the current subscription set for this client instance.
    /// </summary>
    public int SubscriptionId { get; init; }

    /// <summary>
    /// Client is terminating the connection.
    /// </summary>
    public bool Terminating { get; init; }

    /// <summary>
    /// Compression codec used to compress the metrics.
    /// 0 = none, 1 = gzip, 2 = snappy, 3 = lz4, 4 = zstd.
    /// </summary>
    public sbyte CompressionType { get; init; }

    /// <summary>
    /// Metrics encoded in OpenTelemetry MetricsData binary format.
    /// </summary>
    public required byte[] Metrics { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteUuid(ClientInstanceId);
        writer.WriteInt32(SubscriptionId);
        writer.WriteBoolean(Terminating);
        writer.WriteInt8(CompressionType);
        writer.WriteCompactBytes(Metrics);

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static PushTelemetryRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var clientInstanceId = reader.ReadUuid();
        var subscriptionId = reader.ReadInt32();
        var terminating = reader.ReadBoolean();
        var compressionType = reader.ReadInt8();
        var metrics = reader.ReadCompactBytes() ?? [];

        reader.SkipTaggedFields();

        return new PushTelemetryRequest
        {
            ApiKey = ApiKey.PushTelemetry,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            ClientInstanceId = clientInstanceId,
            SubscriptionId = subscriptionId,
            Terminating = terminating,
            CompressionType = compressionType,
            Metrics = metrics
        };
    }
}

/// <summary>
/// Kafka PushTelemetry response (API Key 72, v0-0).
/// </summary>
public sealed class PushTelemetryResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static PushTelemetryResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();

        reader.SkipTaggedFields();

        return new PushTelemetryResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode
        };
    }
}
