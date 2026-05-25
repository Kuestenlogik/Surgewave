namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka BrokerRegistration request (API Key 62, v0-3) - KRaft mode only.
/// Sent by brokers to the controller to register themselves with the cluster.
/// This is the first request a broker sends after connecting to the controller.
/// </summary>
public sealed class BrokerRegistrationRequest : KafkaRequest
{
    /// <summary>The broker ID.</summary>
    public required int BrokerId { get; init; }

    /// <summary>The cluster ID of the broker process.</summary>
    public required string ClusterId { get; init; }

    /// <summary>
    /// The incarnation ID of the broker process (UUID).
    /// A new UUID is generated each time the broker starts.
    /// </summary>
    public required Guid IncarnationId { get; init; }

    /// <summary>The listeners supported by this broker.</summary>
    public required List<Listener> Listeners { get; init; }

    /// <summary>The features supported by this broker.</summary>
    public required List<Feature> Features { get; init; }

    /// <summary>The rack which this broker is in (nullable).</summary>
    public string? Rack { get; init; }

    /// <summary>If the required configurations for ZK migration are present, this value is set to true (v1+).</summary>
    public bool IsMigratingZkBroker { get; init; }

    /// <summary>Log directories configured in this broker which are available (v2+).</summary>
    public List<Guid> LogDirs { get; init; } = [];

    /// <summary>The epoch before a clean shutdown (v3+).</summary>
    public long PreviousBrokerEpoch { get; init; } = -1;

    public sealed class Listener
    {
        /// <summary>The name of the listener.</summary>
        public required string Name { get; init; }

        /// <summary>The hostname.</summary>
        public required string Host { get; init; }

        /// <summary>The port.</summary>
        public required ushort Port { get; init; }

        /// <summary>The security protocol (0=PLAINTEXT, 1=SSL, 2=SASL_PLAINTEXT, 3=SASL_SSL).</summary>
        public required short SecurityProtocol { get; init; }
    }

    public sealed class Feature
    {
        /// <summary>The feature name.</summary>
        public required string Name { get; init; }

        /// <summary>The minimum supported feature level.</summary>
        public required short MinSupportedVersion { get; init; }

        /// <summary>The maximum supported feature level.</summary>
        public required short MaxSupportedVersion { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // All versions of BrokerRegistration are flexible (v0+)
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteInt32(BrokerId);
        writer.WriteCompactString(ClusterId);
        writer.WriteUuid(IncarnationId);

        // Listeners array (compact)
        writer.WriteVarInt(Listeners.Count + 1);
        foreach (var listener in Listeners)
        {
            writer.WriteCompactString(listener.Name);
            writer.WriteCompactString(listener.Host);
            writer.WriteInt16((short)listener.Port);
            writer.WriteInt16(listener.SecurityProtocol);
            writer.WriteVarInt(0); // Listener tagged fields
        }

        // Features array (compact)
        writer.WriteVarInt(Features.Count + 1);
        foreach (var feature in Features)
        {
            writer.WriteCompactString(feature.Name);
            writer.WriteInt16(feature.MinSupportedVersion);
            writer.WriteInt16(feature.MaxSupportedVersion);
            writer.WriteVarInt(0); // Feature tagged fields
        }

        writer.WriteCompactString(Rack);

        // v1+: IsMigratingZkBroker
        if (ApiVersion >= 1)
        {
            writer.WriteBoolean(IsMigratingZkBroker);
        }

        // v2+: LogDirs
        if (ApiVersion >= 2)
        {
            writer.WriteVarInt(LogDirs.Count + 1);
            foreach (var logDir in LogDirs)
            {
                writer.WriteUuid(logDir);
            }
        }

        // v3+: PreviousBrokerEpoch
        if (ApiVersion >= 3)
        {
            writer.WriteInt64(PreviousBrokerEpoch);
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static BrokerRegistrationRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var brokerId = reader.ReadInt32();
        var clusterId = reader.ReadCompactString() ?? "";
        var incarnationId = reader.ReadUuid();

        // Listeners
        var listenerCount = reader.ReadVarInt() - 1;
        var listeners = new List<Listener>(listenerCount);
        for (int i = 0; i < listenerCount; i++)
        {
            var name = reader.ReadCompactString() ?? "";
            var host = reader.ReadCompactString() ?? "";
            var port = (ushort)reader.ReadInt16();
            var securityProtocol = reader.ReadInt16();
            reader.SkipTaggedFields();
            listeners.Add(new Listener
            {
                Name = name,
                Host = host,
                Port = port,
                SecurityProtocol = securityProtocol
            });
        }

        // Features
        var featureCount = reader.ReadVarInt() - 1;
        var features = new List<Feature>(featureCount);
        for (int i = 0; i < featureCount; i++)
        {
            var name = reader.ReadCompactString() ?? "";
            var minVersion = reader.ReadInt16();
            var maxVersion = reader.ReadInt16();
            reader.SkipTaggedFields();
            features.Add(new Feature
            {
                Name = name,
                MinSupportedVersion = minVersion,
                MaxSupportedVersion = maxVersion
            });
        }

        var rack = reader.ReadCompactString();

        var isMigratingZkBroker = apiVersion >= 1 && reader.ReadBoolean();

        var logDirs = new List<Guid>();
        if (apiVersion >= 2)
        {
            var logDirCount = reader.ReadVarInt() - 1;
            for (int i = 0; i < logDirCount; i++)
            {
                logDirs.Add(reader.ReadUuid());
            }
        }

        var previousBrokerEpoch = apiVersion >= 3 ? reader.ReadInt64() : -1L;

        reader.SkipTaggedFields();

        return new BrokerRegistrationRequest
        {
            ApiKey = ApiKey.BrokerRegistration,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            BrokerId = brokerId,
            ClusterId = clusterId,
            IncarnationId = incarnationId,
            Listeners = listeners,
            Features = features,
            Rack = rack,
            IsMigratingZkBroker = isMigratingZkBroker,
            LogDirs = logDirs,
            PreviousBrokerEpoch = previousBrokerEpoch
        };
    }
}

/// <summary>
/// Kafka BrokerRegistration response (API Key 62, v0-3)
/// </summary>
public sealed class BrokerRegistrationResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>
    /// The broker's assigned epoch, or -1 if none was assigned.
    /// This epoch is used in all subsequent heartbeats.
    /// </summary>
    public long BrokerEpoch { get; init; } = -1;

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // All versions are flexible
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);
        writer.WriteInt64(BrokerEpoch);

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static BrokerRegistrationResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var brokerEpoch = reader.ReadInt64();

        reader.SkipTaggedFields(); // Body tagged fields

        return new BrokerRegistrationResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            BrokerEpoch = brokerEpoch
        };
    }
}
