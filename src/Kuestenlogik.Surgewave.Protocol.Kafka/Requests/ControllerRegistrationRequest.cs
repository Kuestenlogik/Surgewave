namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka ControllerRegistration request (API Key 70, v0-0).
/// KIP-919: Controller registration for KRaft controllers.
/// </summary>
public sealed class ControllerRegistrationRequest : KafkaRequest
{
    /// <summary>The ID of the controller to register.</summary>
    public int ControllerId { get; init; }

    /// <summary>The controller incarnation ID, which is unique to each process run.</summary>
    public Guid IncarnationId { get; init; }

    /// <summary>Set if the required configurations for ZK migration are present.</summary>
    public bool ZkMigrationReady { get; init; }

    /// <summary>The listeners of this controller.</summary>
    public required List<ListenerInfo> Listeners { get; init; }

    /// <summary>The features on this controller.</summary>
    public required List<FeatureInfo> Features { get; init; }

    public sealed class ListenerInfo
    {
        /// <summary>The name of the endpoint.</summary>
        public required string Name { get; init; }

        /// <summary>The hostname.</summary>
        public required string Host { get; init; }

        /// <summary>The port.</summary>
        public ushort Port { get; init; }

        /// <summary>The security protocol.</summary>
        public short SecurityProtocol { get; init; }
    }

    public sealed class FeatureInfo
    {
        /// <summary>The feature name.</summary>
        public required string Name { get; init; }

        /// <summary>The minimum supported feature level.</summary>
        public short MinSupportedVersion { get; init; }

        /// <summary>The maximum supported feature level.</summary>
        public short MaxSupportedVersion { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteInt32(ControllerId);
        writer.WriteUuid(IncarnationId);
        writer.WriteBoolean(ZkMigrationReady);

        writer.WriteVarInt(Listeners.Count + 1);
        foreach (var listener in Listeners)
        {
            writer.WriteCompactString(listener.Name);
            writer.WriteCompactString(listener.Host);
            writer.WriteInt16((short)listener.Port);
            writer.WriteInt16(listener.SecurityProtocol);
            writer.WriteVarInt(0); // Listener tagged fields
        }

        writer.WriteVarInt(Features.Count + 1);
        foreach (var feature in Features)
        {
            writer.WriteCompactString(feature.Name);
            writer.WriteInt16(feature.MinSupportedVersion);
            writer.WriteInt16(feature.MaxSupportedVersion);
            writer.WriteVarInt(0); // Feature tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static ControllerRegistrationRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var controllerId = reader.ReadInt32();
        var incarnationId = reader.ReadUuid();
        var zkMigrationReady = reader.ReadBoolean();

        var listenerCount = reader.ReadVarInt() - 1;
        var listeners = new List<ListenerInfo>(listenerCount);
        for (int i = 0; i < listenerCount; i++)
        {
            var name = reader.ReadCompactString() ?? "";
            var host = reader.ReadCompactString() ?? "";
            var port = (ushort)reader.ReadInt16();
            var securityProtocol = reader.ReadInt16();
            reader.SkipTaggedFields();

            listeners.Add(new ListenerInfo
            {
                Name = name,
                Host = host,
                Port = port,
                SecurityProtocol = securityProtocol
            });
        }

        var featureCount = reader.ReadVarInt() - 1;
        var features = new List<FeatureInfo>(featureCount);
        for (int i = 0; i < featureCount; i++)
        {
            var name = reader.ReadCompactString() ?? "";
            var minSupportedVersion = reader.ReadInt16();
            var maxSupportedVersion = reader.ReadInt16();
            reader.SkipTaggedFields();

            features.Add(new FeatureInfo
            {
                Name = name,
                MinSupportedVersion = minSupportedVersion,
                MaxSupportedVersion = maxSupportedVersion
            });
        }

        reader.SkipTaggedFields();

        return new ControllerRegistrationRequest
        {
            ApiKey = ApiKey.ControllerRegistration,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            ControllerId = controllerId,
            IncarnationId = incarnationId,
            ZkMigrationReady = zkMigrationReady,
            Listeners = listeners,
            Features = features
        };
    }
}

/// <summary>
/// Kafka ControllerRegistration response (API Key 70, v0-0).
/// </summary>
public sealed class ControllerRegistrationResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The response error code.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The response error message, or null if there was no error.</summary>
    public string? ErrorMessage { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);
        writer.WriteCompactString(ErrorMessage);

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static ControllerRegistrationResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadCompactString();

        reader.SkipTaggedFields();

        return new ControllerRegistrationResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage
        };
    }
}
