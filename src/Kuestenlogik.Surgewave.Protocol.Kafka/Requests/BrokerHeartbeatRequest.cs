namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka BrokerHeartbeat request (API Key 63, v0-1) - KRaft mode only.
/// Sent periodically by brokers to the controller to indicate liveness and report state.
/// </summary>
public sealed class BrokerHeartbeatRequest : KafkaRequest
{
    /// <summary>The broker ID.</summary>
    public required int BrokerId { get; init; }

    /// <summary>
    /// The broker epoch assigned during registration.
    /// Must match the epoch from BrokerRegistrationResponse.
    /// </summary>
    public required long BrokerEpoch { get; init; }

    /// <summary>
    /// The highest metadata offset which the broker has reached.
    /// Used by the controller to determine if the broker is caught up.
    /// </summary>
    public required long CurrentMetadataOffset { get; init; }

    /// <summary>
    /// True if the broker wants to be fenced.
    /// A fenced broker will not be assigned any partition leadership.
    /// </summary>
    public bool WantFence { get; init; }

    /// <summary>
    /// True if the broker wants to initiate controlled shutdown.
    /// The controller will begin migrating partitions away from this broker.
    /// </summary>
    public bool WantShutDown { get; init; }

    /// <summary>
    /// Log directories that are offline (v1+).
    /// The controller uses this to update partition assignments.
    /// </summary>
    public List<Guid> OfflineLogDirs { get; init; } = [];

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // All versions of BrokerHeartbeat are flexible (v0+)
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteInt32(BrokerId);
        writer.WriteInt64(BrokerEpoch);
        writer.WriteInt64(CurrentMetadataOffset);
        writer.WriteBoolean(WantFence);
        writer.WriteBoolean(WantShutDown);

        // v1+: OfflineLogDirs
        if (ApiVersion >= 1)
        {
            writer.WriteVarInt(OfflineLogDirs.Count + 1);
            foreach (var logDir in OfflineLogDirs)
            {
                writer.WriteUuid(logDir);
            }
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static BrokerHeartbeatRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var brokerId = reader.ReadInt32();
        var brokerEpoch = reader.ReadInt64();
        var currentMetadataOffset = reader.ReadInt64();
        var wantFence = reader.ReadBoolean();
        var wantShutDown = reader.ReadBoolean();

        var offlineLogDirs = new List<Guid>();
        if (apiVersion >= 1)
        {
            var count = reader.ReadVarInt() - 1;
            for (int i = 0; i < count; i++)
            {
                offlineLogDirs.Add(reader.ReadUuid());
            }
        }

        reader.SkipTaggedFields();

        return new BrokerHeartbeatRequest
        {
            ApiKey = ApiKey.BrokerHeartbeat,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            BrokerId = brokerId,
            BrokerEpoch = brokerEpoch,
            CurrentMetadataOffset = currentMetadataOffset,
            WantFence = wantFence,
            WantShutDown = wantShutDown,
            OfflineLogDirs = offlineLogDirs
        };
    }
}

/// <summary>
/// Kafka BrokerHeartbeat response (API Key 63, v0-1)
/// </summary>
public sealed class BrokerHeartbeatResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>
    /// True if the broker has caught up with the latest metadata.
    /// The broker should not accept client requests until this is true.
    /// </summary>
    public bool IsCaughtUp { get; init; }

    /// <summary>
    /// True if the broker is currently fenced.
    /// A fenced broker cannot be a partition leader.
    /// </summary>
    public bool IsFenced { get; init; }

    /// <summary>
    /// True if the broker should proceed with shutdown.
    /// Only set after all partitions have been migrated during controlled shutdown.
    /// </summary>
    public bool ShouldShutDown { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // All versions are flexible
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);
        writer.WriteBoolean(IsCaughtUp);
        writer.WriteBoolean(IsFenced);
        writer.WriteBoolean(ShouldShutDown);

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static BrokerHeartbeatResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var isCaughtUp = reader.ReadBoolean();
        var isFenced = reader.ReadBoolean();
        var shouldShutDown = reader.ReadBoolean();

        reader.SkipTaggedFields(); // Body tagged fields

        return new BrokerHeartbeatResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            IsCaughtUp = isCaughtUp,
            IsFenced = isFenced,
            ShouldShutDown = shouldShutDown
        };
    }
}
