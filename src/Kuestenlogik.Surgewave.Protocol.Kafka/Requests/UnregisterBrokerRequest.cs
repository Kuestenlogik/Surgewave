namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka UnregisterBroker request (API Key 64, v0-0).
/// Unregisters a broker from the cluster (KRaft).
/// </summary>
public sealed class UnregisterBrokerRequest : KafkaRequest
{
    /// <summary>The broker ID to unregister.</summary>
    public required int BrokerId { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteInt32(BrokerId);

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static UnregisterBrokerRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var brokerId = reader.ReadInt32();
        reader.SkipTaggedFields();

        return new UnregisterBrokerRequest
        {
            ApiKey = ApiKey.UnregisterBroker,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            BrokerId = brokerId
        };
    }
}

/// <summary>
/// Kafka UnregisterBroker response (API Key 64, v0-0).
/// </summary>
public sealed class UnregisterBrokerResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The error message, or null if there was no error.</summary>
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

    public static UnregisterBrokerResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadCompactString();

        reader.SkipTaggedFields();

        return new UnregisterBrokerResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage
        };
    }
}
