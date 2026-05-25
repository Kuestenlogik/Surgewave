namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka AllocateProducerIds request (API Key 67, v0-0).
/// Inter-broker - allocate a range of producer IDs for a broker.
/// </summary>
public sealed class AllocateProducerIdsRequest : KafkaRequest
{
    /// <summary>The ID of the requesting broker.</summary>
    public int BrokerId { get; init; }

    /// <summary>The epoch of the requesting broker.</summary>
    public long BrokerEpoch { get; init; } = -1;

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0+ is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteInt32(BrokerId);
        writer.WriteInt64(BrokerEpoch);

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static AllocateProducerIdsRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var brokerId = reader.ReadInt32();
        var brokerEpoch = reader.ReadInt64();

        reader.SkipTaggedFields();

        return new AllocateProducerIdsRequest
        {
            ApiKey = ApiKey.AllocateProducerIds,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            BrokerId = brokerId,
            BrokerEpoch = brokerEpoch
        };
    }
}

/// <summary>
/// Kafka AllocateProducerIds response (API Key 67, v0-0).
/// </summary>
public sealed class AllocateProducerIdsResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The top level response error code.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The first producer ID in this range, inclusive.</summary>
    public long ProducerIdStart { get; init; }

    /// <summary>The number of producer IDs in this range.</summary>
    public int ProducerIdLen { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);
        writer.WriteInt64(ProducerIdStart);
        writer.WriteInt32(ProducerIdLen);

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static AllocateProducerIdsResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var producerIdStart = reader.ReadInt64();
        var producerIdLen = reader.ReadInt32();

        reader.SkipTaggedFields();

        return new AllocateProducerIdsResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ProducerIdStart = producerIdStart,
            ProducerIdLen = producerIdLen
        };
    }
}
