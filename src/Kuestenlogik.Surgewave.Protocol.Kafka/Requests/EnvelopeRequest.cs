namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka Envelope request (API Key 58, v0-0).
/// Inter-broker request forwarding - wraps another request for forwarding.
/// </summary>
public sealed class EnvelopeRequest : KafkaRequest
{
    /// <summary>The embedded request header and data.</summary>
    public required byte[] RequestData { get; init; }

    /// <summary>Value of the initial client principal when the request is redirected by a broker.</summary>
    public byte[]? RequestPrincipal { get; init; }

    /// <summary>The original client's address in bytes.</summary>
    public required byte[] ClientHostAddress { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0+ is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteCompactBytes(RequestData);
        writer.WriteCompactBytes(RequestPrincipal);
        writer.WriteCompactBytes(ClientHostAddress);

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static EnvelopeRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var requestData = reader.ReadCompactBytes() ?? [];
        var requestPrincipal = reader.ReadCompactBytes();
        var clientHostAddress = reader.ReadCompactBytes() ?? [];

        reader.SkipTaggedFields();

        return new EnvelopeRequest
        {
            ApiKey = ApiKey.Envelope,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            RequestData = requestData,
            RequestPrincipal = requestPrincipal,
            ClientHostAddress = clientHostAddress
        };
    }
}

/// <summary>
/// Kafka Envelope response (API Key 58, v0-0).
/// </summary>
public sealed class EnvelopeResponse : KafkaResponse
{
    /// <summary>The embedded response header and data.</summary>
    public byte[]? ResponseData { get; init; }

    /// <summary>The error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteCompactBytes(ResponseData);
        writer.WriteInt16((short)ErrorCode);

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static EnvelopeResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var responseData = reader.ReadCompactBytes();
        var errorCode = (ErrorCode)reader.ReadInt16();

        reader.SkipTaggedFields();

        return new EnvelopeResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ResponseData = responseData,
            ErrorCode = errorCode
        };
    }
}
