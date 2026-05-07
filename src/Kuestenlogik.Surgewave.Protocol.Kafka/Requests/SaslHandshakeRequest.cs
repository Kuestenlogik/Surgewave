namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// SASL Handshake request (API Key 17)
/// Used by clients to negotiate SASL mechanism before authentication
/// </summary>
public sealed class SaslHandshakeRequest : KafkaRequest
{
    /// <summary>
    /// The SASL mechanism chosen by the client (e.g., "PLAIN", "SCRAM-SHA-256")
    /// </summary>
    public required string Mechanism { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteString(Mechanism);
    }

    public static SaslHandshakeRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        string mechanism;

        if (apiVersion >= 1)
        {
            // Flexible version - but SaslHandshake doesn't use flexible format
            mechanism = BinaryHelpers.ReadString(reader);
        }
        else
        {
            mechanism = BinaryHelpers.ReadString(reader);
        }

        return new SaslHandshakeRequest
        {
            ApiKey = ApiKey.SaslHandshake,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Mechanism = mechanism
        };
    }
}

/// <summary>
/// SASL Handshake response
/// Returns supported SASL mechanisms
/// </summary>
public sealed class SaslHandshakeResponse : KafkaResponse
{
    public required ErrorCode ErrorCode { get; init; }
    public required string[] EnabledMechanisms { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);

        writer.WriteInt16((short)ErrorCode);

        // Array of mechanism strings
        writer.WriteInt32(EnabledMechanisms.Length);
        foreach (var mechanism in EnabledMechanisms)
        {
            writer.WriteString(mechanism);
        }
    }

    public static SaslHandshakeResponse CreateSuccess(int correlationId, short apiVersion, string[] enabledMechanisms)
    {
        return new SaslHandshakeResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = ErrorCode.None,
            EnabledMechanisms = enabledMechanisms
        };
    }

    public static SaslHandshakeResponse CreateError(int correlationId, short apiVersion, ErrorCode errorCode, string[] enabledMechanisms)
    {
        return new SaslHandshakeResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = errorCode,
            EnabledMechanisms = enabledMechanisms
        };
    }
}
