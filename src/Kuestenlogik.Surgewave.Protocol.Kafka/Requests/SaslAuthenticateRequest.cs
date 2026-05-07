namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// SASL Authenticate request (API Key 36)
/// Carries the actual SASL authentication data
/// </summary>
public sealed class SaslAuthenticateRequest : KafkaRequest
{
    /// <summary>
    /// The SASL authentication bytes from the client (mechanism-specific)
    /// For PLAIN: NULL + username + NULL + password (UTF-8 encoded)
    /// </summary>
    public required byte[] AuthBytes { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        if (ApiVersion >= 2)
        {
            // Flexible version
            writer.WriteVarInt(AuthBytes.Length + 1);
            writer.WriteBytes(AuthBytes);
            writer.WriteVarInt(0); // tagged fields
        }
        else
        {
            writer.WriteBytes(AuthBytes);
        }
    }

    public static SaslAuthenticateRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        byte[] authBytes;

        if (apiVersion >= 2)
        {
            // Flexible version
            var stream = (MemoryStream)reader.BaseStream;
            var remainingBytes = new byte[stream.Length - stream.Position];
            stream.ReadExactly(remainingBytes, 0, remainingBytes.Length);
            var protocolReader = new KafkaProtocolReader(remainingBytes);

            var length = protocolReader.ReadVarInt() - 1;
            authBytes = protocolReader.ReadBytesFixed(length);
            protocolReader.ReadVarInt(); // tagged fields
        }
        else
        {
            // Read BYTES (length-prefixed)
            var length = BinaryHelpers.ReadInt32BigEndian(reader);
            authBytes = reader.ReadBytes(length);
        }

        return new SaslAuthenticateRequest
        {
            ApiKey = ApiKey.SaslAuthenticate,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            AuthBytes = authBytes
        };
    }
}

/// <summary>
/// SASL Authenticate response
/// </summary>
public sealed class SaslAuthenticateResponse : KafkaResponse
{
    public required ErrorCode ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }

    /// <summary>
    /// The SASL authentication bytes from the server (mechanism-specific)
    /// For PLAIN: typically empty on success
    /// </summary>
    public byte[] AuthBytes { get; init; } = [];

    /// <summary>
    /// Duration in milliseconds for which the session is valid (0 = no limit)
    /// Added in version 1
    /// </summary>
    public long SessionLifetimeMs { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // header tagged fields
        }

        writer.WriteInt16((short)ErrorCode);

        if (isFlexible)
        {
            writer.WriteCompactString(ErrorMessage);
            writer.WriteVarInt(AuthBytes.Length + 1);
            writer.WriteBytes(AuthBytes);
        }
        else
        {
            writer.WriteString(ErrorMessage);
            writer.WriteInt32(AuthBytes.Length);
            writer.WriteBytes(AuthBytes);
        }

        // Session lifetime (v1+)
        if (ApiVersion >= 1)
        {
            writer.WriteInt64(SessionLifetimeMs);
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // body tagged fields
        }
    }

    public static SaslAuthenticateResponse CreateSuccess(int correlationId, short apiVersion, byte[]? authBytes = null, long sessionLifetimeMs = 0)
    {
        return new SaslAuthenticateResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            AuthBytes = authBytes ?? [],
            SessionLifetimeMs = sessionLifetimeMs
        };
    }

    /// <summary>
    /// Create a challenge response for multi-step SASL mechanisms (SCRAM)
    /// </summary>
    public static SaslAuthenticateResponse CreateChallenge(int correlationId, short apiVersion, byte[] authBytes)
    {
        return new SaslAuthenticateResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            AuthBytes = authBytes,
            SessionLifetimeMs = 0
        };
    }

    public static SaslAuthenticateResponse CreateError(int correlationId, short apiVersion, ErrorCode errorCode, string? errorMessage)
    {
        return new SaslAuthenticateResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            AuthBytes = [],
            SessionLifetimeMs = 0
        };
    }
}
