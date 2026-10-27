namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// InitProducerId request - Initializes a producer ID for idempotent/transactional producers
/// </summary>
public sealed class InitProducerIdRequest : KafkaRequest
{
    public string? TransactionalId { get; init; }
    public int TransactionTimeoutMs { get; init; }
    public long ProducerId { get; init; } = -1;
    public short ProducerEpoch { get; init; } = -1;

    /// <summary>v6+ (KIP-939): the producer asks for two-phase commit.</summary>
    public bool Enable2Pc { get; init; }

    /// <summary>v6+ (KIP-939): keep a prepared transaction across the init instead of aborting it.</summary>
    public bool KeepPreparedTxn { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        // Request body
        if (ApiVersion >= 2)
        {
            writer.WriteCompactString(TransactionalId);
        }
        else
        {
            writer.WriteString(TransactionalId);
        }

        writer.WriteInt32(TransactionTimeoutMs);

        if (ApiVersion >= 3)
        {
            writer.WriteInt64(ProducerId);
            writer.WriteInt16(ProducerEpoch);
        }

        if (ApiVersion >= 2)
        {
            writer.WriteVarInt(0); // Tagged fields (empty)
        }
    }

    public static InitProducerIdRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool enable2Pc = false, keepPreparedTxn = false;
        string? transactionalId;
        int transactionTimeoutMs;
        long producerId = -1;
        short producerEpoch = -1;

        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.InitProducerId, apiVersion);

        if (isFlexible)
        {
            var stream = (System.IO.MemoryStream)reader.BaseStream;
            var remainingBytes = new byte[stream.Length - stream.Position];
            stream.ReadExactly(remainingBytes, 0, remainingBytes.Length);
            var protocolReader = new KafkaProtocolReader(remainingBytes);

            transactionalId = protocolReader.ReadCompactString();
            transactionTimeoutMs = protocolReader.ReadInt32();

            if (apiVersion >= 3)
            {
                producerId = protocolReader.ReadInt64();
                producerEpoch = protocolReader.ReadInt16();
            }

            // v6+ (KIP-939): Enable2Pc and KeepPreparedTxn. Unread, the first was taken for the tagged-field
            // count and a producer asking for two-phase commit was answered as if it had not.
            if (apiVersion >= 6)
            {
                enable2Pc = protocolReader.ReadBoolean();
                keepPreparedTxn = protocolReader.ReadBoolean();
            }

            // Read tagged fields
            var tagCount = protocolReader.ReadVarInt();
            for (int i = 0; i < tagCount; i++)
            {
                var tag = protocolReader.ReadVarInt();
                var size = protocolReader.ReadVarInt();
                protocolReader.Skip(size);
            }
        }
        else
        {
            var length = BinaryHelpers.ReadInt16BigEndian(reader);
            transactionalId = length < 0 ? null : System.Text.Encoding.UTF8.GetString(reader.ReadBytes(length));
            transactionTimeoutMs = BinaryHelpers.ReadInt32BigEndian(reader);
        }

        return new InitProducerIdRequest
        {
            ApiKey = ApiKey.InitProducerId,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            TransactionalId = transactionalId,
            TransactionTimeoutMs = transactionTimeoutMs,
            ProducerId = producerId,
            ProducerEpoch = producerEpoch,
            Enable2Pc = enable2Pc,
            KeepPreparedTxn = keepPreparedTxn,
        };
    }
}

/// <summary>
/// InitProducerId response - Returns a producer ID and epoch for idempotent/transactional producers
/// </summary>
public sealed class InitProducerIdResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; }
    public required ErrorCode ErrorCode { get; init; }
    public required long ProducerId { get; init; }
    public required short ProducerEpoch { get; init; }

    /// <summary>v6+ (KIP-939): the producer id of an ongoing prepared transaction, -1 when there is none.</summary>
    public long OngoingTxnProducerId { get; init; } = -1;

    /// <summary>v6+ (KIP-939): the epoch of that ongoing prepared transaction, -1 when there is none.</summary>
    public short OngoingTxnProducerEpoch { get; init; } = -1;

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        // Response Header
        writer.WriteInt32(CorrelationId);
        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields (empty)
        }

        // Response Body
        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);
        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);

        if (ApiVersion >= 6)
        {
            // KIP-939 (v6+): the producer id and epoch of a prepared two-phase-commit transaction kept across the
            // init, -1 when there is none. Missing, a v6 response ended ten bytes early.
            writer.WriteInt64(OngoingTxnProducerId);
            writer.WriteInt16(OngoingTxnProducerEpoch);
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response body tagged fields (empty)
        }
    }
}
