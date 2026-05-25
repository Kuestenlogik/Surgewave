namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// EndTxn request - Commits or aborts an ongoing transaction
/// </summary>
public sealed class EndTxnRequest : KafkaRequest
{
    public required string TransactionalId { get; init; }
    public required long ProducerId { get; init; }
    public required short ProducerEpoch { get; init; }
    public required bool Committed { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.EndTxn, ApiVersion);

        if (isFlexible)
        {
            writer.WriteCompactString(TransactionalId);
        }
        else
        {
            writer.WriteString(TransactionalId);
        }

        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);
        writer.WriteBoolean(Committed);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Tagged fields
        }
    }

    public static EndTxnRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.EndTxn, apiVersion);
        string transactionalId;
        long producerId;
        short producerEpoch;
        bool committed;

        if (isFlexible)
        {
            var stream = (MemoryStream)reader.BaseStream;
            var remainingBytes = new byte[stream.Length - stream.Position];
            stream.ReadExactly(remainingBytes, 0, remainingBytes.Length);
            var protocolReader = new KafkaProtocolReader(remainingBytes);

            transactionalId = protocolReader.ReadCompactString() ?? string.Empty;
            producerId = protocolReader.ReadInt64();
            producerEpoch = protocolReader.ReadInt16();
            committed = protocolReader.ReadBoolean();

            // Skip tagged fields
            var tagCount = protocolReader.ReadVarInt();
            for (int t = 0; t < tagCount; t++)
            {
                protocolReader.ReadVarInt();
                var size = protocolReader.ReadVarInt();
                protocolReader.Skip(size);
            }
        }
        else
        {
            var length = BinaryHelpers.ReadInt16BigEndian(reader);
            transactionalId = length < 0 ? string.Empty : System.Text.Encoding.UTF8.GetString(reader.ReadBytes(length));
            producerId = BinaryHelpers.ReadInt64BigEndian(reader);
            producerEpoch = BinaryHelpers.ReadInt16BigEndian(reader);
            committed = reader.ReadByte() != 0;
        }

        return new EndTxnRequest
        {
            ApiKey = ApiKey.EndTxn,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            TransactionalId = transactionalId,
            ProducerId = producerId,
            ProducerEpoch = producerEpoch,
            Committed = committed
        };
    }
}

/// <summary>
/// EndTxn response
/// </summary>
public sealed class EndTxnResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; }
    public required ErrorCode ErrorCode { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 3;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Tagged fields
        }
    }
}
