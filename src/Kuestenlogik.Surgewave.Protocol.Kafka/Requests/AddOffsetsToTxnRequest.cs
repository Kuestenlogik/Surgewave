namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// AddOffsetsToTxn request - Adds consumer group offsets to an ongoing transaction
/// This is essential for exactly-once semantics when consuming and producing in a transaction.
/// </summary>
public sealed class AddOffsetsToTxnRequest : KafkaRequest
{
    public required string TransactionalId { get; init; }
    public required long ProducerId { get; init; }
    public required short ProducerEpoch { get; init; }
    public required string GroupId { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.AddOffsetsToTxn, ApiVersion);

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

        if (isFlexible)
        {
            writer.WriteCompactString(GroupId);
            writer.WriteVarInt(0); // Tagged fields
        }
        else
        {
            writer.WriteString(GroupId);
        }
    }

    public static AddOffsetsToTxnRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.AddOffsetsToTxn, apiVersion);
        string transactionalId;
        long producerId;
        short producerEpoch;
        string groupId;

        if (isFlexible)
        {
            var stream = (MemoryStream)reader.BaseStream;
            var remainingBytes = new byte[stream.Length - stream.Position];
            stream.ReadExactly(remainingBytes, 0, remainingBytes.Length);
            var protocolReader = new KafkaProtocolReader(remainingBytes);

            transactionalId = protocolReader.ReadCompactString() ?? string.Empty;
            producerId = protocolReader.ReadInt64();
            producerEpoch = protocolReader.ReadInt16();
            groupId = protocolReader.ReadCompactString() ?? string.Empty;

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
            var groupLength = BinaryHelpers.ReadInt16BigEndian(reader);
            groupId = groupLength < 0 ? string.Empty : System.Text.Encoding.UTF8.GetString(reader.ReadBytes(groupLength));
        }

        return new AddOffsetsToTxnRequest
        {
            ApiKey = ApiKey.AddOffsetsToTxn,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            TransactionalId = transactionalId,
            ProducerId = producerId,
            ProducerEpoch = producerEpoch,
            GroupId = groupId
        };
    }
}

/// <summary>
/// AddOffsetsToTxn response
/// </summary>
public sealed class AddOffsetsToTxnResponse : KafkaResponse
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
