namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// AddPartitionsToTxn request - Adds partitions to an ongoing transaction
/// </summary>
public sealed class AddPartitionsToTxnRequest : KafkaRequest
{
    public required string TransactionalId { get; init; }
    public required long ProducerId { get; init; }
    public required short ProducerEpoch { get; init; }
    public required Dictionary<string, List<int>> Topics { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.AddPartitionsToTxn, ApiVersion);

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
            writer.WriteVarInt(Topics.Count + 1);
            foreach (var (topic, partitions) in Topics)
            {
                writer.WriteCompactString(topic);
                writer.WriteVarInt(partitions.Count + 1);
                foreach (var partition in partitions)
                {
                    writer.WriteInt32(partition);
                }
                writer.WriteVarInt(0); // Tagged fields
            }
            writer.WriteVarInt(0); // Tagged fields
        }
        else
        {
            writer.WriteInt32(Topics.Count);
            foreach (var (topic, partitions) in Topics)
            {
                writer.WriteString(topic);
                writer.WriteInt32(partitions.Count);
                foreach (var partition in partitions)
                {
                    writer.WriteInt32(partition);
                }
            }
        }
    }

    public static AddPartitionsToTxnRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.AddPartitionsToTxn, apiVersion);
        string transactionalId;
        long producerId;
        short producerEpoch;
        var topics = new Dictionary<string, List<int>>();

        if (isFlexible)
        {
            var stream = (MemoryStream)reader.BaseStream;
            var remainingBytes = new byte[stream.Length - stream.Position];
            stream.ReadExactly(remainingBytes, 0, remainingBytes.Length);
            var protocolReader = new KafkaProtocolReader(remainingBytes);

            transactionalId = protocolReader.ReadCompactString() ?? string.Empty;
            producerId = protocolReader.ReadInt64();
            producerEpoch = protocolReader.ReadInt16();

            var topicCount = protocolReader.ReadVarInt() - 1;
            for (int i = 0; i < topicCount; i++)
            {
                var topic = protocolReader.ReadCompactString() ?? string.Empty;
                var partitionCount = protocolReader.ReadVarInt() - 1;
                var partitions = new List<int>();

                for (int j = 0; j < partitionCount; j++)
                {
                    partitions.Add(protocolReader.ReadInt32());
                }

                topics[topic] = partitions;

                // Skip partition tagged fields
                var tagCount = protocolReader.ReadVarInt();
                for (int t = 0; t < tagCount; t++)
                {
                    protocolReader.ReadVarInt();
                    var size = protocolReader.ReadVarInt();
                    protocolReader.Skip(size);
                }
            }

            // Skip request tagged fields
            var requestTagCount = protocolReader.ReadVarInt();
            for (int t = 0; t < requestTagCount; t++)
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

            var topicCount = BinaryHelpers.ReadInt32BigEndian(reader);
            for (int i = 0; i < topicCount; i++)
            {
                var topicLength = BinaryHelpers.ReadInt16BigEndian(reader);
                var topic = System.Text.Encoding.UTF8.GetString(reader.ReadBytes(topicLength));
                var partitionCount = BinaryHelpers.ReadInt32BigEndian(reader);
                var partitions = new List<int>();

                for (int j = 0; j < partitionCount; j++)
                {
                    partitions.Add(BinaryHelpers.ReadInt32BigEndian(reader));
                }

                topics[topic] = partitions;
            }
        }

        return new AddPartitionsToTxnRequest
        {
            ApiKey = ApiKey.AddPartitionsToTxn,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            TransactionalId = transactionalId,
            ProducerId = producerId,
            ProducerEpoch = producerEpoch,
            Topics = topics
        };
    }
}

/// <summary>
/// AddPartitionsToTxn response
/// </summary>
public sealed class AddPartitionsToTxnResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; }
    public required Dictionary<string, List<PartitionResult>> Results { get; init; }

    public sealed class PartitionResult
    {
        public required int Partition { get; init; }
        public required ErrorCode ErrorCode { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 3;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        writer.WriteInt32(ThrottleTimeMs);

        if (isFlexible)
        {
            writer.WriteVarInt(Results.Count + 1);
            foreach (var (topic, partitions) in Results)
            {
                writer.WriteCompactString(topic);
                writer.WriteVarInt(partitions.Count + 1);
                foreach (var partition in partitions)
                {
                    writer.WriteInt32(partition.Partition);
                    writer.WriteInt16((short)partition.ErrorCode);
                }
                writer.WriteVarInt(0); // Tagged fields
            }
            writer.WriteVarInt(0); // Tagged fields
        }
        else
        {
            writer.WriteInt32(Results.Count);
            foreach (var (topic, partitions) in Results)
            {
                writer.WriteString(topic);
                writer.WriteInt32(partitions.Count);
                foreach (var partition in partitions)
                {
                    writer.WriteInt32(partition.Partition);
                    writer.WriteInt16((short)partition.ErrorCode);
                }
            }
        }
    }
}
