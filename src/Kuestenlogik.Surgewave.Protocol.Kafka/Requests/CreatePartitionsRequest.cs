namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka CreatePartitions request (API Key 37)
/// Used to increase the number of partitions for a topic.
/// Note: Partition count can only be increased, not decreased.
/// </summary>
public sealed class CreatePartitionsRequest : KafkaRequest
{
    public required List<TopicPartitionCount> Topics { get; init; }
    public required int TimeoutMs { get; init; }
    public bool ValidateOnly { get; init; }

    public sealed class TopicPartitionCount
    {
        public required string Name { get; init; }
        public required int Count { get; init; }
        public List<List<int>>? Assignments { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        writer.WriteInt32(Topics.Count);
        foreach (var topic in Topics)
        {
            writer.WriteString(topic.Name);
            writer.WriteInt32(topic.Count);

            if (topic.Assignments == null)
            {
                writer.WriteInt32(0);
            }
            else
            {
                writer.WriteInt32(topic.Assignments.Count);
                foreach (var assignment in topic.Assignments)
                {
                    writer.WriteInt32(assignment.Count);
                    foreach (var brokerId in assignment)
                    {
                        writer.WriteInt32(brokerId);
                    }
                }
            }
        }

        writer.WriteInt32(TimeoutMs);
        writer.WriteInt8(ValidateOnly ? (sbyte)1 : (sbyte)0);
    }

    public static CreatePartitionsRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 2;
        var topics = new List<TopicPartitionCount>();

        if (isFlexible)
        {
            var stream = (MemoryStream)reader.BaseStream;
            var remainingBytes = new byte[stream.Length - stream.Position];
            stream.ReadExactly(remainingBytes, 0, remainingBytes.Length);
            var protocolReader = new KafkaProtocolReader(remainingBytes);

            var topicCount = protocolReader.ReadVarInt() - 1;
            for (int i = 0; i < topicCount; i++)
            {
                var name = protocolReader.ReadCompactString()!;
                var count = protocolReader.ReadInt32();

                // Assignments (COMPACT_ARRAY of COMPACT_ARRAY of INT32)
                var assignmentCount = protocolReader.ReadVarInt() - 1;
                List<List<int>>? assignments = null;
                if (assignmentCount > 0)
                {
                    assignments = new List<List<int>>();
                    for (int j = 0; j < assignmentCount; j++)
                    {
                        var brokerCount = protocolReader.ReadVarInt() - 1;
                        var brokerIds = new List<int>();
                        for (int k = 0; k < brokerCount; k++)
                        {
                            brokerIds.Add(protocolReader.ReadInt32());
                        }
                        protocolReader.ReadVarInt(); // tagged fields
                        assignments.Add(brokerIds);
                    }
                }

                protocolReader.ReadVarInt(); // topic tagged fields

                topics.Add(new TopicPartitionCount
                {
                    Name = name,
                    Count = count,
                    Assignments = assignments
                });
            }

            var timeoutMs = protocolReader.ReadInt32();
            var validateOnly = protocolReader.ReadInt8() != 0;
            protocolReader.ReadVarInt(); // request tagged fields

            return new CreatePartitionsRequest
            {
                ApiKey = ApiKey.CreatePartitions,
                ApiVersion = apiVersion,
                CorrelationId = correlationId,
                ClientId = clientId,
                Topics = topics,
                TimeoutMs = timeoutMs,
                ValidateOnly = validateOnly
            };
        }
        else
        {
            // Non-flexible format (v0-v1)
            var topicCount = BinaryHelpers.ReadInt32BigEndian(reader);
            for (int i = 0; i < topicCount; i++)
            {
                var name = BinaryHelpers.ReadString(reader);
                var count = BinaryHelpers.ReadInt32BigEndian(reader);

                // Assignments
                var assignmentCount = BinaryHelpers.ReadInt32BigEndian(reader);
                List<List<int>>? assignments = null;
                if (assignmentCount > 0)
                {
                    assignments = new List<List<int>>();
                    for (int j = 0; j < assignmentCount; j++)
                    {
                        var brokerCount = BinaryHelpers.ReadInt32BigEndian(reader);
                        var brokerIds = new List<int>();
                        for (int k = 0; k < brokerCount; k++)
                        {
                            brokerIds.Add(BinaryHelpers.ReadInt32BigEndian(reader));
                        }
                        assignments.Add(brokerIds);
                    }
                }

                topics.Add(new TopicPartitionCount
                {
                    Name = name,
                    Count = count,
                    Assignments = assignments
                });
            }

            var timeoutMs = BinaryHelpers.ReadInt32BigEndian(reader);
            var validateOnly = reader.ReadByte() != 0;

            return new CreatePartitionsRequest
            {
                ApiKey = ApiKey.CreatePartitions,
                ApiVersion = apiVersion,
                CorrelationId = correlationId,
                ClientId = clientId,
                Topics = topics,
                TimeoutMs = timeoutMs,
                ValidateOnly = validateOnly
            };
        }
    }
}

/// <summary>
/// Kafka CreatePartitions response
/// </summary>
public sealed class CreatePartitionsResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; }
    public required List<TopicResult> Results { get; init; }

    public sealed class TopicResult
    {
        public required string Name { get; init; }
        public required ErrorCode ErrorCode { get; init; }
        public string? ErrorMessage { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // response header tagged fields
        }

        writer.WriteInt32(ThrottleTimeMs);

        if (isFlexible)
        {
            writer.WriteVarInt(Results.Count + 1); // COMPACT_ARRAY
            foreach (var result in Results)
            {
                writer.WriteCompactString(result.Name);
                writer.WriteInt16((short)result.ErrorCode);
                writer.WriteCompactString(result.ErrorMessage);
                writer.WriteVarInt(0); // topic tagged fields
            }
        }
        else
        {
            writer.WriteInt32(Results.Count);
            foreach (var result in Results)
            {
                writer.WriteString(result.Name);
                writer.WriteInt16((short)result.ErrorCode);
                writer.WriteString(result.ErrorMessage);
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // response tagged fields
        }
    }
}
