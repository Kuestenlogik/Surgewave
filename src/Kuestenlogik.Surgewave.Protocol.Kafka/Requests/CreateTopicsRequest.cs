namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka CreateTopics request (API Key 19)
/// Used by admin clients and Kafka Connect to create topics.
/// </summary>
public sealed class CreateTopicsRequest : KafkaRequest
{
    public required List<TopicToCreate> Topics { get; init; }
    public required int TimeoutMs { get; init; }
    public bool ValidateOnly { get; init; } // v1+

    public sealed class TopicToCreate
    {
        public required string Name { get; init; }
        public required int NumPartitions { get; init; }
        public required short ReplicationFactor { get; init; }
        public List<ReplicaAssignment>? Assignments { get; init; }
        public Dictionary<string, string>? Configs { get; init; }
    }

    public sealed class ReplicaAssignment
    {
        public required int Partition { get; init; }
        public required List<int> BrokerIds { get; init; }
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
            writer.WriteInt32(topic.NumPartitions);
            writer.WriteInt16(topic.ReplicationFactor);

            // Assignments
            if (topic.Assignments == null)
            {
                writer.WriteInt32(0);
            }
            else
            {
                writer.WriteInt32(topic.Assignments.Count);
                foreach (var assignment in topic.Assignments)
                {
                    writer.WriteInt32(assignment.Partition);
                    writer.WriteInt32(assignment.BrokerIds.Count);
                    foreach (var brokerId in assignment.BrokerIds)
                    {
                        writer.WriteInt32(brokerId);
                    }
                }
            }

            // Configs
            if (topic.Configs == null)
            {
                writer.WriteInt32(0);
            }
            else
            {
                writer.WriteInt32(topic.Configs.Count);
                foreach (var (key, value) in topic.Configs)
                {
                    writer.WriteString(key);
                    writer.WriteString(value);
                }
            }
        }

        writer.WriteInt32(TimeoutMs);

        if (ApiVersion >= 1)
        {
            writer.WriteInt8(ValidateOnly ? (sbyte)1 : (sbyte)0);
        }
    }

    public static CreateTopicsRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 5;
        var topics = new List<TopicToCreate>();

        int topicCount;
        if (isFlexible)
        {
            var stream = (MemoryStream)reader.BaseStream;
            var remainingBytes = new byte[stream.Length - stream.Position];
            stream.ReadExactly(remainingBytes, 0, remainingBytes.Length);
            var protocolReader = new KafkaProtocolReader(remainingBytes);

            topicCount = protocolReader.ReadVarInt() - 1;
            for (int i = 0; i < topicCount; i++)
            {
                var name = protocolReader.ReadCompactString()!;
                var numPartitions = protocolReader.ReadInt32();
                var replicationFactor = protocolReader.ReadInt16();

                // Assignments (COMPACT_ARRAY)
                var assignmentCount = protocolReader.ReadVarInt() - 1;
                List<ReplicaAssignment>? assignments = null;
                if (assignmentCount > 0)
                {
                    assignments = new List<ReplicaAssignment>();
                    for (int j = 0; j < assignmentCount; j++)
                    {
                        var partition = protocolReader.ReadInt32();
                        var brokerCount = protocolReader.ReadVarInt() - 1;
                        var brokerIds = new List<int>();
                        for (int k = 0; k < brokerCount; k++)
                        {
                            brokerIds.Add(protocolReader.ReadInt32());
                        }
                        protocolReader.ReadVarInt(); // tagged fields
                        assignments.Add(new ReplicaAssignment { Partition = partition, BrokerIds = brokerIds });
                    }
                }

                // Configs (COMPACT_ARRAY)
                var configCount = protocolReader.ReadVarInt() - 1;
                Dictionary<string, string>? configs = null;
                if (configCount > 0)
                {
                    configs = new Dictionary<string, string>();
                    for (int j = 0; j < configCount; j++)
                    {
                        var configName = protocolReader.ReadCompactString()!;
                        var configValue = protocolReader.ReadCompactString();
                        configs[configName] = configValue ?? "";
                        protocolReader.ReadVarInt(); // tagged fields
                    }
                }

                protocolReader.ReadVarInt(); // topic tagged fields

                topics.Add(new TopicToCreate
                {
                    Name = name,
                    NumPartitions = numPartitions,
                    ReplicationFactor = replicationFactor,
                    Assignments = assignments,
                    Configs = configs
                });
            }

            var timeoutMs = protocolReader.ReadInt32();
            var validateOnly = apiVersion >= 1 && protocolReader.ReadInt8() != 0;
            protocolReader.ReadVarInt(); // request tagged fields

            return new CreateTopicsRequest
            {
                ApiKey = ApiKey.CreateTopics,
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
            // Non-flexible format (v0-v4)
            topicCount = BinaryHelpers.ReadInt32BigEndian(reader);
            for (int i = 0; i < topicCount; i++)
            {
                var name = BinaryHelpers.ReadString(reader);
                var numPartitions = BinaryHelpers.ReadInt32BigEndian(reader);
                var replicationFactor = BinaryHelpers.ReadInt16BigEndian(reader);

                // Assignments
                var assignmentCount = BinaryHelpers.ReadInt32BigEndian(reader);
                List<ReplicaAssignment>? assignments = null;
                if (assignmentCount > 0)
                {
                    assignments = new List<ReplicaAssignment>();
                    for (int j = 0; j < assignmentCount; j++)
                    {
                        var partition = BinaryHelpers.ReadInt32BigEndian(reader);
                        var brokerCount = BinaryHelpers.ReadInt32BigEndian(reader);
                        var brokerIds = new List<int>();
                        for (int k = 0; k < brokerCount; k++)
                        {
                            brokerIds.Add(BinaryHelpers.ReadInt32BigEndian(reader));
                        }
                        assignments.Add(new ReplicaAssignment { Partition = partition, BrokerIds = brokerIds });
                    }
                }

                // Configs
                var configCount = BinaryHelpers.ReadInt32BigEndian(reader);
                Dictionary<string, string>? configs = null;
                if (configCount > 0)
                {
                    configs = new Dictionary<string, string>();
                    for (int j = 0; j < configCount; j++)
                    {
                        var configName = BinaryHelpers.ReadString(reader);
                        var configValue = BinaryHelpers.ReadString(reader);
                        configs[configName] = configValue ?? "";
                    }
                }

                topics.Add(new TopicToCreate
                {
                    Name = name,
                    NumPartitions = numPartitions,
                    ReplicationFactor = replicationFactor,
                    Assignments = assignments,
                    Configs = configs
                });
            }

            var timeoutMs = BinaryHelpers.ReadInt32BigEndian(reader);
            var validateOnly = apiVersion >= 1 && reader.ReadByte() != 0;

            return new CreateTopicsRequest
            {
                ApiKey = ApiKey.CreateTopics,
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
/// Kafka CreateTopics response
/// </summary>
public sealed class CreateTopicsResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; } // v2+
    public required List<TopicResult> Topics { get; init; }

    public sealed class TopicResult
    {
        public required string Name { get; init; }
        public required ErrorCode ErrorCode { get; init; }
        public string? ErrorMessage { get; init; } // v1+
        public int? NumPartitions { get; init; } // v5+
        public short? ReplicationFactor { get; init; } // v5+
        public Dictionary<string, string>? Configs { get; init; } // v5+
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 5;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // response header tagged fields
        }

        // ThrottleTimeMs (v2+)
        if (ApiVersion >= 2)
        {
            writer.WriteInt32(ThrottleTimeMs);
        }

        if (isFlexible)
        {
            writer.WriteVarInt(Topics.Count + 1); // COMPACT_ARRAY
            foreach (var topic in Topics)
            {
                writer.WriteCompactString(topic.Name);
                // TopicId (v7+, 16 bytes UUID)
                if (ApiVersion >= 7)
                {
                    writer.WriteRawBytes(Guid.Empty.ToByteArray());
                }
                writer.WriteInt16((short)topic.ErrorCode);
                writer.WriteCompactString(topic.ErrorMessage);

                // NumPartitions (v5+)
                writer.WriteInt32(topic.NumPartitions ?? -1);

                // ReplicationFactor (v5+)
                writer.WriteInt16(topic.ReplicationFactor ?? -1);

                // Configs (v5+, COMPACT_ARRAY)
                if (topic.Configs != null && topic.Configs.Count > 0)
                {
                    writer.WriteVarInt(topic.Configs.Count + 1);
                    foreach (var (name, value) in topic.Configs)
                    {
                        writer.WriteCompactString(name);
                        writer.WriteCompactString(value);
                        writer.WriteInt8(0); // ConfigSource: DEFAULT_CONFIG
                        writer.WriteInt8(0); // IsSensitive: false
                        writer.WriteInt8(0); // ReadOnly: false
                        writer.WriteVarInt(1); // Synonyms: empty array
                        writer.WriteVarInt(0); // tagged fields
                    }
                }
                else
                {
                    writer.WriteVarInt(1); // empty COMPACT_ARRAY
                }

                writer.WriteVarInt(0); // topic tagged fields
            }
        }
        else
        {
            writer.WriteInt32(Topics.Count);
            foreach (var topic in Topics)
            {
                writer.WriteString(topic.Name);
                writer.WriteInt16((short)topic.ErrorCode);

                // ErrorMessage (v1+)
                if (ApiVersion >= 1)
                {
                    writer.WriteString(topic.ErrorMessage);
                }
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // response tagged fields
        }
    }
}
