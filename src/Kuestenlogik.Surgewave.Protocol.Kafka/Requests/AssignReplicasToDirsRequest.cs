namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka AssignReplicasToDirs request (API Key 73, v0-0).
/// JBOD support - assign replicas to log directories.
/// </summary>
public sealed class AssignReplicasToDirsRequest : KafkaRequest
{
    /// <summary>The ID of the requesting broker.</summary>
    public int BrokerId { get; init; }

    /// <summary>The broker epoch.</summary>
    public long BrokerEpoch { get; init; } = -1;

    /// <summary>The directories to assign replicas to.</summary>
    public required List<DirectoryData> Directories { get; init; }

    public sealed class DirectoryData
    {
        /// <summary>The ID of the directory.</summary>
        public Guid Id { get; init; }

        /// <summary>The topics to assign to the directory.</summary>
        public required List<TopicData> Topics { get; init; }
    }

    public sealed class TopicData
    {
        /// <summary>The ID of the topic.</summary>
        public Guid TopicId { get; init; }

        /// <summary>The partitions to assign to the directory.</summary>
        public required List<PartitionData> Partitions { get; init; }
    }

    public sealed class PartitionData
    {
        /// <summary>The partition index.</summary>
        public int PartitionIndex { get; init; }
    }

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

        writer.WriteVarInt(Directories.Count + 1);
        foreach (var dir in Directories)
        {
            writer.WriteUuid(dir.Id);

            writer.WriteVarInt(dir.Topics.Count + 1);
            foreach (var topic in dir.Topics)
            {
                writer.WriteUuid(topic.TopicId);

                writer.WriteVarInt(topic.Partitions.Count + 1);
                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt32(partition.PartitionIndex);
                    writer.WriteVarInt(0); // Partition tagged fields
                }

                writer.WriteVarInt(0); // Topic tagged fields
            }

            writer.WriteVarInt(0); // Directory tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static AssignReplicasToDirsRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var brokerId = reader.ReadInt32();
        var brokerEpoch = reader.ReadInt64();

        var dirCount = reader.ReadVarInt() - 1;
        var directories = new List<DirectoryData>(dirCount);

        for (int i = 0; i < dirCount; i++)
        {
            var id = reader.ReadUuid();

            var topicCount = reader.ReadVarInt() - 1;
            var topics = new List<TopicData>(topicCount);

            for (int j = 0; j < topicCount; j++)
            {
                var topicId = reader.ReadUuid();

                var partitionCount = reader.ReadVarInt() - 1;
                var partitions = new List<PartitionData>(partitionCount);

                for (int k = 0; k < partitionCount; k++)
                {
                    var partitionIndex = reader.ReadInt32();
                    reader.SkipTaggedFields();

                    partitions.Add(new PartitionData
                    {
                        PartitionIndex = partitionIndex
                    });
                }

                reader.SkipTaggedFields();

                topics.Add(new TopicData
                {
                    TopicId = topicId,
                    Partitions = partitions
                });
            }

            reader.SkipTaggedFields();

            directories.Add(new DirectoryData
            {
                Id = id,
                Topics = topics
            });
        }

        reader.SkipTaggedFields();

        return new AssignReplicasToDirsRequest
        {
            ApiKey = ApiKey.AssignReplicasToDirs,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            BrokerId = brokerId,
            BrokerEpoch = brokerEpoch,
            Directories = directories
        };
    }
}

/// <summary>
/// Kafka AssignReplicasToDirs response (API Key 73, v0-0).
/// </summary>
public sealed class AssignReplicasToDirsResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The top level response error code.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The directories.</summary>
    public required List<DirectoryData> Directories { get; init; }

    public sealed class DirectoryData
    {
        /// <summary>The ID of the directory.</summary>
        public Guid Id { get; init; }

        /// <summary>The topics.</summary>
        public required List<TopicData> Topics { get; init; }
    }

    public sealed class TopicData
    {
        /// <summary>The ID of the topic.</summary>
        public Guid TopicId { get; init; }

        /// <summary>The partitions.</summary>
        public required List<PartitionData> Partitions { get; init; }
    }

    public sealed class PartitionData
    {
        /// <summary>The partition index.</summary>
        public int PartitionIndex { get; init; }

        /// <summary>The partition level error code.</summary>
        public ErrorCode ErrorCode { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);

        writer.WriteVarInt(Directories.Count + 1);
        foreach (var dir in Directories)
        {
            writer.WriteUuid(dir.Id);

            writer.WriteVarInt(dir.Topics.Count + 1);
            foreach (var topic in dir.Topics)
            {
                writer.WriteUuid(topic.TopicId);

                writer.WriteVarInt(topic.Partitions.Count + 1);
                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt32(partition.PartitionIndex);
                    writer.WriteInt16((short)partition.ErrorCode);
                    writer.WriteVarInt(0); // Partition tagged fields
                }

                writer.WriteVarInt(0); // Topic tagged fields
            }

            writer.WriteVarInt(0); // Directory tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static AssignReplicasToDirsResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();

        var dirCount = reader.ReadVarInt() - 1;
        var directories = new List<DirectoryData>(dirCount);

        for (int i = 0; i < dirCount; i++)
        {
            var id = reader.ReadUuid();

            var topicCount = reader.ReadVarInt() - 1;
            var topics = new List<TopicData>(topicCount);

            for (int j = 0; j < topicCount; j++)
            {
                var topicId = reader.ReadUuid();

                var partitionCount = reader.ReadVarInt() - 1;
                var partitions = new List<PartitionData>(partitionCount);

                for (int k = 0; k < partitionCount; k++)
                {
                    var partitionIndex = reader.ReadInt32();
                    var partErrorCode = (ErrorCode)reader.ReadInt16();
                    reader.SkipTaggedFields();

                    partitions.Add(new PartitionData
                    {
                        PartitionIndex = partitionIndex,
                        ErrorCode = partErrorCode
                    });
                }

                reader.SkipTaggedFields();

                topics.Add(new TopicData
                {
                    TopicId = topicId,
                    Partitions = partitions
                });
            }

            reader.SkipTaggedFields();

            directories.Add(new DirectoryData
            {
                Id = id,
                Topics = topics
            });
        }

        reader.SkipTaggedFields();

        return new AssignReplicasToDirsResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            Directories = directories
        };
    }
}
