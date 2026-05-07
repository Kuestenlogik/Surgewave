namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka OffsetDelete request (API Key 47, v0-0).
/// Deletes offsets for a consumer group's topic-partitions.
/// Used to reset consumer group progress or clean up stale offsets.
/// </summary>
public sealed class OffsetDeleteRequest : KafkaRequest
{
    /// <summary>The group ID to delete offsets for.</summary>
    public required string GroupId { get; init; }

    /// <summary>The topics to delete offsets for.</summary>
    public required List<OffsetDeleteTopic> Topics { get; init; }

    public sealed class OffsetDeleteTopic
    {
        /// <summary>The topic name.</summary>
        public required string Name { get; init; }

        /// <summary>The partitions to delete offsets for.</summary>
        public required List<OffsetDeletePartition> Partitions { get; init; }
    }

    public sealed class OffsetDeletePartition
    {
        /// <summary>The partition index.</summary>
        public required int PartitionIndex { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is non-flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        writer.WriteString(GroupId);

        writer.WriteInt32(Topics.Count);
        foreach (var topic in Topics)
        {
            writer.WriteString(topic.Name);
            writer.WriteInt32(topic.Partitions.Count);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
            }
        }
    }

    public static OffsetDeleteRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var groupId = reader.ReadString() ?? "";

        var topicCount = reader.ReadInt32();
        var topics = new List<OffsetDeleteTopic>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var topicName = reader.ReadString() ?? "";
            var partitionCount = reader.ReadInt32();
            var partitions = new List<OffsetDeletePartition>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                partitions.Add(new OffsetDeletePartition
                {
                    PartitionIndex = reader.ReadInt32()
                });
            }

            topics.Add(new OffsetDeleteTopic
            {
                Name = topicName,
                Partitions = partitions
            });
        }

        return new OffsetDeleteRequest
        {
            ApiKey = ApiKey.OffsetDelete,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            GroupId = groupId,
            Topics = topics
        };
    }
}

/// <summary>
/// Kafka OffsetDelete response (API Key 47, v0-0).
/// </summary>
public sealed class OffsetDeleteResponse : KafkaResponse
{
    /// <summary>The top-level error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The responses for each topic.</summary>
    public required List<OffsetDeleteTopicResponse> Topics { get; init; }

    public sealed class OffsetDeleteTopicResponse
    {
        /// <summary>The topic name.</summary>
        public required string Name { get; init; }

        /// <summary>The responses for each partition.</summary>
        public required List<OffsetDeletePartitionResponse> Partitions { get; init; }
    }

    public sealed class OffsetDeletePartitionResponse
    {
        /// <summary>The partition index.</summary>
        public required int PartitionIndex { get; init; }

        /// <summary>The error code, or 0 if the deletion succeeded.</summary>
        public ErrorCode ErrorCode { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is non-flexible
        writer.WriteInt32(CorrelationId);

        writer.WriteInt16((short)ErrorCode);
        writer.WriteInt32(ThrottleTimeMs);

        writer.WriteInt32(Topics.Count);
        foreach (var topic in Topics)
        {
            writer.WriteString(topic.Name);
            writer.WriteInt32(topic.Partitions.Count);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt16((short)partition.ErrorCode);
            }
        }
    }

    public static OffsetDeleteResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        var errorCode = (ErrorCode)reader.ReadInt16();
        var throttleTimeMs = reader.ReadInt32();

        var topicCount = reader.ReadInt32();
        var topics = new List<OffsetDeleteTopicResponse>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var topicName = reader.ReadString() ?? "";
            var partitionCount = reader.ReadInt32();
            var partitions = new List<OffsetDeletePartitionResponse>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                partitions.Add(new OffsetDeletePartitionResponse
                {
                    PartitionIndex = reader.ReadInt32(),
                    ErrorCode = (ErrorCode)reader.ReadInt16()
                });
            }

            topics.Add(new OffsetDeleteTopicResponse
            {
                Name = topicName,
                Partitions = partitions
            });
        }

        return new OffsetDeleteResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = errorCode,
            ThrottleTimeMs = throttleTimeMs,
            Topics = topics
        };
    }
}
