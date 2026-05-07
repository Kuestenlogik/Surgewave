namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka BeginQuorumEpoch request (API Key 53, v0-1).
/// KRaft quorum protocol - notify voters of a new epoch with a new leader.
/// </summary>
public sealed class BeginQuorumEpochRequest : KafkaRequest
{
    /// <summary>The cluster ID (optional in v0).</summary>
    public string? ClusterId { get; init; }

    /// <summary>The voter topics.</summary>
    public required List<TopicData> Topics { get; init; }

    public sealed class TopicData
    {
        /// <summary>The topic name.</summary>
        public required string TopicName { get; init; }

        /// <summary>The partitions.</summary>
        public required List<PartitionData> Partitions { get; init; }
    }

    public sealed class PartitionData
    {
        /// <summary>The partition index.</summary>
        public int PartitionIndex { get; init; }

        /// <summary>The ID of the newly elected leader.</summary>
        public int LeaderId { get; init; }

        /// <summary>The epoch of the newly elected leader.</summary>
        public int LeaderEpoch { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is non-flexible, v1+ is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);

        if (ApiVersion >= 1)
        {
            writer.WriteCompactString(ClientId);
            writer.WriteVarInt(0); // Header tagged fields
            writer.WriteCompactString(ClusterId);
        }
        else
        {
            writer.WriteString(ClientId);
        }

        if (ApiVersion >= 1)
        {
            writer.WriteVarInt(Topics.Count + 1);
        }
        else
        {
            writer.WriteInt32(Topics.Count);
        }

        foreach (var topic in Topics)
        {
            if (ApiVersion >= 1)
            {
                writer.WriteCompactString(topic.TopicName);
                writer.WriteVarInt(topic.Partitions.Count + 1);
            }
            else
            {
                writer.WriteString(topic.TopicName);
                writer.WriteInt32(topic.Partitions.Count);
            }

            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt32(partition.LeaderId);
                writer.WriteInt32(partition.LeaderEpoch);

                if (ApiVersion >= 1)
                {
                    writer.WriteVarInt(0); // Partition tagged fields
                }
            }

            if (ApiVersion >= 1)
            {
                writer.WriteVarInt(0); // Topic tagged fields
            }
        }

        if (ApiVersion >= 1)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static BeginQuorumEpochRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        string? clusterId = null;

        if (apiVersion >= 1)
        {
            clusterId = reader.ReadCompactString();
        }

        int topicCount;
        if (apiVersion >= 1)
        {
            topicCount = reader.ReadVarInt() - 1;
        }
        else
        {
            topicCount = reader.ReadInt32();
        }

        var topics = new List<TopicData>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            string topicName;
            int partitionCount;

            if (apiVersion >= 1)
            {
                topicName = reader.ReadCompactString() ?? "";
                partitionCount = reader.ReadVarInt() - 1;
            }
            else
            {
                topicName = reader.ReadString() ?? "";
                partitionCount = reader.ReadInt32();
            }

            var partitions = new List<PartitionData>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                var partitionIndex = reader.ReadInt32();
                var leaderId = reader.ReadInt32();
                var leaderEpoch = reader.ReadInt32();

                if (apiVersion >= 1)
                {
                    reader.SkipTaggedFields();
                }

                partitions.Add(new PartitionData
                {
                    PartitionIndex = partitionIndex,
                    LeaderId = leaderId,
                    LeaderEpoch = leaderEpoch
                });
            }

            if (apiVersion >= 1)
            {
                reader.SkipTaggedFields();
            }

            topics.Add(new TopicData
            {
                TopicName = topicName,
                Partitions = partitions
            });
        }

        if (apiVersion >= 1)
        {
            reader.SkipTaggedFields();
        }

        return new BeginQuorumEpochRequest
        {
            ApiKey = ApiKey.BeginQuorumEpoch,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            ClusterId = clusterId,
            Topics = topics
        };
    }
}

/// <summary>
/// Kafka BeginQuorumEpoch response (API Key 53, v0-1).
/// </summary>
public sealed class BeginQuorumEpochResponse : KafkaResponse
{
    /// <summary>The top level error code.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The voter topics.</summary>
    public required List<TopicData> Topics { get; init; }

    public sealed class TopicData
    {
        /// <summary>The topic name.</summary>
        public required string TopicName { get; init; }

        /// <summary>The partitions.</summary>
        public required List<PartitionData> Partitions { get; init; }
    }

    public sealed class PartitionData
    {
        /// <summary>The partition index.</summary>
        public int PartitionIndex { get; init; }

        /// <summary>The partition level error code.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The ID of the current leader or -1 if the leader is unknown.</summary>
        public int LeaderId { get; init; }

        /// <summary>The latest known leader epoch.</summary>
        public int LeaderEpoch { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);

        if (ApiVersion >= 1)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        writer.WriteInt16((short)ErrorCode);

        if (ApiVersion >= 1)
        {
            writer.WriteVarInt(Topics.Count + 1);
        }
        else
        {
            writer.WriteInt32(Topics.Count);
        }

        foreach (var topic in Topics)
        {
            if (ApiVersion >= 1)
            {
                writer.WriteCompactString(topic.TopicName);
                writer.WriteVarInt(topic.Partitions.Count + 1);
            }
            else
            {
                writer.WriteString(topic.TopicName);
                writer.WriteInt32(topic.Partitions.Count);
            }

            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt16((short)partition.ErrorCode);
                writer.WriteInt32(partition.LeaderId);
                writer.WriteInt32(partition.LeaderEpoch);

                if (ApiVersion >= 1)
                {
                    writer.WriteVarInt(0); // Partition tagged fields
                }
            }

            if (ApiVersion >= 1)
            {
                writer.WriteVarInt(0); // Topic tagged fields
            }
        }

        if (ApiVersion >= 1)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static BeginQuorumEpochResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        if (apiVersion >= 1)
        {
            reader.SkipTaggedFields(); // Response header tagged fields
        }

        var errorCode = (ErrorCode)reader.ReadInt16();

        int topicCount;
        if (apiVersion >= 1)
        {
            topicCount = reader.ReadVarInt() - 1;
        }
        else
        {
            topicCount = reader.ReadInt32();
        }

        var topics = new List<TopicData>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            string topicName;
            int partitionCount;

            if (apiVersion >= 1)
            {
                topicName = reader.ReadCompactString() ?? "";
                partitionCount = reader.ReadVarInt() - 1;
            }
            else
            {
                topicName = reader.ReadString() ?? "";
                partitionCount = reader.ReadInt32();
            }

            var partitions = new List<PartitionData>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                var partitionIndex = reader.ReadInt32();
                var partErrorCode = (ErrorCode)reader.ReadInt16();
                var leaderId = reader.ReadInt32();
                var leaderEpoch = reader.ReadInt32();

                if (apiVersion >= 1)
                {
                    reader.SkipTaggedFields();
                }

                partitions.Add(new PartitionData
                {
                    PartitionIndex = partitionIndex,
                    ErrorCode = partErrorCode,
                    LeaderId = leaderId,
                    LeaderEpoch = leaderEpoch
                });
            }

            if (apiVersion >= 1)
            {
                reader.SkipTaggedFields();
            }

            topics.Add(new TopicData
            {
                TopicName = topicName,
                Partitions = partitions
            });
        }

        if (apiVersion >= 1)
        {
            reader.SkipTaggedFields();
        }

        return new BeginQuorumEpochResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = errorCode,
            Topics = topics
        };
    }
}
