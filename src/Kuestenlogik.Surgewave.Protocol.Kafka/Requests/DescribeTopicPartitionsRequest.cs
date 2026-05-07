namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka DescribeTopicPartitions request (API Key 75, v0-0).
/// Describe topic partitions with paginated results.
/// </summary>
public sealed class DescribeTopicPartitionsRequest : KafkaRequest
{
    /// <summary>The topics to fetch details for.</summary>
    public required List<TopicRequest> Topics { get; init; }

    /// <summary>The maximum number of partitions included in the response.</summary>
    public int ResponsePartitionLimit { get; init; } = 2000;

    /// <summary>
    /// The cursor for pagination. Null for the first request.
    /// </summary>
    public Cursor? StartingCursor { get; init; }

    public sealed class TopicRequest
    {
        /// <summary>The topic name.</summary>
        public required string Name { get; init; }
    }

    public sealed class Cursor
    {
        /// <summary>The name for the topic to start from.</summary>
        public required string TopicName { get; init; }

        /// <summary>The partition index to start from.</summary>
        public int PartitionIndex { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteVarInt(Topics.Count + 1);
        foreach (var topic in Topics)
        {
            writer.WriteCompactString(topic.Name);
            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteInt32(ResponsePartitionLimit);

        if (StartingCursor == null)
        {
            writer.WriteInt8(-1); // Null marker for optional struct
        }
        else
        {
            writer.WriteInt8(1); // Present marker
            writer.WriteCompactString(StartingCursor.TopicName);
            writer.WriteInt32(StartingCursor.PartitionIndex);
            writer.WriteVarInt(0); // Cursor tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static DescribeTopicPartitionsRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<TopicRequest>(topicCount);
        for (int i = 0; i < topicCount; i++)
        {
            topics.Add(new TopicRequest
            {
                Name = reader.ReadCompactString() ?? ""
            });
            reader.SkipTaggedFields();
        }

        var responsePartitionLimit = reader.ReadInt32();

        Cursor? startingCursor = null;
        var cursorPresent = reader.ReadInt8();
        if (cursorPresent >= 0)
        {
            var topicName = reader.ReadCompactString() ?? "";
            var partitionIndex = reader.ReadInt32();
            reader.SkipTaggedFields();

            startingCursor = new Cursor
            {
                TopicName = topicName,
                PartitionIndex = partitionIndex
            };
        }

        reader.SkipTaggedFields();

        return new DescribeTopicPartitionsRequest
        {
            ApiKey = ApiKey.DescribeTopicPartitions,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Topics = topics,
            ResponsePartitionLimit = responsePartitionLimit,
            StartingCursor = startingCursor
        };
    }
}

/// <summary>
/// Kafka DescribeTopicPartitions response (API Key 75, v0-0).
/// </summary>
public sealed class DescribeTopicPartitionsResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>Each topic in the response.</summary>
    public required List<DescribeTopicPartitionsResponseTopic> Topics { get; init; }

    /// <summary>
    /// The next cursor, or null if there are no more partitions.
    /// </summary>
    public Cursor? NextCursor { get; init; }

    public sealed class DescribeTopicPartitionsResponseTopic
    {
        /// <summary>The topic error code, or 0 if there was no error.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The topic name.</summary>
        public string? Name { get; init; }

        /// <summary>The topic ID.</summary>
        public Guid TopicId { get; init; }

        /// <summary>True if the topic is internal.</summary>
        public bool IsInternal { get; init; }

        /// <summary>Each partition in the topic.</summary>
        public required List<DescribeTopicPartitionsResponsePartition> Partitions { get; init; }

        /// <summary>32-bit bitfield to represent authorized operations.</summary>
        public int TopicAuthorizedOperations { get; init; } = int.MinValue;
    }

    public sealed class DescribeTopicPartitionsResponsePartition
    {
        /// <summary>The partition error code, or 0 if there was no error.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The partition index.</summary>
        public int PartitionIndex { get; init; }

        /// <summary>The ID of the leader broker.</summary>
        public int LeaderId { get; init; }

        /// <summary>The leader epoch, or -1 if there is no leader.</summary>
        public int LeaderEpoch { get; init; } = -1;

        /// <summary>The set of all nodes that host this partition.</summary>
        public required List<int> ReplicaNodes { get; init; }

        /// <summary>The set of nodes that are in sync with the leader.</summary>
        public required List<int> IsrNodes { get; init; }

        /// <summary>The set of nodes eligible to become leader.</summary>
        public required List<int> EligibleLeaderReplicas { get; init; }

        /// <summary>The last known ELR.</summary>
        public required List<int> LastKnownElr { get; init; }

        /// <summary>The set of nodes that are offline.</summary>
        public required List<int> OfflineReplicas { get; init; }
    }

    public sealed class Cursor
    {
        /// <summary>The name for the topic to start from.</summary>
        public required string TopicName { get; init; }

        /// <summary>The partition index to start from.</summary>
        public int PartitionIndex { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);

        writer.WriteVarInt(Topics.Count + 1);
        foreach (var topic in Topics)
        {
            writer.WriteInt16((short)topic.ErrorCode);
            writer.WriteCompactString(topic.Name);
            writer.WriteUuid(topic.TopicId);
            writer.WriteBoolean(topic.IsInternal);

            writer.WriteVarInt(topic.Partitions.Count + 1);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt16((short)partition.ErrorCode);
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt32(partition.LeaderId);
                writer.WriteInt32(partition.LeaderEpoch);

                writer.WriteVarInt(partition.ReplicaNodes.Count + 1);
                foreach (var replica in partition.ReplicaNodes)
                {
                    writer.WriteInt32(replica);
                }

                writer.WriteVarInt(partition.IsrNodes.Count + 1);
                foreach (var isr in partition.IsrNodes)
                {
                    writer.WriteInt32(isr);
                }

                writer.WriteVarInt(partition.EligibleLeaderReplicas.Count + 1);
                foreach (var elr in partition.EligibleLeaderReplicas)
                {
                    writer.WriteInt32(elr);
                }

                writer.WriteVarInt(partition.LastKnownElr.Count + 1);
                foreach (var lke in partition.LastKnownElr)
                {
                    writer.WriteInt32(lke);
                }

                writer.WriteVarInt(partition.OfflineReplicas.Count + 1);
                foreach (var offline in partition.OfflineReplicas)
                {
                    writer.WriteInt32(offline);
                }

                writer.WriteVarInt(0); // Partition tagged fields
            }

            writer.WriteInt32(topic.TopicAuthorizedOperations);

            writer.WriteVarInt(0); // Topic tagged fields
        }

        if (NextCursor == null)
        {
            writer.WriteInt8(-1); // Null marker
        }
        else
        {
            writer.WriteInt8(1); // Present marker
            writer.WriteCompactString(NextCursor.TopicName);
            writer.WriteInt32(NextCursor.PartitionIndex);
            writer.WriteVarInt(0); // Cursor tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static DescribeTopicPartitionsResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();

        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<DescribeTopicPartitionsResponseTopic>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var errorCode = (ErrorCode)reader.ReadInt16();
            var name = reader.ReadCompactString();
            var topicId = reader.ReadUuid();
            var isInternal = reader.ReadBoolean();

            var partitionCount = reader.ReadVarInt() - 1;
            var partitions = new List<DescribeTopicPartitionsResponsePartition>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                var partErrorCode = (ErrorCode)reader.ReadInt16();
                var partitionIndex = reader.ReadInt32();
                var leaderId = reader.ReadInt32();
                var leaderEpoch = reader.ReadInt32();

                var replicaCount = reader.ReadVarInt() - 1;
                var replicaNodes = new List<int>(replicaCount);
                for (int k = 0; k < replicaCount; k++)
                {
                    replicaNodes.Add(reader.ReadInt32());
                }

                var isrCount = reader.ReadVarInt() - 1;
                var isrNodes = new List<int>(isrCount);
                for (int k = 0; k < isrCount; k++)
                {
                    isrNodes.Add(reader.ReadInt32());
                }

                var elrCount = reader.ReadVarInt() - 1;
                var eligibleLeaderReplicas = new List<int>(elrCount);
                for (int k = 0; k < elrCount; k++)
                {
                    eligibleLeaderReplicas.Add(reader.ReadInt32());
                }

                var lkeCount = reader.ReadVarInt() - 1;
                var lastKnownElr = new List<int>(lkeCount);
                for (int k = 0; k < lkeCount; k++)
                {
                    lastKnownElr.Add(reader.ReadInt32());
                }

                var offlineCount = reader.ReadVarInt() - 1;
                var offlineReplicas = new List<int>(offlineCount);
                for (int k = 0; k < offlineCount; k++)
                {
                    offlineReplicas.Add(reader.ReadInt32());
                }

                reader.SkipTaggedFields();

                partitions.Add(new DescribeTopicPartitionsResponsePartition
                {
                    ErrorCode = partErrorCode,
                    PartitionIndex = partitionIndex,
                    LeaderId = leaderId,
                    LeaderEpoch = leaderEpoch,
                    ReplicaNodes = replicaNodes,
                    IsrNodes = isrNodes,
                    EligibleLeaderReplicas = eligibleLeaderReplicas,
                    LastKnownElr = lastKnownElr,
                    OfflineReplicas = offlineReplicas
                });
            }

            var topicAuthorizedOperations = reader.ReadInt32();

            reader.SkipTaggedFields();

            topics.Add(new DescribeTopicPartitionsResponseTopic
            {
                ErrorCode = errorCode,
                Name = name,
                TopicId = topicId,
                IsInternal = isInternal,
                Partitions = partitions,
                TopicAuthorizedOperations = topicAuthorizedOperations
            });
        }

        Cursor? nextCursor = null;
        var cursorPresent = reader.ReadInt8();
        if (cursorPresent >= 0)
        {
            var topicName = reader.ReadCompactString() ?? "";
            var partitionIndex = reader.ReadInt32();
            reader.SkipTaggedFields();

            nextCursor = new Cursor
            {
                TopicName = topicName,
                PartitionIndex = partitionIndex
            };
        }

        reader.SkipTaggedFields();

        return new DescribeTopicPartitionsResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            Topics = topics,
            NextCursor = nextCursor
        };
    }
}
