namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka OffsetForLeaderEpoch request (API Key 23, v0-4).
/// Used by followers to find the offset for a specific leader epoch,
/// enabling log truncation detection and recovery.
/// </summary>
public sealed class OffsetForLeaderEpochRequest : KafkaRequest
{
    /// <summary>The broker ID of the follower, or -1 if this request is from a consumer (v3+).</summary>
    public int ReplicaId { get; init; } = -2; // -2 = debug client, -1 = consumer

    /// <summary>Topics to get offsets for.</summary>
    public required List<TopicRequest> Topics { get; init; }

    public sealed class TopicRequest
    {
        /// <summary>The topic name.</summary>
        public required string Topic { get; init; }

        /// <summary>Partitions to get offsets for.</summary>
        public required List<PartitionRequest> Partitions { get; init; }
    }

    public sealed class PartitionRequest
    {
        /// <summary>The partition index.</summary>
        public required int PartitionIndex { get; init; }

        /// <summary>
        /// An epoch used to fence consumers/replicas with old metadata (v2+).
        /// If the epoch provided is larger than the current partition leader epoch,
        /// the broker returns the current leader epoch.
        /// </summary>
        public int CurrentLeaderEpoch { get; init; } = -1;

        /// <summary>The epoch to look up an end offset for.</summary>
        public required int LeaderEpoch { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        var isFlexible = ApiVersion >= 4;

        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteCompactString(ClientId);
            writer.WriteVarInt(0); // Header tagged fields
        }
        else
        {
            writer.WriteString(ClientId);
        }

        // v3+: ReplicaId
        if (ApiVersion >= 3)
        {
            writer.WriteInt32(ReplicaId);
        }

        // Topics array
        if (isFlexible)
        {
            writer.WriteVarInt(Topics.Count + 1);
        }
        else
        {
            writer.WriteInt32(Topics.Count);
        }

        foreach (var topic in Topics)
        {
            if (isFlexible)
            {
                writer.WriteCompactString(topic.Topic);
                writer.WriteVarInt(topic.Partitions.Count + 1);
            }
            else
            {
                writer.WriteString(topic.Topic);
                writer.WriteInt32(topic.Partitions.Count);
            }

            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);

                // v2+: CurrentLeaderEpoch
                if (ApiVersion >= 2)
                {
                    writer.WriteInt32(partition.CurrentLeaderEpoch);
                }

                writer.WriteInt32(partition.LeaderEpoch);

                if (isFlexible)
                {
                    writer.WriteVarInt(0); // Partition tagged fields
                }
            }

            if (isFlexible)
            {
                writer.WriteVarInt(0); // Topic tagged fields
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static OffsetForLeaderEpochRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var isFlexible = apiVersion >= 4;

        var replicaId = apiVersion >= 3 ? reader.ReadInt32() : -2;

        var topicCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
        var topics = new List<TopicRequest>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var topicName = isFlexible ? reader.ReadCompactString() ?? "" : reader.ReadString() ?? "";
            var partitionCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
            var partitions = new List<PartitionRequest>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                var partitionIndex = reader.ReadInt32();
                var currentLeaderEpoch = apiVersion >= 2 ? reader.ReadInt32() : -1;
                var leaderEpoch = reader.ReadInt32();

                if (isFlexible)
                {
                    reader.SkipTaggedFields();
                }

                partitions.Add(new PartitionRequest
                {
                    PartitionIndex = partitionIndex,
                    CurrentLeaderEpoch = currentLeaderEpoch,
                    LeaderEpoch = leaderEpoch
                });
            }

            if (isFlexible)
            {
                reader.SkipTaggedFields();
            }

            topics.Add(new TopicRequest
            {
                Topic = topicName,
                Partitions = partitions
            });
        }

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new OffsetForLeaderEpochRequest
        {
            ApiKey = ApiKey.OffsetForLeaderEpoch,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            ReplicaId = replicaId,
            Topics = topics
        };
    }
}

/// <summary>
/// Kafka OffsetForLeaderEpoch response (API Key 23, v0-4).
/// </summary>
public sealed class OffsetForLeaderEpochResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled (v2+).</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>Topics with epoch end offsets.</summary>
    public required List<TopicResponse> Topics { get; init; }

    public sealed class TopicResponse
    {
        /// <summary>The topic name.</summary>
        public required string Topic { get; init; }

        /// <summary>Partitions with epoch end offsets.</summary>
        public required List<PartitionResponse> Partitions { get; init; }
    }

    public sealed class PartitionResponse
    {
        /// <summary>The error code, or 0 if there was no error.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The partition index.</summary>
        public required int PartitionIndex { get; init; }

        /// <summary>The leader epoch of the partition (v1+).</summary>
        public int LeaderEpoch { get; init; } = -1;

        /// <summary>
        /// The end offset of the epoch, which is the start offset of the next epoch.
        /// -1 if the requested epoch is the current epoch.
        /// </summary>
        public long EndOffset { get; init; } = -1;
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        var isFlexible = ApiVersion >= 4;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        // v2+: ThrottleTimeMs
        if (ApiVersion >= 2)
        {
            writer.WriteInt32(ThrottleTimeMs);
        }

        // Topics array
        if (isFlexible)
        {
            writer.WriteVarInt(Topics.Count + 1);
        }
        else
        {
            writer.WriteInt32(Topics.Count);
        }

        foreach (var topic in Topics)
        {
            if (isFlexible)
            {
                writer.WriteCompactString(topic.Topic);
                writer.WriteVarInt(topic.Partitions.Count + 1);
            }
            else
            {
                writer.WriteString(topic.Topic);
                writer.WriteInt32(topic.Partitions.Count);
            }

            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt16((short)partition.ErrorCode);
                writer.WriteInt32(partition.PartitionIndex);

                // v1+: LeaderEpoch
                if (ApiVersion >= 1)
                {
                    writer.WriteInt32(partition.LeaderEpoch);
                }

                writer.WriteInt64(partition.EndOffset);

                if (isFlexible)
                {
                    writer.WriteVarInt(0); // Partition tagged fields
                }
            }

            if (isFlexible)
            {
                writer.WriteVarInt(0); // Topic tagged fields
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static OffsetForLeaderEpochResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        var isFlexible = apiVersion >= 4;

        if (isFlexible)
        {
            reader.SkipTaggedFields(); // Response header tagged fields
        }

        var throttleTimeMs = apiVersion >= 2 ? reader.ReadInt32() : 0;

        var topicCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
        var topics = new List<TopicResponse>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var topicName = isFlexible ? reader.ReadCompactString() ?? "" : reader.ReadString() ?? "";
            var partitionCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
            var partitions = new List<PartitionResponse>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                var errorCode = (ErrorCode)reader.ReadInt16();
                var partitionIndex = reader.ReadInt32();
                var leaderEpoch = apiVersion >= 1 ? reader.ReadInt32() : -1;
                var endOffset = reader.ReadInt64();

                if (isFlexible)
                {
                    reader.SkipTaggedFields();
                }

                partitions.Add(new PartitionResponse
                {
                    ErrorCode = errorCode,
                    PartitionIndex = partitionIndex,
                    LeaderEpoch = leaderEpoch,
                    EndOffset = endOffset
                });
            }

            if (isFlexible)
            {
                reader.SkipTaggedFields();
            }

            topics.Add(new TopicResponse
            {
                Topic = topicName,
                Partitions = partitions
            });
        }

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new OffsetForLeaderEpochResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            Topics = topics
        };
    }
}
