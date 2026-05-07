namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka Vote request (API Key 52, v0-1).
/// KRaft quorum protocol - request votes for leader election.
/// </summary>
public sealed class VoteRequest : KafkaRequest
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

        /// <summary>The bumped epoch of the candidate sending the request.</summary>
        public int CandidateEpoch { get; init; }

        /// <summary>The ID of the voter sending the request.</summary>
        public int CandidateId { get; init; }

        /// <summary>The epoch of the last record written to the metadata log.</summary>
        public int LastOffsetEpoch { get; init; }

        /// <summary>The offset of the last record written to the metadata log.</summary>
        public long LastOffset { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0+ is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteCompactString(ClusterId);

        writer.WriteVarInt(Topics.Count + 1);
        foreach (var topic in Topics)
        {
            writer.WriteCompactString(topic.TopicName);

            writer.WriteVarInt(topic.Partitions.Count + 1);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt32(partition.CandidateEpoch);
                writer.WriteInt32(partition.CandidateId);
                writer.WriteInt32(partition.LastOffsetEpoch);
                writer.WriteInt64(partition.LastOffset);
                writer.WriteVarInt(0); // Partition tagged fields
            }

            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static VoteRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var clusterId = reader.ReadCompactString();

        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<TopicData>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var topicName = reader.ReadCompactString() ?? "";

            var partitionCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionData>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                var partitionIndex = reader.ReadInt32();
                var candidateEpoch = reader.ReadInt32();
                var candidateId = reader.ReadInt32();
                var lastOffsetEpoch = reader.ReadInt32();
                var lastOffset = reader.ReadInt64();
                reader.SkipTaggedFields();

                partitions.Add(new PartitionData
                {
                    PartitionIndex = partitionIndex,
                    CandidateEpoch = candidateEpoch,
                    CandidateId = candidateId,
                    LastOffsetEpoch = lastOffsetEpoch,
                    LastOffset = lastOffset
                });
            }

            reader.SkipTaggedFields();

            topics.Add(new TopicData
            {
                TopicName = topicName,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new VoteRequest
        {
            ApiKey = ApiKey.Vote,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            ClusterId = clusterId,
            Topics = topics
        };
    }
}

/// <summary>
/// Kafka Vote response (API Key 52, v0-1).
/// </summary>
public sealed class VoteResponse : KafkaResponse
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

        /// <summary>True if the vote was granted.</summary>
        public bool VoteGranted { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt16((short)ErrorCode);

        writer.WriteVarInt(Topics.Count + 1);
        foreach (var topic in Topics)
        {
            writer.WriteCompactString(topic.TopicName);

            writer.WriteVarInt(topic.Partitions.Count + 1);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt16((short)partition.ErrorCode);
                writer.WriteInt32(partition.LeaderId);
                writer.WriteInt32(partition.LeaderEpoch);
                writer.WriteBoolean(partition.VoteGranted);
                writer.WriteVarInt(0); // Partition tagged fields
            }

            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static VoteResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var errorCode = (ErrorCode)reader.ReadInt16();

        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<TopicData>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var topicName = reader.ReadCompactString() ?? "";

            var partitionCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionData>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                var partitionIndex = reader.ReadInt32();
                var partErrorCode = (ErrorCode)reader.ReadInt16();
                var leaderId = reader.ReadInt32();
                var leaderEpoch = reader.ReadInt32();
                var voteGranted = reader.ReadBoolean();
                reader.SkipTaggedFields();

                partitions.Add(new PartitionData
                {
                    PartitionIndex = partitionIndex,
                    ErrorCode = partErrorCode,
                    LeaderId = leaderId,
                    LeaderEpoch = leaderEpoch,
                    VoteGranted = voteGranted
                });
            }

            reader.SkipTaggedFields();

            topics.Add(new TopicData
            {
                TopicName = topicName,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new VoteResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = errorCode,
            Topics = topics
        };
    }
}
