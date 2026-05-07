namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka DescribeQuorum request (API Key 55, v0-1).
/// Describes the current quorum for KRaft.
/// </summary>
public sealed class DescribeQuorumRequest : KafkaRequest
{
    /// <summary>The topics to describe.</summary>
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
        public required int PartitionIndex { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0+ is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteVarInt(Topics.Count + 1);
        foreach (var topic in Topics)
        {
            writer.WriteCompactString(topic.TopicName);

            writer.WriteVarInt(topic.Partitions.Count + 1);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteVarInt(0); // Partition tagged fields
            }

            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static DescribeQuorumRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<TopicData>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var topicName = reader.ReadCompactString() ?? "";

            var partitionCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionData>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                partitions.Add(new PartitionData
                {
                    PartitionIndex = reader.ReadInt32()
                });
                reader.SkipTaggedFields();
            }

            reader.SkipTaggedFields();

            topics.Add(new TopicData
            {
                TopicName = topicName,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new DescribeQuorumRequest
        {
            ApiKey = ApiKey.DescribeQuorum,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Topics = topics
        };
    }
}

/// <summary>
/// Kafka DescribeQuorum response (API Key 55, v0-1).
/// </summary>
public sealed class DescribeQuorumResponse : KafkaResponse
{
    /// <summary>The top-level error code.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The topics.</summary>
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
        public required int PartitionIndex { get; init; }

        /// <summary>The error code for this partition.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The ID of the leader, or -1 if there is no leader.</summary>
        public int LeaderId { get; init; }

        /// <summary>The epoch of the leader.</summary>
        public int LeaderEpoch { get; init; }

        /// <summary>The latest known high watermark.</summary>
        public long HighWatermark { get; init; }

        /// <summary>The current voters.</summary>
        public required List<ReplicaState> CurrentVoters { get; init; }

        /// <summary>The current observers.</summary>
        public required List<ReplicaState> Observers { get; init; }
    }

    public sealed class ReplicaState
    {
        /// <summary>The replica ID.</summary>
        public required int ReplicaId { get; init; }

        /// <summary>The last known log end offset of this replica.</summary>
        public long LogEndOffset { get; init; }

        /// <summary>
        /// The last known leader wall clock time time when a follower fetched from the leader.
        /// This is only populated for v1+. In v0, this is -1.
        /// </summary>
        public long LastFetchTimestamp { get; init; } = -1;

        /// <summary>
        /// The leader wall clock append time of the offset for which the follower made the most recent fetch request.
        /// This is only populated for v1+. In v0, this is -1.
        /// </summary>
        public long LastCaughtUpTimestamp { get; init; } = -1;
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
                writer.WriteInt64(partition.HighWatermark);

                writer.WriteVarInt(partition.CurrentVoters.Count + 1);
                foreach (var voter in partition.CurrentVoters)
                {
                    writer.WriteInt32(voter.ReplicaId);
                    writer.WriteInt64(voter.LogEndOffset);
                    if (ApiVersion >= 1)
                    {
                        writer.WriteInt64(voter.LastFetchTimestamp);
                        writer.WriteInt64(voter.LastCaughtUpTimestamp);
                    }
                    writer.WriteVarInt(0); // Voter tagged fields
                }

                writer.WriteVarInt(partition.Observers.Count + 1);
                foreach (var observer in partition.Observers)
                {
                    writer.WriteInt32(observer.ReplicaId);
                    writer.WriteInt64(observer.LogEndOffset);
                    if (ApiVersion >= 1)
                    {
                        writer.WriteInt64(observer.LastFetchTimestamp);
                        writer.WriteInt64(observer.LastCaughtUpTimestamp);
                    }
                    writer.WriteVarInt(0); // Observer tagged fields
                }

                writer.WriteVarInt(0); // Partition tagged fields
            }

            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static DescribeQuorumResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
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
                var highWatermark = reader.ReadInt64();

                var voterCount = reader.ReadVarInt() - 1;
                var currentVoters = new List<ReplicaState>(voterCount);
                for (int k = 0; k < voterCount; k++)
                {
                    var state = new ReplicaState
                    {
                        ReplicaId = reader.ReadInt32(),
                        LogEndOffset = reader.ReadInt64(),
                        LastFetchTimestamp = apiVersion >= 1 ? reader.ReadInt64() : -1,
                        LastCaughtUpTimestamp = apiVersion >= 1 ? reader.ReadInt64() : -1
                    };
                    reader.SkipTaggedFields();
                    currentVoters.Add(state);
                }

                var observerCount = reader.ReadVarInt() - 1;
                var observers = new List<ReplicaState>(observerCount);
                for (int k = 0; k < observerCount; k++)
                {
                    var state = new ReplicaState
                    {
                        ReplicaId = reader.ReadInt32(),
                        LogEndOffset = reader.ReadInt64(),
                        LastFetchTimestamp = apiVersion >= 1 ? reader.ReadInt64() : -1,
                        LastCaughtUpTimestamp = apiVersion >= 1 ? reader.ReadInt64() : -1
                    };
                    reader.SkipTaggedFields();
                    observers.Add(state);
                }

                reader.SkipTaggedFields();

                partitions.Add(new PartitionData
                {
                    PartitionIndex = partitionIndex,
                    ErrorCode = partErrorCode,
                    LeaderId = leaderId,
                    LeaderEpoch = leaderEpoch,
                    HighWatermark = highWatermark,
                    CurrentVoters = currentVoters,
                    Observers = observers
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

        return new DescribeQuorumResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = errorCode,
            Topics = topics
        };
    }
}
