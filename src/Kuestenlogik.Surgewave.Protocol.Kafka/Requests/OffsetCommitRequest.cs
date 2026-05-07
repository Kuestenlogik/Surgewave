namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// OffsetCommit request - Commit offsets for a consumer group
/// v2-7: Non-flexible format with topic names
/// v8-9: Flexible format with topic names (v9 adds KIP-848 consumer protocol support)
/// v10+: Flexible format with TopicId (UUID) instead of topic names
/// </summary>
public sealed class OffsetCommitRequest : KafkaRequest
{
    public required string GroupId { get; init; }
    /// <summary>GenerationId (classic protocol) or MemberEpoch (KIP-848 consumer protocol)</summary>
    public int GenerationIdOrMemberEpoch { get; init; } = -1;
    public string? MemberId { get; init; }
    public string? GroupInstanceId { get; init; }
    public long RetentionTimeMs { get; init; } = -1; // v2-v4
    public required List<TopicPartitionCommit> Topics { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.OffsetCommit, ApiVersion);

        if (isFlexible)
        {
            writer.WriteCompactString(GroupId);
        }
        else
        {
            writer.WriteString(GroupId);
        }

        if (ApiVersion >= 1)
        {
            writer.WriteInt32(GenerationIdOrMemberEpoch);
            if (isFlexible)
            {
                writer.WriteCompactString(MemberId ?? string.Empty);
            }
            else
            {
                writer.WriteString(MemberId ?? string.Empty);
            }
        }

        if (ApiVersion >= 7)
        {
            if (isFlexible)
            {
                writer.WriteCompactString(GroupInstanceId);
            }
            else
            {
                writer.WriteString(GroupInstanceId ?? string.Empty);
            }
        }

        if (ApiVersion >= 2 && ApiVersion <= 4)
        {
            writer.WriteInt64(RetentionTimeMs);
        }

        // Topics - v10+ uses TopicId instead of topic name
        if (isFlexible)
        {
            writer.WriteCompactArray(Topics.ToArray(), (topic) =>
            {
                if (ApiVersion >= 10)
                {
                    writer.WriteUuid(topic.TopicId);
                }
                else
                {
                    writer.WriteCompactString(topic.Topic);
                }
                writer.WriteCompactArray(topic.Partitions.ToArray(), (partition) =>
                {
                    writer.WriteInt32(partition.PartitionIndex);
                    writer.WriteInt64(partition.CommittedOffset);

                    if (ApiVersion >= 6)
                    {
                        writer.WriteInt32(partition.CommittedLeaderEpoch);
                    }

                    if (ApiVersion == 1)
                    {
                        writer.WriteInt64(partition.CommitTimestamp);
                    }

                    writer.WriteCompactString(partition.Metadata ?? string.Empty);
                    writer.WriteVarInt(0); // Tagged fields
                });
                writer.WriteVarInt(0); // Tagged fields
            });
            writer.WriteVarInt(0); // Tagged fields
        }
        else
        {
            writer.WriteInt32(Topics.Count);
            foreach (var topic in Topics)
            {
                writer.WriteString(topic.Topic);
                writer.WriteInt32(topic.Partitions.Count);
                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt32(partition.PartitionIndex);
                    writer.WriteInt64(partition.CommittedOffset);

                    if (ApiVersion >= 6)
                    {
                        writer.WriteInt32(partition.CommittedLeaderEpoch);
                    }

                    if (ApiVersion == 1)
                    {
                        writer.WriteInt64(partition.CommitTimestamp);
                    }

                    writer.WriteString(partition.Metadata ?? string.Empty);
                }
            }
        }
    }

    public static OffsetCommitRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.OffsetCommit, apiVersion);
        string groupId;
        int generationIdOrMemberEpoch = -1;
        string? memberId = null;
        string? groupInstanceId = null;
        long retentionTimeMs = -1;
        List<TopicPartitionCommit> topics;

        if (isFlexible)
        {
            var stream = (MemoryStream)reader.BaseStream;
            var remaining = new byte[stream.Length - stream.Position];
            stream.Read(remaining, 0, remaining.Length);
            var kafkaReader = new KafkaProtocolReader(remaining);

            groupId = kafkaReader.ReadCompactString() ?? string.Empty;

            if (apiVersion >= 1)
            {
                generationIdOrMemberEpoch = kafkaReader.ReadInt32();
                memberId = kafkaReader.ReadCompactString();
            }

            if (apiVersion >= 7)
            {
                groupInstanceId = kafkaReader.ReadCompactString();
                if (groupInstanceId == string.Empty) groupInstanceId = null;
            }

            if (apiVersion >= 2 && apiVersion <= 4)
            {
                retentionTimeMs = kafkaReader.ReadInt64();
            }

            var topicCount = kafkaReader.ReadVarInt() - 1;
            topics = new List<TopicPartitionCommit>();

            for (int i = 0; i < topicCount; i++)
            {
                // v10+ uses TopicId instead of topic name
                string? topicName = null;
                Guid topicId = Guid.Empty;

                if (apiVersion >= 10)
                {
                    topicId = kafkaReader.ReadUuid();
                }
                else
                {
                    topicName = kafkaReader.ReadCompactString();
                }

                var partitionCount = kafkaReader.ReadVarInt() - 1;
                var partitions = new List<PartitionCommit>();

                for (int j = 0; j < partitionCount; j++)
                {
                    var partitionIndex = kafkaReader.ReadInt32();
                    var committedOffset = kafkaReader.ReadInt64();
                    int committedLeaderEpoch = -1;
                    long commitTimestamp = -1;

                    if (apiVersion >= 6)
                    {
                        committedLeaderEpoch = kafkaReader.ReadInt32();
                    }

                    if (apiVersion == 1)
                    {
                        commitTimestamp = kafkaReader.ReadInt64();
                    }

                    var metadata = kafkaReader.ReadCompactString();

                    partitions.Add(new PartitionCommit
                    {
                        PartitionIndex = partitionIndex,
                        CommittedOffset = committedOffset,
                        CommittedLeaderEpoch = committedLeaderEpoch,
                        CommitTimestamp = commitTimestamp,
                        Metadata = metadata
                    });

                    kafkaReader.ReadVarInt(); // Skip partition tagged fields
                }

                topics.Add(new TopicPartitionCommit
                {
                    Topic = topicName ?? string.Empty,
                    TopicId = topicId,
                    Partitions = partitions
                });

                kafkaReader.ReadVarInt(); // Skip topic tagged fields
            }

            kafkaReader.ReadVarInt(); // Skip request tagged fields
            stream.Position = stream.Length - kafkaReader.Remaining;
        }
        else
        {
            groupId = BinaryHelpers.ReadString(reader) ?? string.Empty;

            if (apiVersion >= 1)
            {
                generationIdOrMemberEpoch = BinaryHelpers.ReadInt32BigEndian(reader);
                memberId = BinaryHelpers.ReadString(reader);
            }

            if (apiVersion >= 7)
            {
                groupInstanceId = BinaryHelpers.ReadString(reader);
                if (groupInstanceId == string.Empty) groupInstanceId = null;
            }

            if (apiVersion >= 2 && apiVersion <= 4)
            {
                retentionTimeMs = BinaryHelpers.ReadInt64BigEndian(reader);
            }

            var topicCount = BinaryHelpers.ReadInt32BigEndian(reader);
            topics = new List<TopicPartitionCommit>();

            for (int i = 0; i < topicCount; i++)
            {
                var topicName = BinaryHelpers.ReadString(reader);
                var partitionCount = BinaryHelpers.ReadInt32BigEndian(reader);
                var partitions = new List<PartitionCommit>();

                for (int j = 0; j < partitionCount; j++)
                {
                    var partitionIndex = BinaryHelpers.ReadInt32BigEndian(reader);
                    var committedOffset = BinaryHelpers.ReadInt64BigEndian(reader);
                    int committedLeaderEpoch = -1;
                    long commitTimestamp = -1;

                    if (apiVersion >= 6)
                    {
                        committedLeaderEpoch = BinaryHelpers.ReadInt32BigEndian(reader);
                    }

                    if (apiVersion == 1)
                    {
                        commitTimestamp = BinaryHelpers.ReadInt64BigEndian(reader);
                    }

                    var metadata = BinaryHelpers.ReadString(reader);

                    partitions.Add(new PartitionCommit
                    {
                        PartitionIndex = partitionIndex,
                        CommittedOffset = committedOffset,
                        CommittedLeaderEpoch = committedLeaderEpoch,
                        CommitTimestamp = commitTimestamp,
                        Metadata = metadata
                    });
                }

                topics.Add(new TopicPartitionCommit
                {
                    Topic = topicName ?? string.Empty,
                    Partitions = partitions
                });
            }
        }

        return new OffsetCommitRequest
        {
            ApiKey = ApiKey.OffsetCommit,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            GroupId = groupId,
            GenerationIdOrMemberEpoch = generationIdOrMemberEpoch,
            MemberId = memberId,
            GroupInstanceId = groupInstanceId,
            RetentionTimeMs = retentionTimeMs,
            Topics = topics
        };
    }

}

public sealed class TopicPartitionCommit
{
    /// <summary>Topic name (v0-9)</summary>
    public required string Topic { get; init; }
    /// <summary>Topic ID (v10+)</summary>
    public Guid TopicId { get; init; }
    public required List<PartitionCommit> Partitions { get; init; }
}

public sealed class PartitionCommit
{
    public required int PartitionIndex { get; init; }
    public required long CommittedOffset { get; init; }
    public int CommittedLeaderEpoch { get; init; } = -1;
    public long CommitTimestamp { get; init; } = -1; // v1 only
    public string? Metadata { get; init; }
}

/// <summary>
/// OffsetCommit response
/// v2-7: Non-flexible format with topic names
/// v8-9: Flexible format with topic names
/// v10+: Flexible format with TopicId (UUID) instead of topic names
/// </summary>
public sealed class OffsetCommitResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; }
    public required List<TopicPartitionCommitResult> Topics { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // Response header
        writer.WriteInt32(CorrelationId);
        bool isFlexible = ApiVersion >= 8;
        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        if (ApiVersion >= 3)
        {
            writer.WriteInt32(ThrottleTimeMs);
        }

        if (isFlexible)
        {
            writer.WriteCompactArray(Topics.ToArray(), (topic) =>
            {
                if (ApiVersion >= 10)
                {
                    writer.WriteUuid(topic.TopicId);
                }
                else
                {
                    writer.WriteCompactString(topic.Topic);
                }
                writer.WriteCompactArray(topic.Partitions.ToArray(), (partition) =>
                {
                    writer.WriteInt32(partition.PartitionIndex);
                    writer.WriteInt16((short)partition.ErrorCode);
                    writer.WriteVarInt(0); // Tagged fields
                });
                writer.WriteVarInt(0); // Tagged fields
            });
            writer.WriteVarInt(0); // Tagged fields
        }
        else
        {
            writer.WriteInt32(Topics.Count);
            foreach (var topic in Topics)
            {
                writer.WriteString(topic.Topic);
                writer.WriteInt32(topic.Partitions.Count);
                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt32(partition.PartitionIndex);
                    writer.WriteInt16((short)partition.ErrorCode);
                }
            }
        }
    }
}

public sealed class TopicPartitionCommitResult
{
    /// <summary>Topic name (v0-9)</summary>
    public required string Topic { get; init; }
    /// <summary>Topic ID (v10+)</summary>
    public Guid TopicId { get; init; }
    public required List<PartitionCommitResult> Partitions { get; init; }
}

public sealed class PartitionCommitResult
{
    public required int PartitionIndex { get; init; }
    public required ErrorCode ErrorCode { get; init; }
}
