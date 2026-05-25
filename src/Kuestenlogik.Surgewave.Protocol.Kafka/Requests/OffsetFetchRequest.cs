namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// OffsetFetch request - Fetch committed offsets for consumer groups.
/// v1-7: Single group with GroupId and Topics fields
/// v8+: Multiple groups with Groups array (multi-group batch fetch)
/// v9+: Adds MemberId and MemberEpoch for KIP-848 consumer protocol
/// v10+: Uses TopicId (UUID) instead of topic names
/// </summary>
public sealed class OffsetFetchRequest : KafkaRequest
{
    /// <summary>GroupId for v1-7 requests</summary>
    public string? GroupId { get; init; }
    /// <summary>Topics for v1-7 requests (null means all topics)</summary>
    public List<TopicPartitionRequest>? Topics { get; init; }
    /// <summary>RequireStable flag (v7+)</summary>
    public bool RequireStable { get; init; }
    /// <summary>Groups array for v8+ requests (multi-group batch fetch)</summary>
    public List<OffsetFetchRequestGroup>? Groups { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.OffsetFetch, ApiVersion);

        if (ApiVersion >= 8)
        {
            // v8+: Groups array format
            var groups = Groups ?? [];
            writer.WriteVarInt(groups.Count + 1); // COMPACT_ARRAY
            foreach (var group in groups)
            {
                writer.WriteCompactString(group.GroupId);

                // v9+: MemberId and MemberEpoch for KIP-848
                if (ApiVersion >= 9)
                {
                    writer.WriteCompactString(group.MemberId);
                    writer.WriteInt32(group.MemberEpoch);
                }

                if (group.Topics != null)
                {
                    writer.WriteVarInt(group.Topics.Count + 1);
                    foreach (var topic in group.Topics)
                    {
                        // v10+: TopicId instead of topic name
                        if (ApiVersion >= 10)
                        {
                            writer.WriteUuid(topic.TopicId);
                        }
                        else
                        {
                            writer.WriteCompactString(topic.Topic);
                        }
                        writer.WriteVarInt(topic.PartitionIndexes.Count + 1);
                        foreach (var partition in topic.PartitionIndexes)
                        {
                            writer.WriteInt32(partition);
                        }
                        writer.WriteVarInt(0); // Topic tagged fields
                    }
                }
                else
                {
                    writer.WriteVarInt(0); // Null topics array
                }
                writer.WriteVarInt(0); // Group tagged fields
            }
            writer.WriteInt8((sbyte)(RequireStable ? 1 : 0));
            writer.WriteVarInt(0); // Request tagged fields
        }
        else if (isFlexible)
        {
            // v6-7: Flexible format with single group
            writer.WriteCompactString(GroupId);
            if (Topics != null)
            {
                writer.WriteVarInt(Topics.Count + 1);
                foreach (var topic in Topics)
                {
                    writer.WriteCompactString(topic.Topic);
                    writer.WriteVarInt(topic.PartitionIndexes.Count + 1);
                    foreach (var partition in topic.PartitionIndexes)
                    {
                        writer.WriteInt32(partition);
                    }
                    writer.WriteVarInt(0); // Tagged fields
                }
            }
            else
            {
                writer.WriteVarInt(0); // Null compact array
            }
            if (ApiVersion >= 7)
            {
                writer.WriteInt8((sbyte)(RequireStable ? 1 : 0));
            }
            writer.WriteVarInt(0); // Tagged fields
        }
        else
        {
            // v1-5: Non-flexible format
            writer.WriteString(GroupId ?? string.Empty);
            if (Topics != null)
            {
                writer.WriteInt32(Topics.Count);
                foreach (var topic in Topics)
                {
                    writer.WriteString(topic.Topic);
                    writer.WriteInt32(topic.PartitionIndexes.Count);
                    foreach (var partition in topic.PartitionIndexes)
                    {
                        writer.WriteInt32(partition);
                    }
                }
            }
            else if (ApiVersion >= 2)
            {
                writer.WriteInt32(-1); // Null array in v2+
            }
        }
    }

    public static OffsetFetchRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.OffsetFetch, apiVersion);

        if (apiVersion >= 8)
        {
            // v8+: Groups array format
            return ReadV8Plus(reader, apiVersion, correlationId, clientId);
        }
        else if (isFlexible)
        {
            // v6-7: Flexible format
            return ReadFlexible(reader, apiVersion, correlationId, clientId);
        }
        else
        {
            // v1-5: Non-flexible format
            return ReadNonFlexible(reader, apiVersion, correlationId, clientId);
        }
    }

    private static OffsetFetchRequest ReadV8Plus(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remaining = new byte[stream.Length - stream.Position];
        stream.Read(remaining, 0, remaining.Length);
        var kafkaReader = new KafkaProtocolReader(remaining);

        var groups = new List<OffsetFetchRequestGroup>();
        var groupsLength = kafkaReader.ReadVarInt() - 1;
        for (int g = 0; g < groupsLength; g++)
        {
            var groupId = kafkaReader.ReadCompactString() ?? string.Empty;

            // v9+: MemberId and MemberEpoch for KIP-848
            string? memberId = null;
            int memberEpoch = -1;
            if (apiVersion >= 9)
            {
                memberId = kafkaReader.ReadCompactString();
                memberEpoch = kafkaReader.ReadInt32();
            }

            List<TopicPartitionRequest>? topics = null;
            var topicsLength = kafkaReader.ReadVarInt();
            if (topicsLength > 0)
            {
                topics = new List<TopicPartitionRequest>();
                for (int t = 0; t < topicsLength - 1; t++)
                {
                    // v10+: TopicId instead of topic name
                    string topicName = string.Empty;
                    Guid topicId = Guid.Empty;
                    if (apiVersion >= 10)
                    {
                        topicId = kafkaReader.ReadUuid();
                    }
                    else
                    {
                        topicName = kafkaReader.ReadCompactString() ?? string.Empty;
                    }

                    var partitionCount = kafkaReader.ReadVarInt() - 1;
                    var partitions = new List<int>();
                    for (int p = 0; p < partitionCount; p++)
                    {
                        partitions.Add(kafkaReader.ReadInt32());
                    }
                    kafkaReader.ReadVarInt(); // Skip topic tagged fields
                    topics.Add(new TopicPartitionRequest { Topic = topicName, TopicId = topicId, PartitionIndexes = partitions });
                }
            }

            kafkaReader.ReadVarInt(); // Skip group tagged fields
            groups.Add(new OffsetFetchRequestGroup { GroupId = groupId, MemberId = memberId, MemberEpoch = memberEpoch, Topics = topics });
        }

        var requireStable = kafkaReader.ReadInt8() != 0;
        kafkaReader.ReadVarInt(); // Skip request tagged fields

        return new OffsetFetchRequest
        {
            ApiKey = ApiKey.OffsetFetch,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            GroupId = groups.Count > 0 ? groups[0].GroupId : string.Empty,
            Topics = groups.Count > 0 ? groups[0].Topics : null,
            Groups = groups,
            RequireStable = requireStable
        };
    }

    private static OffsetFetchRequest ReadFlexible(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remaining = new byte[stream.Length - stream.Position];
        stream.Read(remaining, 0, remaining.Length);
        var kafkaReader = new KafkaProtocolReader(remaining);

        var groupId = kafkaReader.ReadCompactString() ?? string.Empty;

        List<TopicPartitionRequest>? topics = null;
        var topicsLength = kafkaReader.ReadVarInt();
        if (topicsLength > 0)
        {
            topics = new List<TopicPartitionRequest>();
            for (int i = 0; i < topicsLength - 1; i++)
            {
                var topicName = kafkaReader.ReadCompactString() ?? string.Empty;
                var partitionCount = kafkaReader.ReadVarInt() - 1;
                var partitions = new List<int>();
                for (int j = 0; j < partitionCount; j++)
                {
                    partitions.Add(kafkaReader.ReadInt32());
                }
                kafkaReader.ReadVarInt(); // Skip topic tagged fields
                topics.Add(new TopicPartitionRequest { Topic = topicName, PartitionIndexes = partitions });
            }
        }

        var requireStable = apiVersion >= 7 && kafkaReader.ReadInt8() != 0;
        kafkaReader.ReadVarInt(); // Skip request tagged fields

        return new OffsetFetchRequest
        {
            ApiKey = ApiKey.OffsetFetch,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            GroupId = groupId,
            Topics = topics,
            RequireStable = requireStable
        };
    }

    private static OffsetFetchRequest ReadNonFlexible(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var groupId = BinaryHelpers.ReadString(reader);
        List<TopicPartitionRequest>? topics = null;

        var topicsCount = BinaryHelpers.ReadInt32BigEndian(reader);
        if (topicsCount >= 0)
        {
            topics = new List<TopicPartitionRequest>();
            for (int i = 0; i < topicsCount; i++)
            {
                var topicName = BinaryHelpers.ReadString(reader);
                var partitionCount = BinaryHelpers.ReadInt32BigEndian(reader);
                var partitions = new List<int>();
                for (int j = 0; j < partitionCount; j++)
                {
                    partitions.Add(BinaryHelpers.ReadInt32BigEndian(reader));
                }
                topics.Add(new TopicPartitionRequest { Topic = topicName, PartitionIndexes = partitions });
            }
        }

        return new OffsetFetchRequest
        {
            ApiKey = ApiKey.OffsetFetch,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            GroupId = groupId,
            Topics = topics,
            RequireStable = false
        };
    }
}

/// <summary>
/// Group entry for OffsetFetch v8+ multi-group requests
/// </summary>
public sealed class OffsetFetchRequestGroup
{
    public required string GroupId { get; init; }
    /// <summary>MemberId for KIP-848 consumer protocol (v9+)</summary>
    public string? MemberId { get; init; }
    /// <summary>MemberEpoch for KIP-848 consumer protocol (v9+), -1 if not used</summary>
    public int MemberEpoch { get; init; } = -1;
    public List<TopicPartitionRequest>? Topics { get; init; }
}

public sealed class TopicPartitionRequest
{
    /// <summary>Topic name (v1-9)</summary>
    public required string Topic { get; init; }
    /// <summary>Topic ID (v10+)</summary>
    public Guid TopicId { get; init; }
    public required List<int> PartitionIndexes { get; init; }
}

/// <summary>
/// OffsetFetch response
/// v1-7: Single group with Topics array
/// v8+: Multiple groups with Groups array
/// v10+: Uses TopicId (UUID) instead of topic names
/// </summary>
public sealed class OffsetFetchResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; }
    /// <summary>Topics for v1-7 responses</summary>
    public List<TopicPartitionOffset>? Topics { get; init; }
    /// <summary>Group-level error code (v2-7)</summary>
    public ErrorCode ErrorCode { get; init; }
    /// <summary>Groups array for v8+ responses</summary>
    public List<OffsetFetchResponseGroup>? Groups { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // Response Header
        writer.WriteInt32(CorrelationId);
        bool isFlexible = ApiVersion >= 6;
        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        if (ApiVersion >= 3)
        {
            writer.WriteInt32(ThrottleTimeMs);
        }

        if (ApiVersion >= 8)
        {
            // v8+: Groups array format
            var groups = Groups ?? [];
            writer.WriteVarInt(groups.Count + 1); // COMPACT_ARRAY
            foreach (var group in groups)
            {
                writer.WriteCompactString(group.GroupId);
                var topics = group.Topics ?? [];
                writer.WriteVarInt(topics.Count + 1);
                foreach (var topic in topics)
                {
                    // v10+: TopicId instead of topic name
                    if (ApiVersion >= 10)
                    {
                        writer.WriteUuid(topic.TopicId);
                    }
                    else
                    {
                        writer.WriteCompactString(topic.Topic);
                    }
                    writer.WriteVarInt(topic.Partitions.Count + 1);
                    foreach (var partition in topic.Partitions)
                    {
                        writer.WriteInt32(partition.PartitionIndex);
                        writer.WriteInt64(partition.CommittedOffset);
                        writer.WriteInt32(partition.CommittedLeaderEpoch);
                        writer.WriteCompactString(partition.Metadata);
                        writer.WriteInt16((short)partition.ErrorCode);
                        writer.WriteVarInt(0); // Partition tagged fields
                    }
                    writer.WriteVarInt(0); // Topic tagged fields
                }
                writer.WriteInt16((short)group.ErrorCode);
                writer.WriteVarInt(0); // Group tagged fields
            }
            writer.WriteVarInt(0); // Response body tagged fields
        }
        else if (isFlexible)
        {
            // v6-7: Flexible format with single group
            var topics = Topics ?? [];
            writer.WriteVarInt(topics.Count + 1);
            foreach (var topic in topics)
            {
                writer.WriteCompactString(topic.Topic);
                writer.WriteVarInt(topic.Partitions.Count + 1);
                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt32(partition.PartitionIndex);
                    writer.WriteInt64(partition.CommittedOffset);
                    if (ApiVersion >= 5)
                    {
                        writer.WriteInt32(partition.CommittedLeaderEpoch);
                    }
                    writer.WriteCompactString(partition.Metadata);
                    writer.WriteInt16((short)partition.ErrorCode);
                    writer.WriteVarInt(0); // Partition tagged fields
                }
                writer.WriteVarInt(0); // Topic tagged fields
            }
            writer.WriteInt16((short)ErrorCode);
            writer.WriteVarInt(0); // Response body tagged fields
        }
        else
        {
            // v1-5: Non-flexible format
            var topics = Topics ?? [];
            writer.WriteInt32(topics.Count);
            foreach (var topic in topics)
            {
                writer.WriteString(topic.Topic);
                writer.WriteInt32(topic.Partitions.Count);
                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt32(partition.PartitionIndex);
                    writer.WriteInt64(partition.CommittedOffset);
                    if (ApiVersion >= 5)
                    {
                        writer.WriteInt32(partition.CommittedLeaderEpoch);
                    }
                    writer.WriteString(partition.Metadata ?? string.Empty);
                    writer.WriteInt16((short)partition.ErrorCode);
                }
            }
            if (ApiVersion >= 2)
            {
                writer.WriteInt16((short)ErrorCode);
            }
        }
    }
}

/// <summary>
/// Group entry for OffsetFetch v8+ responses
/// </summary>
public sealed class OffsetFetchResponseGroup
{
    public required string GroupId { get; init; }
    public List<TopicPartitionOffset>? Topics { get; init; }
    public ErrorCode ErrorCode { get; init; }
}

public sealed class TopicPartitionOffset
{
    /// <summary>Topic name (v1-9)</summary>
    public required string Topic { get; init; }
    /// <summary>Topic ID (v10+)</summary>
    public Guid TopicId { get; init; }
    public required List<PartitionOffsetMetadata> Partitions { get; init; }
}

public sealed class PartitionOffsetMetadata
{
    public required int PartitionIndex { get; init; }
    public required long CommittedOffset { get; init; }
    public int CommittedLeaderEpoch { get; init; } = -1;
    public string? Metadata { get; init; }
    public required ErrorCode ErrorCode { get; init; }
}
