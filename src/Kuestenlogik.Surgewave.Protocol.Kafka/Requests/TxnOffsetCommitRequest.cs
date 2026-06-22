namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// TxnOffsetCommit request — commits consumer offsets as part of a transaction.
/// Pairs with AddOffsetsToTxn so consumer-group offsets land atomically with
/// any produced messages when the transaction commits.
///
/// At v0-5 the topic is identified by its name (compact string). KIP-1319
/// (v6) replaces Name with a TopicId (uuid); the broker resolves the UUID via
/// its log manager and returns UNKNOWN_TOPIC_ID when unresolvable. The model
/// here carries BOTH fields and the wire methods pick the right one per
/// version so callers don't have to branch.
/// </summary>
public sealed class TxnOffsetCommitRequest : KafkaRequest
{
    public required string TransactionalId { get; init; }
    public required string GroupId { get; init; }
    public required long ProducerId { get; init; }
    public required short ProducerEpoch { get; init; }
    public int? GenerationId { get; init; }  // v3+
    public string? MemberId { get; init; }    // v3+
    public string? GroupInstanceId { get; init; }  // v3+
    public required List<TxnOffsetCommitTopic> Topics { get; init; }

    public sealed class TxnOffsetCommitTopic
    {
        /// <summary>Topic name (v0-5). Null at v6+ on the wire; populated server-side after TopicId resolution.</summary>
        public string? Name { get; init; }

        /// <summary>Topic UUID (v6+). <see cref="Guid.Empty"/> at v0-5.</summary>
        public Guid TopicId { get; init; }

        public required List<TxnOffsetCommitPartition> Partitions { get; init; }
    }

    public sealed class TxnOffsetCommitPartition
    {
        public required int Partition { get; init; }
        public required long CommittedOffset { get; init; }
        public int? CommittedLeaderEpoch { get; init; }  // v2+
        public string? Metadata { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.TxnOffsetCommit, ApiVersion);

        if (isFlexible)
        {
            writer.WriteCompactString(TransactionalId);
            writer.WriteCompactString(GroupId);
        }
        else
        {
            writer.WriteString(TransactionalId);
            writer.WriteString(GroupId);
        }

        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);

        // v3+ fields
        if (ApiVersion >= 3)
        {
            writer.WriteInt32(GenerationId ?? -1);
            if (isFlexible)
            {
                writer.WriteCompactString(MemberId ?? string.Empty);
                writer.WriteCompactString(GroupInstanceId);
            }
            else
            {
                writer.WriteString(MemberId ?? string.Empty);
                writer.WriteString(GroupInstanceId);
            }
        }

        // Topics array
        if (isFlexible)
        {
            writer.WriteVarInt(Topics.Count + 1);
            foreach (var topic in Topics)
            {
                if (ApiVersion >= 6)
                {
                    writer.WriteUuid(topic.TopicId);
                }
                else
                {
                    writer.WriteCompactString(topic.Name);
                }
                writer.WriteVarInt(topic.Partitions.Count + 1);
                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt32(partition.Partition);
                    writer.WriteInt64(partition.CommittedOffset);
                    if (ApiVersion >= 2)
                    {
                        writer.WriteInt32(partition.CommittedLeaderEpoch ?? -1);
                    }
                    writer.WriteCompactString(partition.Metadata);
                    writer.WriteVarInt(0); // Partition tagged fields
                }
                writer.WriteVarInt(0); // Topic tagged fields
            }
            writer.WriteVarInt(0); // Body tagged fields
        }
        else
        {
            writer.WriteInt32(Topics.Count);
            foreach (var topic in Topics)
            {
                // Non-flexible versions are <= v2 so always Name-based.
                writer.WriteString(topic.Name);
                writer.WriteInt32(topic.Partitions.Count);
                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt32(partition.Partition);
                    writer.WriteInt64(partition.CommittedOffset);
                    if (ApiVersion >= 2)
                    {
                        writer.WriteInt32(partition.CommittedLeaderEpoch ?? -1);
                    }
                    writer.WriteString(partition.Metadata);
                }
            }
        }
    }

    public static TxnOffsetCommitRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.TxnOffsetCommit, apiVersion);
        string transactionalId;
        string groupId;
        long producerId;
        short producerEpoch;
        int? generationId = null;
        string? memberId = null;
        string? groupInstanceId = null;
        var topics = new List<TxnOffsetCommitTopic>();

        if (isFlexible)
        {
            var stream = (MemoryStream)reader.BaseStream;
            var remainingBytes = new byte[stream.Length - stream.Position];
            stream.ReadExactly(remainingBytes, 0, remainingBytes.Length);
            var protocolReader = new KafkaProtocolReader(remainingBytes);

            transactionalId = protocolReader.ReadCompactString() ?? string.Empty;
            groupId = protocolReader.ReadCompactString() ?? string.Empty;
            producerId = protocolReader.ReadInt64();
            producerEpoch = protocolReader.ReadInt16();

            if (apiVersion >= 3)
            {
                generationId = protocolReader.ReadInt32();
                memberId = protocolReader.ReadCompactString();
                groupInstanceId = protocolReader.ReadCompactString();
            }

            var topicCount = protocolReader.ReadVarInt() - 1;
            for (int i = 0; i < topicCount; i++)
            {
                string? topicName = null;
                Guid topicId = Guid.Empty;
                if (apiVersion >= 6)
                {
                    topicId = protocolReader.ReadUuid();
                }
                else
                {
                    topicName = protocolReader.ReadCompactString() ?? string.Empty;
                }

                var partitionCount = protocolReader.ReadVarInt() - 1;
                var partitions = new List<TxnOffsetCommitPartition>();

                for (int j = 0; j < partitionCount; j++)
                {
                    var partition = protocolReader.ReadInt32();
                    var committedOffset = protocolReader.ReadInt64();
                    int? committedLeaderEpoch = null;
                    if (apiVersion >= 2)
                    {
                        committedLeaderEpoch = protocolReader.ReadInt32();
                    }
                    var metadata = protocolReader.ReadCompactString();
                    protocolReader.SkipTaggedFields(); // partition tagged fields

                    partitions.Add(new TxnOffsetCommitPartition
                    {
                        Partition = partition,
                        CommittedOffset = committedOffset,
                        CommittedLeaderEpoch = committedLeaderEpoch,
                        Metadata = metadata
                    });
                }

                protocolReader.SkipTaggedFields(); // topic tagged fields

                topics.Add(new TxnOffsetCommitTopic
                {
                    Name = topicName,
                    TopicId = topicId,
                    Partitions = partitions
                });
            }

            protocolReader.SkipTaggedFields(); // request tagged fields
        }
        else
        {
            var length = BinaryHelpers.ReadInt16BigEndian(reader);
            transactionalId = length < 0 ? string.Empty : System.Text.Encoding.UTF8.GetString(reader.ReadBytes(length));
            var groupLength = BinaryHelpers.ReadInt16BigEndian(reader);
            groupId = groupLength < 0 ? string.Empty : System.Text.Encoding.UTF8.GetString(reader.ReadBytes(groupLength));
            producerId = BinaryHelpers.ReadInt64BigEndian(reader);
            producerEpoch = BinaryHelpers.ReadInt16BigEndian(reader);

            if (apiVersion >= 3)
            {
                generationId = BinaryHelpers.ReadInt32BigEndian(reader);
                var memberIdLength = BinaryHelpers.ReadInt16BigEndian(reader);
                memberId = memberIdLength < 0 ? null : System.Text.Encoding.UTF8.GetString(reader.ReadBytes(memberIdLength));
                var groupInstanceIdLength = BinaryHelpers.ReadInt16BigEndian(reader);
                groupInstanceId = groupInstanceIdLength < 0 ? null : System.Text.Encoding.UTF8.GetString(reader.ReadBytes(groupInstanceIdLength));
            }

            var topicCount = BinaryHelpers.ReadInt32BigEndian(reader);
            for (int i = 0; i < topicCount; i++)
            {
                var topicLength = BinaryHelpers.ReadInt16BigEndian(reader);
                var topicName = System.Text.Encoding.UTF8.GetString(reader.ReadBytes(topicLength));
                var partitionCount = BinaryHelpers.ReadInt32BigEndian(reader);
                var partitions = new List<TxnOffsetCommitPartition>();

                for (int j = 0; j < partitionCount; j++)
                {
                    var partition = BinaryHelpers.ReadInt32BigEndian(reader);
                    var committedOffset = BinaryHelpers.ReadInt64BigEndian(reader);
                    int? committedLeaderEpoch = null;
                    if (apiVersion >= 2)
                    {
                        committedLeaderEpoch = BinaryHelpers.ReadInt32BigEndian(reader);
                    }
                    var metadataLength = BinaryHelpers.ReadInt16BigEndian(reader);
                    var metadata = metadataLength < 0 ? null : System.Text.Encoding.UTF8.GetString(reader.ReadBytes(metadataLength));

                    partitions.Add(new TxnOffsetCommitPartition
                    {
                        Partition = partition,
                        CommittedOffset = committedOffset,
                        CommittedLeaderEpoch = committedLeaderEpoch,
                        Metadata = metadata
                    });
                }

                topics.Add(new TxnOffsetCommitTopic
                {
                    Name = topicName,
                    TopicId = Guid.Empty,
                    Partitions = partitions
                });
            }
        }

        return new TxnOffsetCommitRequest
        {
            ApiKey = ApiKey.TxnOffsetCommit,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            TransactionalId = transactionalId,
            GroupId = groupId,
            ProducerId = producerId,
            ProducerEpoch = producerEpoch,
            GenerationId = generationId,
            MemberId = memberId,
            GroupInstanceId = groupInstanceId,
            Topics = topics
        };
    }
}

/// <summary>
/// TxnOffsetCommit response. KIP-1319 (v6) replaces topic Name with TopicId
/// on the response side too. The model carries both fields so a coordinator
/// can populate Name (its working identifier) and TopicId (the wire identity
/// echoed from the request) independently.
/// </summary>
public sealed class TxnOffsetCommitResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; }
    public required List<TxnOffsetCommitTopicResult> Topics { get; init; }

    public sealed class TxnOffsetCommitTopicResult
    {
        /// <summary>Topic name (v0-5). Null at v6+ on the wire.</summary>
        public string? Name { get; init; }

        /// <summary>Topic UUID (v6+). <see cref="Guid.Empty"/> at v0-5.</summary>
        public Guid TopicId { get; init; }

        public required List<TxnOffsetCommitPartitionResult> Partitions { get; init; }
    }

    public sealed class TxnOffsetCommitPartitionResult
    {
        public required int Partition { get; init; }
        public required ErrorCode ErrorCode { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 3;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        writer.WriteInt32(ThrottleTimeMs);

        if (isFlexible)
        {
            writer.WriteVarInt(Topics.Count + 1);
            foreach (var topic in Topics)
            {
                if (ApiVersion >= 6)
                {
                    writer.WriteUuid(topic.TopicId);
                }
                else
                {
                    writer.WriteCompactString(topic.Name);
                }
                writer.WriteVarInt(topic.Partitions.Count + 1);
                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt32(partition.Partition);
                    writer.WriteInt16((short)partition.ErrorCode);
                    writer.WriteVarInt(0); // Partition tagged fields
                }
                writer.WriteVarInt(0); // Topic tagged fields
            }
            writer.WriteVarInt(0); // Body tagged fields
        }
        else
        {
            writer.WriteInt32(Topics.Count);
            foreach (var topic in Topics)
            {
                writer.WriteString(topic.Name);
                writer.WriteInt32(topic.Partitions.Count);
                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt32(partition.Partition);
                    writer.WriteInt16((short)partition.ErrorCode);
                }
            }
        }
    }
}
