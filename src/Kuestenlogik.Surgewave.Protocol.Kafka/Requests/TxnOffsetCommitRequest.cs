namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// TxnOffsetCommit request - Commits consumer offsets as part of a transaction.
/// This is the second part of exactly-once consumer semantics - after AddOffsetsToTxn
/// registers the consumer group, TxnOffsetCommit actually commits the offsets atomically
/// with any produced messages when the transaction commits.
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
    public required Dictionary<string, List<TxnOffsetCommitPartition>> Topics { get; init; }

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
            foreach (var (topic, partitions) in Topics)
            {
                writer.WriteCompactString(topic);
                writer.WriteVarInt(partitions.Count + 1);
                foreach (var partition in partitions)
                {
                    writer.WriteInt32(partition.Partition);
                    writer.WriteInt64(partition.CommittedOffset);
                    if (ApiVersion >= 2)
                    {
                        writer.WriteInt32(partition.CommittedLeaderEpoch ?? -1);
                    }
                    writer.WriteCompactString(partition.Metadata);
                    writer.WriteVarInt(0); // Tagged fields
                }
                writer.WriteVarInt(0); // Tagged fields
            }
            writer.WriteVarInt(0); // Tagged fields
        }
        else
        {
            writer.WriteInt32(Topics.Count);
            foreach (var (topic, partitions) in Topics)
            {
                writer.WriteString(topic);
                writer.WriteInt32(partitions.Count);
                foreach (var partition in partitions)
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
        var topics = new Dictionary<string, List<TxnOffsetCommitPartition>>();

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
                var topic = protocolReader.ReadCompactString() ?? string.Empty;
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

                    // Skip partition tagged fields
                    var tagCount = protocolReader.ReadVarInt();
                    for (int t = 0; t < tagCount; t++)
                    {
                        protocolReader.ReadVarInt();
                        var size = protocolReader.ReadVarInt();
                        protocolReader.Skip(size);
                    }

                    partitions.Add(new TxnOffsetCommitPartition
                    {
                        Partition = partition,
                        CommittedOffset = committedOffset,
                        CommittedLeaderEpoch = committedLeaderEpoch,
                        Metadata = metadata
                    });
                }

                // Skip topic tagged fields
                var topicTagCount = protocolReader.ReadVarInt();
                for (int t = 0; t < topicTagCount; t++)
                {
                    protocolReader.ReadVarInt();
                    var size = protocolReader.ReadVarInt();
                    protocolReader.Skip(size);
                }

                topics[topic] = partitions;
            }

            // Skip request tagged fields
            var requestTagCount = protocolReader.ReadVarInt();
            for (int t = 0; t < requestTagCount; t++)
            {
                protocolReader.ReadVarInt();
                var size = protocolReader.ReadVarInt();
                protocolReader.Skip(size);
            }
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
                var topic = System.Text.Encoding.UTF8.GetString(reader.ReadBytes(topicLength));
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

                topics[topic] = partitions;
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
/// TxnOffsetCommit response
/// </summary>
public sealed class TxnOffsetCommitResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; }
    public required Dictionary<string, List<TxnOffsetCommitPartitionResult>> Topics { get; init; }

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
            foreach (var (topic, partitions) in Topics)
            {
                writer.WriteCompactString(topic);
                writer.WriteVarInt(partitions.Count + 1);
                foreach (var partition in partitions)
                {
                    writer.WriteInt32(partition.Partition);
                    writer.WriteInt16((short)partition.ErrorCode);
                    writer.WriteVarInt(0); // Tagged fields
                }
                writer.WriteVarInt(0); // Tagged fields
            }
            writer.WriteVarInt(0); // Tagged fields
        }
        else
        {
            writer.WriteInt32(Topics.Count);
            foreach (var (topic, partitions) in Topics)
            {
                writer.WriteString(topic);
                writer.WriteInt32(partitions.Count);
                foreach (var partition in partitions)
                {
                    writer.WriteInt32(partition.Partition);
                    writer.WriteInt16((short)partition.ErrorCode);
                }
            }
        }
    }
}
