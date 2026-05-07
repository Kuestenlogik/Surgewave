namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka DeleteRecords request (API Key 21)
/// Deletes records from a topic-partition up to a specified offset.
/// Used for GDPR compliance and storage management.
/// </summary>
public sealed class DeleteRecordsRequest : KafkaRequest
{
    public required List<TopicPartitions> Topics { get; init; }
    public required int TimeoutMs { get; init; }

    public sealed class TopicPartitions
    {
        public required string Topic { get; init; }
        public required List<PartitionOffset> Partitions { get; init; }
    }

    public sealed class PartitionOffset
    {
        public required int Partition { get; init; }
        /// <summary>
        /// Delete all records with offset less than this value.
        /// Use -1 to delete all records (up to high watermark).
        /// </summary>
        public required long Offset { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        writer.WriteInt32(Topics.Count);
        foreach (var topic in Topics)
        {
            writer.WriteString(topic.Topic);
            writer.WriteInt32(topic.Partitions.Count);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.Partition);
                writer.WriteInt64(partition.Offset);
            }
        }

        writer.WriteInt32(TimeoutMs);
    }

    public static DeleteRecordsRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 2;
        var topics = new List<TopicPartitions>();

        if (isFlexible)
        {
            var stream = (MemoryStream)reader.BaseStream;
            var remainingBytes = new byte[stream.Length - stream.Position];
            stream.ReadExactly(remainingBytes, 0, remainingBytes.Length);
            var protocolReader = new KafkaProtocolReader(remainingBytes);

            var topicCount = protocolReader.ReadVarInt() - 1;
            for (int i = 0; i < topicCount; i++)
            {
                var topicName = protocolReader.ReadCompactString()!;
                var partitionCount = protocolReader.ReadVarInt() - 1;
                var partitions = new List<PartitionOffset>();

                for (int j = 0; j < partitionCount; j++)
                {
                    var partition = protocolReader.ReadInt32();
                    var offset = protocolReader.ReadInt64();
                    protocolReader.ReadVarInt(); // partition tagged fields
                    partitions.Add(new PartitionOffset { Partition = partition, Offset = offset });
                }

                protocolReader.ReadVarInt(); // topic tagged fields
                topics.Add(new TopicPartitions { Topic = topicName, Partitions = partitions });
            }

            var timeoutMs = protocolReader.ReadInt32();
            protocolReader.ReadVarInt(); // request tagged fields

            return new DeleteRecordsRequest
            {
                ApiKey = ApiKey.DeleteRecords,
                ApiVersion = apiVersion,
                CorrelationId = correlationId,
                ClientId = clientId,
                Topics = topics,
                TimeoutMs = timeoutMs
            };
        }
        else
        {
            // Non-flexible format (v0-v1)
            var topicCount = BinaryHelpers.ReadInt32BigEndian(reader);
            for (int i = 0; i < topicCount; i++)
            {
                var topicName = BinaryHelpers.ReadString(reader);
                var partitionCount = BinaryHelpers.ReadInt32BigEndian(reader);
                var partitions = new List<PartitionOffset>();

                for (int j = 0; j < partitionCount; j++)
                {
                    var partition = BinaryHelpers.ReadInt32BigEndian(reader);
                    var offset = BinaryHelpers.ReadInt64BigEndian(reader);
                    partitions.Add(new PartitionOffset { Partition = partition, Offset = offset });
                }

                topics.Add(new TopicPartitions { Topic = topicName, Partitions = partitions });
            }

            var timeoutMs = BinaryHelpers.ReadInt32BigEndian(reader);

            return new DeleteRecordsRequest
            {
                ApiKey = ApiKey.DeleteRecords,
                ApiVersion = apiVersion,
                CorrelationId = correlationId,
                ClientId = clientId,
                Topics = topics,
                TimeoutMs = timeoutMs
            };
        }
    }
}

/// <summary>
/// Kafka DeleteRecords response
/// </summary>
public sealed class DeleteRecordsResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; }
    public required List<TopicResult> Topics { get; init; }

    public sealed class TopicResult
    {
        public required string Topic { get; init; }
        public required List<PartitionResult> Partitions { get; init; }
    }

    public sealed class PartitionResult
    {
        public required int Partition { get; init; }
        /// <summary>
        /// The new log start offset (lowest readable offset after deletion).
        /// </summary>
        public required long LowWatermark { get; init; }
        public required ErrorCode ErrorCode { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // response header tagged fields
        }

        writer.WriteInt32(ThrottleTimeMs);

        if (isFlexible)
        {
            writer.WriteVarInt(Topics.Count + 1); // COMPACT_ARRAY
            foreach (var topic in Topics)
            {
                writer.WriteCompactString(topic.Topic);
                writer.WriteVarInt(topic.Partitions.Count + 1); // COMPACT_ARRAY
                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt32(partition.Partition);
                    writer.WriteInt64(partition.LowWatermark);
                    writer.WriteInt16((short)partition.ErrorCode);
                    writer.WriteVarInt(0); // partition tagged fields
                }
                writer.WriteVarInt(0); // topic tagged fields
            }
            writer.WriteVarInt(0); // response tagged fields
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
                    writer.WriteInt32(partition.Partition);
                    writer.WriteInt64(partition.LowWatermark);
                    writer.WriteInt16((short)partition.ErrorCode);
                }
            }
        }
    }
}
