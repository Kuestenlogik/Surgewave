namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// ListOffsets request - Get the offset for a given timestamp
/// Kafka 4.0 supports versions 1-11. Version 0 was removed.
///
/// Version history:
/// - v1: Removes MaxNumOffsets, returns single offset
/// - v2: Adds isolation level for transactional reads
/// - v4: Adds current leader epoch for fencing
/// - v6: Enables flexible versions
/// - v7: Enables listing by max timestamp (KIP-734)
/// - v8: Enables listing by local log start offset (KIP-405 tiered storage)
/// - v9: Enables listing by last tiered offset (KIP-1005)
/// - v10: Adds TimeoutMs for async remote storage (KIP-1075)
/// - v11: Enables listing by earliest pending upload offset (KIP-1023)
/// </summary>
public sealed class ListOffsetsRequest : KafkaRequest
{
    /// <summary>Special timestamp constants for offset queries</summary>
    public static class TimestampType
    {
        /// <summary>Get the latest offset (log end offset or stable offset based on isolation level)</summary>
        public const long Latest = -1;
        /// <summary>Get the earliest offset (log start offset)</summary>
        public const long Earliest = -2;
        /// <summary>Get the offset with max timestamp (v7+, KIP-734)</summary>
        public const long MaxTimestamp = -3;
        /// <summary>Get the earliest local log start offset (v8+, KIP-405 tiered storage)</summary>
        public const long EarliestLocalTimestamp = -4;
        /// <summary>Get the last tiered offset (v9+, KIP-1005)</summary>
        public const long LastTieredOffset = -5;
        /// <summary>Get the earliest pending upload offset (v11+, KIP-1023)</summary>
        public const long EarliestPendingUploadOffset = -6;
    }

    public required int ReplicaId { get; init; } // -1 for consumer
    public required byte IsolationLevel { get; init; } // 0 = READ_UNCOMMITTED, 1 = READ_COMMITTED (v2+)
    public required List<TopicPartitionTimestamp> Topics { get; init; }
    /// <summary>Timeout in milliseconds for async remote storage (v10+, KIP-1075)</summary>
    public int TimeoutMs { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        writer.WriteInt32(ReplicaId);

        if (ApiVersion >= 2)
        {
            writer.WriteInt8((sbyte)IsolationLevel);
        }

        // Write topics array
        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.ListOffsets, ApiVersion);
        if (isFlexible)
        {
            writer.WriteCompactArray(Topics.ToArray(), (topic) =>
            {
                writer.WriteCompactString(topic.Topic);
                writer.WriteCompactArray(topic.Partitions.ToArray(), (partition) =>
                {
                    writer.WriteInt32(partition.PartitionIndex);
                    // CurrentLeaderEpoch (v4+)
                    if (ApiVersion >= 4)
                    {
                        writer.WriteInt32(partition.CurrentLeaderEpoch);
                    }
                    writer.WriteInt64(partition.Timestamp);
                    writer.WriteVarInt(0); // Tagged fields
                });
                writer.WriteVarInt(0); // Tagged fields
            });
            // TimeoutMs (v10+)
            if (ApiVersion >= 10)
            {
                writer.WriteInt32(TimeoutMs);
            }
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
                    // CurrentLeaderEpoch (v4+)
                    if (ApiVersion >= 4)
                    {
                        writer.WriteInt32(partition.CurrentLeaderEpoch);
                    }
                    writer.WriteInt64(partition.Timestamp);
                }
            }
        }
    }

    public static ListOffsetsRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        int replicaId = BinaryHelpers.ReadInt32BigEndian(reader);
        byte isolationLevel = 0;

        if (apiVersion >= 2)
        {
            isolationLevel = reader.ReadByte();
        }

        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.ListOffsets, apiVersion);
        var topics = new List<TopicPartitionTimestamp>();
        int timeoutMs = 0;

        int topicCount;
        if (isFlexible)
        {
            var stream = (MemoryStream)reader.BaseStream;
            var remaining = new byte[stream.Length - stream.Position];
            stream.Read(remaining, 0, remaining.Length);
            var kafkaReader = new KafkaProtocolReader(remaining);

            topicCount = kafkaReader.ReadVarInt() - 1;
            for (int i = 0; i < topicCount; i++)
            {
                var topicName = kafkaReader.ReadCompactString();
                var partitionCount = kafkaReader.ReadVarInt() - 1;
                var partitions = new List<PartitionTimestamp>();

                for (int j = 0; j < partitionCount; j++)
                {
                    var partitionIndex = kafkaReader.ReadInt32();
                    int currentLeaderEpoch = -1;

                    // CurrentLeaderEpoch (v4+)
                    if (apiVersion >= 4)
                    {
                        currentLeaderEpoch = kafkaReader.ReadInt32();
                    }

                    long timestamp = kafkaReader.ReadInt64();

                    partitions.Add(new PartitionTimestamp
                    {
                        PartitionIndex = partitionIndex,
                        CurrentLeaderEpoch = currentLeaderEpoch,
                        Timestamp = timestamp
                    });

                    kafkaReader.ReadVarInt(); // Skip tagged fields
                }

                topics.Add(new TopicPartitionTimestamp
                {
                    Topic = topicName ?? string.Empty,
                    Partitions = partitions
                });

                kafkaReader.ReadVarInt(); // Skip tagged fields
            }

            // TimeoutMs (v10+)
            if (apiVersion >= 10)
            {
                timeoutMs = kafkaReader.ReadInt32();
            }

            kafkaReader.ReadVarInt(); // Skip request tagged fields
            stream.Position = stream.Length - kafkaReader.Remaining;
        }
        else
        {
            topicCount = BinaryHelpers.ReadInt32BigEndian(reader);
            for (int i = 0; i < topicCount; i++)
            {
                var topicName = BinaryHelpers.ReadString(reader);
                var partitionCount = BinaryHelpers.ReadInt32BigEndian(reader);
                var partitions = new List<PartitionTimestamp>();

                for (int j = 0; j < partitionCount; j++)
                {
                    var partitionIndex = BinaryHelpers.ReadInt32BigEndian(reader);
                    int currentLeaderEpoch = -1;

                    // CurrentLeaderEpoch (v4+)
                    if (apiVersion >= 4)
                    {
                        currentLeaderEpoch = BinaryHelpers.ReadInt32BigEndian(reader);
                    }

                    long timestamp = BinaryHelpers.ReadInt64BigEndian(reader);

                    partitions.Add(new PartitionTimestamp
                    {
                        PartitionIndex = partitionIndex,
                        CurrentLeaderEpoch = currentLeaderEpoch,
                        Timestamp = timestamp
                    });
                }

                topics.Add(new TopicPartitionTimestamp
                {
                    Topic = topicName,
                    Partitions = partitions
                });
            }
        }

        return new ListOffsetsRequest
        {
            ApiKey = ApiKey.ListOffsets,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            ReplicaId = replicaId,
            IsolationLevel = isolationLevel,
            Topics = topics,
            TimeoutMs = timeoutMs
        };
    }

}

public sealed class TopicPartitionTimestamp
{
    public required string Topic { get; init; }
    public required List<PartitionTimestamp> Partitions { get; init; }
}

public sealed class PartitionTimestamp
{
    public required int PartitionIndex { get; init; }
    /// <summary>The current leader epoch (v4+), -1 if not specified</summary>
    public int CurrentLeaderEpoch { get; init; } = -1;
    /// <summary>
    /// The timestamp to query. Use ListOffsetsRequest.TimestampType constants:
    /// -1 = latest, -2 = earliest, -3 = max timestamp (v7+), -4 = local log start (v8+),
    /// -5 = last tiered offset (v9+), -6 = earliest pending upload offset (v11+)
    /// </summary>
    public required long Timestamp { get; init; }
}

/// <summary>
/// ListOffsets response
/// </summary>
public sealed class ListOffsetsResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; }
    public required List<TopicPartitionOffsets> Topics { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 6;

        // Response header
        writer.WriteInt32(CorrelationId);

        // Response header tagged fields (v6+)
        if (isFlexible)
        {
            writer.WriteVarInt(0); // No header tagged fields
        }

        if (ApiVersion >= 2)
        {
            writer.WriteInt32(ThrottleTimeMs);
        }
        if (isFlexible)
        {
            writer.WriteCompactArray(Topics.ToArray(), (topic) =>
            {
                writer.WriteCompactString(topic.Topic);
                writer.WriteCompactArray(topic.Partitions.ToArray(), (partition) =>
                {
                    writer.WriteInt32(partition.PartitionIndex);
                    writer.WriteInt16((short)partition.ErrorCode);

                    if (ApiVersion >= 1)
                    {
                        writer.WriteInt64(partition.Timestamp);
                        writer.WriteInt64(partition.Offset);
                    }
                    else
                    {
                        // V0 returns an array of offsets
                        writer.WriteInt32(1); // Array length
                        writer.WriteInt64(partition.Offset);
                    }

                    if (ApiVersion >= 4)
                    {
                        writer.WriteInt32(partition.LeaderEpoch);
                    }

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

                    if (ApiVersion >= 1)
                    {
                        writer.WriteInt64(partition.Timestamp);
                        writer.WriteInt64(partition.Offset);
                    }
                    else
                    {
                        // V0 returns an array of offsets
                        writer.WriteInt32(1); // Array length
                        writer.WriteInt64(partition.Offset);
                    }

                    if (ApiVersion >= 4)
                    {
                        writer.WriteInt32(partition.LeaderEpoch);
                    }
                }
            }
        }
    }
}

public sealed class TopicPartitionOffsets
{
    public required string Topic { get; init; }
    public required List<PartitionOffsetInfo> Partitions { get; init; }
}

public sealed class PartitionOffsetInfo
{
    public required int PartitionIndex { get; init; }
    public required ErrorCode ErrorCode { get; init; }
    public required long Timestamp { get; init; }
    public required long Offset { get; init; }
    public int LeaderEpoch { get; init; } = -1;
}
