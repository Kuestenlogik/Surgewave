namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka AlterReplicaLogDirs request (API Key 34, v0-2).
/// Used to move replicas between log directories on a broker.
/// This enables administrators to balance disk usage across multiple log directories.
/// </summary>
public sealed class AlterReplicaLogDirsRequest : KafkaRequest
{
    /// <summary>The directories to add to.</summary>
    public required List<DirEntry> Dirs { get; init; }

    public sealed class DirEntry
    {
        /// <summary>The absolute directory path.</summary>
        public required string Path { get; init; }

        /// <summary>The topics to add to the directory.</summary>
        public required List<TopicEntry> Topics { get; init; }
    }

    public sealed class TopicEntry
    {
        /// <summary>The topic name.</summary>
        public required string Topic { get; init; }

        /// <summary>The partition indices.</summary>
        public required List<int> Partitions { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        var isFlexible = ApiVersion >= 2;

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

        // Dirs array
        if (isFlexible)
        {
            writer.WriteVarInt(Dirs.Count + 1);
        }
        else
        {
            writer.WriteInt32(Dirs.Count);
        }

        foreach (var dir in Dirs)
        {
            if (isFlexible)
            {
                writer.WriteCompactString(dir.Path);
                writer.WriteVarInt(dir.Topics.Count + 1);
            }
            else
            {
                writer.WriteString(dir.Path);
                writer.WriteInt32(dir.Topics.Count);
            }

            foreach (var topic in dir.Topics)
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
                    writer.WriteInt32(partition);
                }

                if (isFlexible)
                {
                    writer.WriteVarInt(0); // Topic tagged fields
                }
            }

            if (isFlexible)
            {
                writer.WriteVarInt(0); // Dir tagged fields
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static AlterReplicaLogDirsRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var isFlexible = apiVersion >= 2;

        var dirCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
        var dirs = new List<DirEntry>(dirCount);

        for (int i = 0; i < dirCount; i++)
        {
            var path = isFlexible ? reader.ReadCompactString() ?? "" : reader.ReadString() ?? "";
            var topicCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
            var topics = new List<TopicEntry>(topicCount);

            for (int j = 0; j < topicCount; j++)
            {
                var topicName = isFlexible ? reader.ReadCompactString() ?? "" : reader.ReadString() ?? "";
                var partitionCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
                var partitions = new List<int>(partitionCount);

                for (int k = 0; k < partitionCount; k++)
                {
                    partitions.Add(reader.ReadInt32());
                }

                if (isFlexible)
                {
                    reader.SkipTaggedFields();
                }

                topics.Add(new TopicEntry
                {
                    Topic = topicName,
                    Partitions = partitions
                });
            }

            if (isFlexible)
            {
                reader.SkipTaggedFields();
            }

            dirs.Add(new DirEntry
            {
                Path = path,
                Topics = topics
            });
        }

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new AlterReplicaLogDirsRequest
        {
            ApiKey = ApiKey.AlterReplicaLogDirs,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Dirs = dirs
        };
    }
}

/// <summary>
/// Kafka AlterReplicaLogDirs response (API Key 34, v0-2).
/// </summary>
public sealed class AlterReplicaLogDirsResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The results for each topic.</summary>
    public required List<TopicResult> Results { get; init; }

    public sealed class TopicResult
    {
        /// <summary>The topic name.</summary>
        public required string Topic { get; init; }

        /// <summary>The results for each partition.</summary>
        public required List<PartitionResult> Partitions { get; init; }
    }

    public sealed class PartitionResult
    {
        /// <summary>The partition index.</summary>
        public required int PartitionIndex { get; init; }

        /// <summary>The error code, or 0 if there was no error.</summary>
        public ErrorCode ErrorCode { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        var isFlexible = ApiVersion >= 2;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        writer.WriteInt32(ThrottleTimeMs);

        // Results array
        if (isFlexible)
        {
            writer.WriteVarInt(Results.Count + 1);
        }
        else
        {
            writer.WriteInt32(Results.Count);
        }

        foreach (var result in Results)
        {
            if (isFlexible)
            {
                writer.WriteCompactString(result.Topic);
                writer.WriteVarInt(result.Partitions.Count + 1);
            }
            else
            {
                writer.WriteString(result.Topic);
                writer.WriteInt32(result.Partitions.Count);
            }

            foreach (var partition in result.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt16((short)partition.ErrorCode);

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

    public static AlterReplicaLogDirsResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        var isFlexible = apiVersion >= 2;

        if (isFlexible)
        {
            reader.SkipTaggedFields(); // Response header tagged fields
        }

        var throttleTimeMs = reader.ReadInt32();

        var resultCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
        var results = new List<TopicResult>(resultCount);

        for (int i = 0; i < resultCount; i++)
        {
            var topicName = isFlexible ? reader.ReadCompactString() ?? "" : reader.ReadString() ?? "";
            var partitionCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
            var partitions = new List<PartitionResult>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                var partitionIndex = reader.ReadInt32();
                var errorCode = (ErrorCode)reader.ReadInt16();

                if (isFlexible)
                {
                    reader.SkipTaggedFields();
                }

                partitions.Add(new PartitionResult
                {
                    PartitionIndex = partitionIndex,
                    ErrorCode = errorCode
                });
            }

            if (isFlexible)
            {
                reader.SkipTaggedFields();
            }

            results.Add(new TopicResult
            {
                Topic = topicName,
                Partitions = partitions
            });
        }

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new AlterReplicaLogDirsResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            Results = results
        };
    }
}
