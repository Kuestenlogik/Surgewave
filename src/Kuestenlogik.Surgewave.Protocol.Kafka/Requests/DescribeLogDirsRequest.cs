namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka DescribeLogDirs request (API Key 35, v0-4).
/// Used to describe the state of log directories on brokers.
/// Returns information about disk usage and replica assignments per log directory.
/// </summary>
public sealed class DescribeLogDirsRequest : KafkaRequest
{
    /// <summary>
    /// Topics to describe, or null to describe all topics.
    /// </summary>
    public List<TopicRequest>? Topics { get; init; }

    public sealed class TopicRequest
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

        // Topics array (nullable)
        if (Topics == null)
        {
            if (isFlexible)
            {
                writer.WriteVarInt(0); // Null compact array
            }
            else
            {
                writer.WriteInt32(-1); // Null array
            }
        }
        else
        {
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
                    writer.WriteInt32(partition);
                }

                if (isFlexible)
                {
                    writer.WriteVarInt(0); // Topic tagged fields
                }
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static DescribeLogDirsRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var isFlexible = apiVersion >= 2;

        List<TopicRequest>? topics = null;
        var topicCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();

        if (topicCount >= 0)
        {
            topics = new List<TopicRequest>(topicCount);
            for (int i = 0; i < topicCount; i++)
            {
                var topicName = isFlexible ? reader.ReadCompactString() ?? "" : reader.ReadString() ?? "";
                var partitionCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
                var partitions = new List<int>(partitionCount);

                for (int j = 0; j < partitionCount; j++)
                {
                    partitions.Add(reader.ReadInt32());
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
        }

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new DescribeLogDirsRequest
        {
            ApiKey = ApiKey.DescribeLogDirs,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Topics = topics
        };
    }
}

/// <summary>
/// Kafka DescribeLogDirs response (API Key 35, v0-4).
/// </summary>
public sealed class DescribeLogDirsResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The error code, or 0 if there was no error (v3+).</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The log directories.</summary>
    public required List<LogDirResult> Results { get; init; }

    public sealed class LogDirResult
    {
        /// <summary>The error code, or 0 if there was no error.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The absolute log directory path.</summary>
        public required string LogDir { get; init; }

        /// <summary>Each topic.</summary>
        public required List<TopicResult> Topics { get; init; }

        /// <summary>The total size in bytes of the volume the log directory is in (v4+).</summary>
        public long TotalBytes { get; init; } = -1;

        /// <summary>The usable size in bytes of the volume the log directory is in (v4+).</summary>
        public long UsableBytes { get; init; } = -1;
    }

    public sealed class TopicResult
    {
        /// <summary>The topic name.</summary>
        public required string Topic { get; init; }

        /// <summary>Each partition.</summary>
        public required List<PartitionResult> Partitions { get; init; }
    }

    public sealed class PartitionResult
    {
        /// <summary>The partition index.</summary>
        public required int PartitionIndex { get; init; }

        /// <summary>The size of the log segments in this partition in bytes.</summary>
        public long Size { get; init; }

        /// <summary>
        /// The lag of the log's LEO w.r.t. partition's HW (if it is the current log for the partition)
        /// or current replica's LEO (if it is the future log for the partition).
        /// </summary>
        public long OffsetLag { get; init; }

        /// <summary>True if this log is created by AlterReplicaLogDirsRequest and will replace the current log (v1+).</summary>
        public bool IsFutureKey { get; init; }
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

        // v3+: ErrorCode at top level
        if (ApiVersion >= 3)
        {
            writer.WriteInt16((short)ErrorCode);
        }

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
            writer.WriteInt16((short)result.ErrorCode);

            if (isFlexible)
            {
                writer.WriteCompactString(result.LogDir);
                writer.WriteVarInt(result.Topics.Count + 1);
            }
            else
            {
                writer.WriteString(result.LogDir);
                writer.WriteInt32(result.Topics.Count);
            }

            foreach (var topic in result.Topics)
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
                    writer.WriteInt64(partition.Size);
                    writer.WriteInt64(partition.OffsetLag);

                    // v1+: IsFutureKey
                    if (ApiVersion >= 1)
                    {
                        writer.WriteBoolean(partition.IsFutureKey);
                    }

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

            // v4+: TotalBytes and UsableBytes
            if (ApiVersion >= 4)
            {
                writer.WriteInt64(result.TotalBytes);
                writer.WriteInt64(result.UsableBytes);
            }

            if (isFlexible)
            {
                writer.WriteVarInt(0); // LogDir tagged fields
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static DescribeLogDirsResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        var isFlexible = apiVersion >= 2;

        if (isFlexible)
        {
            reader.SkipTaggedFields(); // Response header tagged fields
        }

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = apiVersion >= 3 ? (ErrorCode)reader.ReadInt16() : ErrorCode.None;

        var resultCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
        var results = new List<LogDirResult>(resultCount);

        for (int i = 0; i < resultCount; i++)
        {
            var logDirErrorCode = (ErrorCode)reader.ReadInt16();
            var logDir = isFlexible ? reader.ReadCompactString() ?? "" : reader.ReadString() ?? "";

            var topicCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
            var topics = new List<TopicResult>(topicCount);

            for (int j = 0; j < topicCount; j++)
            {
                var topicName = isFlexible ? reader.ReadCompactString() ?? "" : reader.ReadString() ?? "";
                var partitionCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
                var partitions = new List<PartitionResult>(partitionCount);

                for (int k = 0; k < partitionCount; k++)
                {
                    var partitionIndex = reader.ReadInt32();
                    var size = reader.ReadInt64();
                    var offsetLag = reader.ReadInt64();
                    var isFutureKey = apiVersion >= 1 && reader.ReadBoolean();

                    if (isFlexible)
                    {
                        reader.SkipTaggedFields();
                    }

                    partitions.Add(new PartitionResult
                    {
                        PartitionIndex = partitionIndex,
                        Size = size,
                        OffsetLag = offsetLag,
                        IsFutureKey = isFutureKey
                    });
                }

                if (isFlexible)
                {
                    reader.SkipTaggedFields();
                }

                topics.Add(new TopicResult
                {
                    Topic = topicName,
                    Partitions = partitions
                });
            }

            var totalBytes = apiVersion >= 4 ? reader.ReadInt64() : -1L;
            var usableBytes = apiVersion >= 4 ? reader.ReadInt64() : -1L;

            if (isFlexible)
            {
                reader.SkipTaggedFields();
            }

            results.Add(new LogDirResult
            {
                ErrorCode = logDirErrorCode,
                LogDir = logDir,
                Topics = topics,
                TotalBytes = totalBytes,
                UsableBytes = usableBytes
            });
        }

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new DescribeLogDirsResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            Results = results
        };
    }
}
