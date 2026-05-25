namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka ElectLeaders request (API Key 43, v0-2).
/// Triggers leader election for specified partitions.
/// </summary>
public sealed class ElectLeadersRequest : KafkaRequest
{
    /// <summary>
    /// Type of elections to conduct (v1+).
    /// 0 = preferred replica election
    /// 1 = unclean election (first live replica if no in-sync replicas)
    /// </summary>
    public sbyte ElectionType { get; init; }

    /// <summary>
    /// The topic partitions to elect leaders for.
    /// If null, elect for all partitions.
    /// </summary>
    public List<TopicPartitions>? TopicPartitionsList { get; init; }

    /// <summary>The time in ms to wait for the election to complete.</summary>
    public int TimeoutMs { get; init; } = 60000;

    public sealed class TopicPartitions
    {
        /// <summary>The topic name.</summary>
        public required string Topic { get; init; }

        /// <summary>The partition indices to elect leaders for.</summary>
        public required List<int> Partitions { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

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

        // ElectionType (v1+)
        if (ApiVersion >= 1)
        {
            writer.WriteInt8(ElectionType);
        }

        // TopicPartitions (nullable)
        if (isFlexible)
        {
            if (TopicPartitionsList == null)
            {
                writer.WriteVarInt(0); // Null compact array
            }
            else
            {
                writer.WriteVarInt(TopicPartitionsList.Count + 1);
                foreach (var tp in TopicPartitionsList)
                {
                    writer.WriteCompactString(tp.Topic);
                    writer.WriteVarInt(tp.Partitions.Count + 1);
                    foreach (var partition in tp.Partitions)
                    {
                        writer.WriteInt32(partition);
                    }
                    writer.WriteVarInt(0); // Topic tagged fields
                }
            }
        }
        else
        {
            if (TopicPartitionsList == null)
            {
                writer.WriteInt32(-1); // Null array
            }
            else
            {
                writer.WriteInt32(TopicPartitionsList.Count);
                foreach (var tp in TopicPartitionsList)
                {
                    writer.WriteString(tp.Topic);
                    writer.WriteInt32(tp.Partitions.Count);
                    foreach (var partition in tp.Partitions)
                    {
                        writer.WriteInt32(partition);
                    }
                }
            }
        }

        writer.WriteInt32(TimeoutMs);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static ElectLeadersRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 2;

        sbyte electionType = apiVersion >= 1 ? reader.ReadInt8() : (sbyte)0;

        List<TopicPartitions>? topicPartitions = null;

        if (isFlexible)
        {
            var count = reader.ReadVarInt() - 1;
            if (count >= 0)
            {
                topicPartitions = new List<TopicPartitions>(count);
                for (int i = 0; i < count; i++)
                {
                    var topic = reader.ReadCompactString() ?? "";
                    var partitionCount = reader.ReadVarInt() - 1;
                    var partitions = new List<int>(partitionCount);
                    for (int j = 0; j < partitionCount; j++)
                    {
                        partitions.Add(reader.ReadInt32());
                    }
                    reader.SkipTaggedFields();

                    topicPartitions.Add(new TopicPartitions
                    {
                        Topic = topic,
                        Partitions = partitions
                    });
                }
            }
        }
        else
        {
            var count = reader.ReadInt32();
            if (count >= 0)
            {
                topicPartitions = new List<TopicPartitions>(count);
                for (int i = 0; i < count; i++)
                {
                    var topic = reader.ReadString() ?? "";
                    var partitionCount = reader.ReadInt32();
                    var partitions = new List<int>(partitionCount);
                    for (int j = 0; j < partitionCount; j++)
                    {
                        partitions.Add(reader.ReadInt32());
                    }

                    topicPartitions.Add(new TopicPartitions
                    {
                        Topic = topic,
                        Partitions = partitions
                    });
                }
            }
        }

        var timeoutMs = reader.ReadInt32();

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new ElectLeadersRequest
        {
            ApiKey = ApiKey.ElectLeaders,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            ElectionType = electionType,
            TopicPartitionsList = topicPartitions,
            TimeoutMs = timeoutMs
        };
    }
}

/// <summary>
/// Kafka ElectLeaders response (API Key 43, v0-2).
/// </summary>
public sealed class ElectLeadersResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The top level response error code (v1+).</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The election results.</summary>
    public required List<ReplicaElectionResult> ReplicaElectionResults { get; init; }

    public sealed class ReplicaElectionResult
    {
        /// <summary>The topic name.</summary>
        public required string Topic { get; init; }

        /// <summary>The results for each partition.</summary>
        public required List<PartitionResult> PartitionResults { get; init; }
    }

    public sealed class PartitionResult
    {
        /// <summary>The partition id.</summary>
        public required int PartitionId { get; init; }

        /// <summary>The result error, or zero if there was no error.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The result message, or null if there was no error.</summary>
        public string? ErrorMessage { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        writer.WriteInt32(ThrottleTimeMs);

        if (ApiVersion >= 1)
        {
            writer.WriteInt16((short)ErrorCode);
        }

        if (isFlexible)
        {
            writer.WriteVarInt(ReplicaElectionResults.Count + 1);
            foreach (var result in ReplicaElectionResults)
            {
                writer.WriteCompactString(result.Topic);
                writer.WriteVarInt(result.PartitionResults.Count + 1);
                foreach (var pr in result.PartitionResults)
                {
                    writer.WriteInt32(pr.PartitionId);
                    writer.WriteInt16((short)pr.ErrorCode);
                    writer.WriteCompactString(pr.ErrorMessage);
                    writer.WriteVarInt(0); // Partition tagged fields
                }
                writer.WriteVarInt(0); // Topic tagged fields
            }
            writer.WriteVarInt(0); // Body tagged fields
        }
        else
        {
            writer.WriteInt32(ReplicaElectionResults.Count);
            foreach (var result in ReplicaElectionResults)
            {
                writer.WriteString(result.Topic);
                writer.WriteInt32(result.PartitionResults.Count);
                foreach (var pr in result.PartitionResults)
                {
                    writer.WriteInt32(pr.PartitionId);
                    writer.WriteInt16((short)pr.ErrorCode);
                    writer.WriteString(pr.ErrorMessage);
                }
            }
        }
    }

    public static ElectLeadersResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        bool isFlexible = apiVersion >= 2;

        if (isFlexible)
        {
            reader.SkipTaggedFields(); // Response header tagged fields
        }

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = apiVersion >= 1 ? (ErrorCode)reader.ReadInt16() : ErrorCode.None;

        List<ReplicaElectionResult> results;

        if (isFlexible)
        {
            var resultCount = reader.ReadVarInt() - 1;
            results = new List<ReplicaElectionResult>(resultCount);

            for (int i = 0; i < resultCount; i++)
            {
                var topic = reader.ReadCompactString() ?? "";
                var partitionCount = reader.ReadVarInt() - 1;
                var partitionResults = new List<PartitionResult>(partitionCount);

                for (int j = 0; j < partitionCount; j++)
                {
                    partitionResults.Add(new PartitionResult
                    {
                        PartitionId = reader.ReadInt32(),
                        ErrorCode = (ErrorCode)reader.ReadInt16(),
                        ErrorMessage = reader.ReadCompactString()
                    });
                    reader.SkipTaggedFields();
                }
                reader.SkipTaggedFields();

                results.Add(new ReplicaElectionResult
                {
                    Topic = topic,
                    PartitionResults = partitionResults
                });
            }
            reader.SkipTaggedFields();
        }
        else
        {
            var resultCount = reader.ReadInt32();
            results = new List<ReplicaElectionResult>(resultCount);

            for (int i = 0; i < resultCount; i++)
            {
                var topic = reader.ReadString() ?? "";
                var partitionCount = reader.ReadInt32();
                var partitionResults = new List<PartitionResult>(partitionCount);

                for (int j = 0; j < partitionCount; j++)
                {
                    partitionResults.Add(new PartitionResult
                    {
                        PartitionId = reader.ReadInt32(),
                        ErrorCode = (ErrorCode)reader.ReadInt16(),
                        ErrorMessage = reader.ReadString()
                    });
                }

                results.Add(new ReplicaElectionResult
                {
                    Topic = topic,
                    PartitionResults = partitionResults
                });
            }
        }

        return new ElectLeadersResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ReplicaElectionResults = results
        };
    }
}
