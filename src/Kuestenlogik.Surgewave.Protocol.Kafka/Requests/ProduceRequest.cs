using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka Produce request (v3-11, v12-13 for Kafka 4.0)
/// v12: Transaction V2 protocol support
/// v13: Uses TopicId (UUID) instead of topic name
/// </summary>
public sealed class ProduceRequest : KafkaRequest
{
    /// <summary>Transactional ID (v3+), null if not transactional</summary>
    public string? TransactionalId { get; init; }
    public required short RequiredAcks { get; init; }
    public required int TimeoutMs { get; init; }
    public required List<TopicProduceData> TopicData { get; init; }

    public sealed class TopicProduceData
    {
        /// <summary>Topic name (v0-12)</summary>
        public string? Name { get; init; }
        /// <summary>Topic ID (v13+)</summary>
        public Guid TopicId { get; init; }
        public required List<PartitionProduceData> PartitionData { get; init; }
    }

    public sealed class PartitionProduceData
    {
        public required int Index { get; init; }
        /// <summary>
        /// Raw Kafka RecordBatch bytes. When parsed from a pooled request buffer via
        /// the zero-copy path, this is a <see cref="ReadOnlyMemory{T}"/> slice into
        /// the pooled array — no allocation, no copy. The memory is valid until
        /// <c>ProcessKafkaRequestsAsync</c> returns the buffer to the pool.
        /// </summary>
        public required ReadOnlyMemory<byte> Records { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 9;
        bool usesTopicId = ApiVersion >= 13;

        // Header
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

        // TransactionalId (v3+)
        if (ApiVersion >= 3)
        {
            if (isFlexible)
                writer.WriteCompactString(TransactionalId);
            else
                writer.WriteString(TransactionalId);
        }

        // Body
        writer.WriteInt16(RequiredAcks);
        writer.WriteInt32(TimeoutMs);

        // Topic data
        if (isFlexible)
            writer.WriteVarInt(TopicData.Count + 1);
        else
            writer.WriteInt32(TopicData.Count);

        foreach (var topic in TopicData)
        {
            if (usesTopicId)
            {
                writer.WriteUuid(topic.TopicId);
            }
            else if (isFlexible)
            {
                writer.WriteCompactString(topic.Name);
            }
            else
            {
                writer.WriteString(topic.Name);
            }

            // Partitions
            if (isFlexible)
                writer.WriteVarInt(topic.PartitionData.Count + 1);
            else
                writer.WriteInt32(topic.PartitionData.Count);

            foreach (var partition in topic.PartitionData)
            {
                writer.WriteInt32(partition.Index);

                if (isFlexible)
                {
                    writer.WriteVarInt(partition.Records.Length + 1);
                    writer.WriteRaw(partition.Records.Span);
                    writer.WriteVarInt(0); // Partition tagged fields
                }
                else
                {
                    writer.WriteInt32(partition.Records.Length);
                    writer.WriteRaw(partition.Records.Span);
                }
            }

            // Topic tagged fields
            if (isFlexible)
                writer.WriteVarInt(0);
        }

        // Body tagged fields
        if (isFlexible)
            writer.WriteVarInt(0);
    }

    public static ProduceRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        // Zero-copy: reuse the underlying MemoryStream buffer directly instead of
        // copying all remaining bytes into a new array. The buffer is owned by the
        // pooled request read pipeline (ReadRequestRetainingBufferAsync) and lives
        // until the finally block in ProcessKafkaRequestsAsync returns it to the pool.
        var stream = (System.IO.MemoryStream)reader.BaseStream;
        var position = (int)stream.Position;
        var remainingSize = (int)(stream.Length - position);
        KafkaProtocolReader protocolReader;
        if (stream.TryGetBuffer(out var segment))
        {
            // Fast path: direct view into the stream's internal buffer — no copy.
            protocolReader = new KafkaProtocolReader(segment.Array!, segment.Offset + position, remainingSize);
        }
        else
        {
            // Fallback for non-exposable MemoryStreams (shouldn't happen in production).
            var remainingBytes = reader.ReadBytes(remainingSize);
            protocolReader = new KafkaProtocolReader(remainingBytes);
        }

        return ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    /// <summary>
    /// Hot-path overload: parses straight out of the caller's (pooled) request buffer.
    /// The Records slices point into that buffer and stay valid until the read pipeline
    /// returns it to the pool.
    /// </summary>
    public static ProduceRequest ReadFrom(KafkaProtocolReader protocolReader, short apiVersion, int correlationId, string clientId)
    {
        // Produce v9+ uses flexible format (COMPACT_ARRAY + COMPACT_STRING)
        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.Produce, apiVersion);
        bool usesTopicId = apiVersion >= 13;

        // TransactionalId (v3+, nullable)
        string? transactionalId = null;
        if (apiVersion >= ProtocolVersions.Features.Produce.TransactionalIdVersion)
        {
            transactionalId = isFlexible ? protocolReader.ReadCompactString() : protocolReader.ReadString();
        }

        // Acks (int16)
        var requiredAcks = protocolReader.ReadInt16();

        // TimeoutMs (int32)
        var timeoutMs = protocolReader.ReadInt32();

        // TopicData
        var topicCount = isFlexible ? protocolReader.ReadVarInt() - 1 : protocolReader.ReadInt32();
        // Clamped: a corrupt/hostile count must not turn into a huge pre-allocation. Real requests
        // are far below the cap, so they still get an exact-size list.
        var topicData = new List<TopicProduceData>(Math.Clamp(topicCount, 0, 1024));

        for (int i = 0; i < topicCount; i++)
        {
            string? topicName = null;
            Guid topicId = Guid.Empty;

            if (usesTopicId)
            {
                topicId = protocolReader.ReadUuid();
            }
            else if (isFlexible)
            {
                topicName = protocolReader.ReadCompactString();
            }
            else
            {
                topicName = protocolReader.ReadString();
            }

            // Partitions
            var partitionCount = isFlexible ? protocolReader.ReadVarInt() - 1 : protocolReader.ReadInt32();
            var partitions = new List<PartitionProduceData>(Math.Clamp(partitionCount, 0, 1024));

            for (int j = 0; j < partitionCount; j++)
            {
                var partition = protocolReader.ReadInt32();

                // RecordSet — zero-copy: return a Memory<byte> slice into the pooled
                // request buffer instead of allocating + copying. The memory is valid
                // until ProcessKafkaRequestsAsync returns the buffer to the pool.
                ReadOnlyMemory<byte> recordBatch;
                if (isFlexible)
                {
                    recordBatch = protocolReader.ReadCompactBytesMemory();

                    // Tagged fields for partition
                    var partitionTagCount = protocolReader.ReadVarInt();
                    for (int k = 0; k < partitionTagCount; k++)
                    {
                        var tag = protocolReader.ReadVarInt();
                        var size = protocolReader.ReadVarInt();
                        protocolReader.Skip(size);
                    }
                }
                else
                {
                    var batchSize = protocolReader.ReadInt32();
                    recordBatch = protocolReader.ReadBytesFixedMemory(batchSize);
                }

                partitions.Add(new PartitionProduceData
                {
                    Index = partition,
                    Records = recordBatch
                });
            }

            // Tagged fields for topic
            if (isFlexible)
            {
                var topicTagCount = protocolReader.ReadVarInt();
                for (int k = 0; k < topicTagCount; k++)
                {
                    var tag = protocolReader.ReadVarInt();
                    var size = protocolReader.ReadVarInt();
                    protocolReader.Skip(size);
                }
            }

            topicData.Add(new TopicProduceData
            {
                Name = topicName,
                TopicId = topicId,
                PartitionData = partitions
            });
        }

        // Tagged fields for request body
        if (isFlexible)
        {
            var tagCount = protocolReader.ReadVarInt();
            for (int i = 0; i < tagCount; i++)
            {
                var tag = protocolReader.ReadVarInt();
                var size = protocolReader.ReadVarInt();
                protocolReader.Skip(size);
            }
        }

        return new ProduceRequest
        {
            ApiKey = ApiKey.Produce,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            TransactionalId = transactionalId,
            RequiredAcks = requiredAcks,
            TimeoutMs = timeoutMs,
            TopicData = topicData
        };
    }

}

/// <summary>
/// Kafka Produce response (v3-11, v12-13 for Kafka 4.0)
/// v10+: NodeEndpoints tagged field support
/// v12: Transaction V2 protocol support
/// v13: Uses TopicId (UUID) instead of topic name
/// </summary>
public sealed class ProduceResponse : KafkaResponse
{
    public required List<TopicProduceResponse> Responses { get; init; }
    public required int ThrottleTimeMs { get; init; }

    /// <summary>
    /// Broker endpoint information for direct connections (v10+ tagged field).
    /// Allows clients to connect directly to partition leaders.
    /// </summary>
    public List<NodeEndpoint>? NodeEndpoints { get; init; }

    public sealed class TopicProduceResponse
    {
        /// <summary>Topic name (v0-12)</summary>
        public string? Name { get; init; }
        /// <summary>Topic ID (v13+)</summary>
        public Guid TopicId { get; init; }
        public required List<PartitionProduceResponse> PartitionResponses { get; init; }
    }

    public sealed class PartitionProduceResponse
    {
        public required int Index { get; init; }
        public required ErrorCode ErrorCode { get; init; }
        public required long BaseOffset { get; init; }
        public long LogAppendTimeMs { get; init; } = -1;
        public long LogStartOffset { get; init; } = -1;

        /// <summary>
        /// Current leader information for this partition (v10+ tagged field).
        /// Allows clients to refresh metadata more efficiently on NOT_LEADER_OR_FOLLOWER errors.
        /// </summary>
        public LeaderInfo? CurrentLeader { get; init; }
    }

    /// <summary>
    /// Leader information for a partition.
    /// </summary>
    public sealed class LeaderInfo
    {
        /// <summary>The ID of the current leader, or -1 if unknown.</summary>
        public required int LeaderId { get; init; }
        /// <summary>The latest known leader epoch.</summary>
        public required int LeaderEpoch { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // Response Header
        writer.WriteInt32(CorrelationId);

        bool isFlexible = ApiVersion >= 9;
        bool usesTopicId = ApiVersion >= 13;

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields (empty)
        }

        // Responses
        if (isFlexible)
        {
            writer.WriteVarInt(Responses.Count + 1);
        }
        else
        {
            writer.WriteInt32(Responses.Count);
        }

        foreach (var topicResponse in Responses)
        {
            if (usesTopicId)
            {
                writer.WriteUuid(topicResponse.TopicId);
            }
            else if (isFlexible)
            {
                writer.WriteCompactString(topicResponse.Name);
            }
            else
            {
                writer.WriteString(topicResponse.Name);
            }

            // Partitions
            if (isFlexible)
            {
                writer.WriteVarInt(topicResponse.PartitionResponses.Count + 1);
            }
            else
            {
                writer.WriteInt32(topicResponse.PartitionResponses.Count);
            }

            foreach (var partition in topicResponse.PartitionResponses)
            {
                writer.WriteInt32(partition.Index);
                writer.WriteInt16((short)partition.ErrorCode);
                writer.WriteInt64(partition.BaseOffset);

                // LogAppendTimeMs (v2+)
                if (ApiVersion >= 2)
                {
                    writer.WriteInt64(partition.LogAppendTimeMs);
                }

                // LogStartOffset (v5+)
                if (ApiVersion >= 5)
                {
                    writer.WriteInt64(partition.LogStartOffset);
                }

                // RecordErrors (v8+) - empty array
                if (ApiVersion >= 8)
                {
                    if (isFlexible)
                    {
                        writer.WriteVarInt(1); // COMPACT_ARRAY with 0 elements
                    }
                    else
                    {
                        writer.WriteInt32(0);
                    }
                }

                // ErrorMessage (v8+) - null
                if (ApiVersion >= 8)
                {
                    if (isFlexible)
                    {
                        writer.WriteVarInt(0); // Null compact string
                    }
                    else
                    {
                        writer.WriteInt16(-1); // Null string
                    }
                }

                // Partition tagged fields (v10+: CurrentLeader = tag 0)
                if (isFlexible)
                {
                    if (ApiVersion >= 10 && partition.CurrentLeader != null)
                    {
                        writer.WriteVarInt(1); // 1 tagged field
                        writer.WriteVarInt(0); // Tag 0 = CurrentLeader

                        // CurrentLeader: LeaderId (int32) + LeaderEpoch (int32) + no nested tags
                        using var leaderWriter = new KafkaProtocolWriter(16);
                        leaderWriter.WriteInt32(partition.CurrentLeader.LeaderId);
                        leaderWriter.WriteInt32(partition.CurrentLeader.LeaderEpoch);
                        leaderWriter.WriteVarInt(0); // No nested tagged fields

                        var leaderSpan = leaderWriter.WrittenSpan;
                        writer.WriteVarInt(leaderSpan.Length);
                        writer.WriteRaw(leaderSpan);
                    }
                    else
                    {
                        writer.WriteVarInt(0);
                    }
                }
            }

            // Topic tagged fields
            if (isFlexible)
            {
                writer.WriteVarInt(0);
            }
        }

        // ThrottleTimeMs (v1+)
        if (ApiVersion >= 1)
        {
            writer.WriteInt32(ThrottleTimeMs);
        }

        // Response body tagged fields (flexible only)
        // Tag 0 = NodeEndpoints (v10+)
        if (isFlexible)
        {
            if (ApiVersion >= 10 && NodeEndpoints is { Count: > 0 })
            {
                // Write tag 0 = NodeEndpoints
                writer.WriteVarInt(1); // 1 tagged field
                writer.WriteVarInt(0); // Tag 0

                // Calculate field size for NodeEndpoints
                using var sizeWriter = new KafkaProtocolWriter(256);
                sizeWriter.WriteVarInt(NodeEndpoints.Count + 1);
                foreach (var endpoint in NodeEndpoints)
                {
                    sizeWriter.WriteInt32(endpoint.NodeId);
                    sizeWriter.WriteCompactString(endpoint.Host);
                    sizeWriter.WriteInt32(endpoint.Port);
                    sizeWriter.WriteCompactString(endpoint.Rack); // Nullable compact string
                    sizeWriter.WriteVarInt(0); // No endpoint tagged fields
                }
                var endpointsSpan = sizeWriter.WrittenSpan;
                writer.WriteVarInt(endpointsSpan.Length);
                writer.WriteRaw(endpointsSpan);
            }
            else
            {
                writer.WriteVarInt(0);
            }
        }
    }
}
