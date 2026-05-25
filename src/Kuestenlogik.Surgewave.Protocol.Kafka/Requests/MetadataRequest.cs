namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka Metadata request (v0-13)
/// </summary>
public sealed class MetadataRequest : KafkaRequest
{
    public required List<MetadataRequestTopic>? Topics { get; init; } // null means all topics
    /// <summary>Allow auto topic creation (v4+)</summary>
    public bool AllowAutoTopicCreation { get; init; } = true;
    /// <summary>Include topic authorized operations (v8+)</summary>
    public bool IncludeTopicAuthorizedOperations { get; init; }

    public sealed class MetadataRequestTopic
    {
        /// <summary>Topic ID (v10+)</summary>
        public Guid TopicId { get; init; }
        /// <summary>Topic name (nullable for v10+ when using TopicId)</summary>
        public string? Name { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 9;

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

        // Topics array
        if (Topics == null)
        {
            if (isFlexible)
                writer.WriteVarInt(0); // null compact array
            else
                writer.WriteInt32(-1);
        }
        else
        {
            if (isFlexible)
            {
                writer.WriteVarInt(Topics.Count + 1);
                foreach (var topic in Topics)
                {
                    if (ApiVersion >= 10)
                        writer.WriteUuid(topic.TopicId);
                    writer.WriteCompactString(topic.Name);
                    writer.WriteVarInt(0); // Topic tagged fields
                }
            }
            else
            {
                writer.WriteInt32(Topics.Count);
                foreach (var topic in Topics)
                {
                    writer.WriteString(topic.Name);
                }
            }
        }

        // AllowAutoTopicCreation (v4+)
        if (ApiVersion >= 4)
            writer.WriteInt8((sbyte)(AllowAutoTopicCreation ? 1 : 0));

        // IncludeClusterAuthorizedOperations (v8-10, deprecated in v11+)
        if (ApiVersion >= 8 && ApiVersion <= 10)
            writer.WriteInt8(0);

        // IncludeTopicAuthorizedOperations (v8+)
        if (ApiVersion >= 8)
            writer.WriteInt8((sbyte)(IncludeTopicAuthorizedOperations ? 1 : 0));

        // Body tagged fields
        if (isFlexible)
            writer.WriteVarInt(0);
    }

    public static MetadataRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        List<MetadataRequestTopic>? topics = null;
        bool allowAutoTopicCreation = true;
        bool includeTopicAuthorizedOperations = false;

        // Metadata v9+ uses flexible format (COMPACT_ARRAY + COMPACT_STRING)
        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.Metadata, apiVersion);

        // Get remaining bytes for KafkaProtocolReader
        var stream = (System.IO.MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);
        var protocolReader = new KafkaProtocolReader(remainingBytes);

        // Topics array
        int topicCount;
        if (isFlexible)
        {
            topicCount = protocolReader.ReadVarInt() - 1; // COMPACT_ARRAY: -1 means null
        }
        else
        {
            topicCount = protocolReader.ReadInt32();
        }

        if (topicCount >= 0)
        {
            topics = new List<MetadataRequestTopic>();
            for (int i = 0; i < topicCount; i++)
            {
                Guid topicId = Guid.Empty;
                string? topicName;

                // For v10+, each topic has a TopicId (UUID) followed by Name
                if (apiVersion >= 10)
                {
                    topicId = protocolReader.ReadUuid();
                }

                topicName = isFlexible ? protocolReader.ReadCompactString() : protocolReader.ReadString();

                // Read tagged fields for each topic (v9+)
                if (isFlexible)
                {
                    var topicTagCount = protocolReader.ReadVarInt();
                    for (int j = 0; j < topicTagCount; j++)
                    {
                        var tag = protocolReader.ReadVarInt();
                        var size = protocolReader.ReadVarInt();
                        protocolReader.Skip(size);
                    }
                }

                topics.Add(new MetadataRequestTopic
                {
                    TopicId = topicId,
                    Name = topicName
                });
            }
        }

        // AllowAutoTopicCreation (v4+)
        if (apiVersion >= 4)
            allowAutoTopicCreation = protocolReader.ReadInt8() != 0;

        // IncludeClusterAuthorizedOperations (v8-10, deprecated in v11+)
        if (apiVersion >= 8 && apiVersion <= 10)
            protocolReader.ReadInt8(); // Skip

        // IncludeTopicAuthorizedOperations (v8+)
        if (apiVersion >= 8)
            includeTopicAuthorizedOperations = protocolReader.ReadInt8() != 0;

        // Body tagged fields
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

        return new MetadataRequest
        {
            ApiKey = ApiKey.Metadata,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Topics = topics,
            AllowAutoTopicCreation = allowAutoTopicCreation,
            IncludeTopicAuthorizedOperations = includeTopicAuthorizedOperations
        };
    }

}

/// <summary>
/// Kafka Metadata response (v0-13)
/// </summary>
public sealed class MetadataResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; }
    public required List<MetadataResponseBroker> Brokers { get; init; }
    public required string? ClusterId { get; init; }
    public required int ControllerId { get; init; }
    public required List<MetadataResponseTopic> Topics { get; init; }
    /// <summary>Top-level error code (v13+)</summary>
    public ErrorCode ErrorCode { get; init; } = ErrorCode.None;

    public sealed class MetadataResponseBroker
    {
        public required int NodeId { get; init; }
        public required string Host { get; init; }
        public required int Port { get; init; }
        public string? Rack { get; init; }
    }

    public sealed class MetadataResponseTopic
    {
        public required ErrorCode ErrorCode { get; init; }
        public required string? Name { get; init; }
        /// <summary>Topic ID (v10+)</summary>
        public Guid TopicId { get; init; }
        public bool IsInternal { get; init; }
        public required List<MetadataResponsePartition> Partitions { get; init; }
        /// <summary>Authorized operations bitfield (v8+)</summary>
        public int TopicAuthorizedOperations { get; init; } = int.MinValue;
    }

    public sealed class MetadataResponsePartition
    {
        public required ErrorCode ErrorCode { get; init; }
        public required int PartitionIndex { get; init; }
        public required int LeaderId { get; init; }
        /// <summary>Leader epoch (v7+)</summary>
        public int LeaderEpoch { get; init; } = -1;
        public required List<int> ReplicaNodes { get; init; }
        public required List<int> IsrNodes { get; init; }
        /// <summary>Offline replicas (v5+)</summary>
        public List<int>? OfflineReplicas { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // Response Header v1 for flexible versions (v9+)
        writer.WriteInt32(CorrelationId);
        bool isFlexible = ApiVersion >= 9;

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields (empty)
        }

        // ThrottleTimeMs (v3+)
        if (ApiVersion >= 3)
        {
            writer.WriteInt32(ThrottleTimeMs);
        }

        // Brokers
        if (isFlexible)
        {
            writer.WriteVarInt(Brokers.Count + 1); // COMPACT_ARRAY
            foreach (var broker in Brokers)
            {
                writer.WriteInt32(broker.NodeId);
                writer.WriteCompactString(broker.Host);
                writer.WriteInt32(broker.Port);
                // Rack (v1+, nullable)
                if (ApiVersion >= 1)
                {
                    writer.WriteCompactString(broker.Rack);
                }
                writer.WriteVarInt(0); // Broker tagged fields (empty)
            }
        }
        else
        {
            writer.WriteInt32(Brokers.Count);
            foreach (var broker in Brokers)
            {
                writer.WriteInt32(broker.NodeId);
                writer.WriteString(broker.Host);
                writer.WriteInt32(broker.Port);
                // Rack (v1+, nullable)
                if (ApiVersion >= 1)
                {
                    writer.WriteString(broker.Rack);
                }
            }
        }

        // Cluster ID (v2+)
        if (ApiVersion >= 2)
        {
            if (isFlexible)
            {
                writer.WriteCompactString(ClusterId);
            }
            else
            {
                writer.WriteString(ClusterId);
            }
        }

        // Controller ID (v1+)
        if (ApiVersion >= 1)
        {
            writer.WriteInt32(ControllerId);
        }

        // Topics
        if (isFlexible)
        {
            writer.WriteVarInt(Topics.Count + 1); // COMPACT_ARRAY
            foreach (var topic in Topics)
            {
                writer.WriteInt16((short)topic.ErrorCode);
                writer.WriteCompactString(topic.Name);

                // TopicId (v10+)
                if (ApiVersion >= 10)
                {
                    writer.WriteUuid(topic.TopicId);
                }

                // IsInternal (v1+)
                if (ApiVersion >= 1)
                {
                    writer.WriteInt8((sbyte)(topic.IsInternal ? 1 : 0));
                }

                // Partitions - COMPACT_ARRAY
                writer.WriteVarInt(topic.Partitions.Count + 1);
                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt16((short)partition.ErrorCode);
                    writer.WriteInt32(partition.PartitionIndex);
                    writer.WriteInt32(partition.LeaderId);

                    // LeaderEpoch (v7+)
                    if (ApiVersion >= 7)
                    {
                        writer.WriteInt32(partition.LeaderEpoch);
                    }

                    // ReplicaNodes - COMPACT_ARRAY
                    writer.WriteVarInt(partition.ReplicaNodes.Count + 1);
                    foreach (var replica in partition.ReplicaNodes)
                    {
                        writer.WriteInt32(replica);
                    }

                    // IsrNodes - COMPACT_ARRAY
                    writer.WriteVarInt(partition.IsrNodes.Count + 1);
                    foreach (var isr in partition.IsrNodes)
                    {
                        writer.WriteInt32(isr);
                    }

                    // OfflineReplicas (v5+)
                    if (ApiVersion >= 5)
                    {
                        var offline = partition.OfflineReplicas ?? [];
                        writer.WriteVarInt(offline.Count + 1);
                        foreach (var replica in offline)
                        {
                            writer.WriteInt32(replica);
                        }
                    }

                    writer.WriteVarInt(0); // Partition tagged fields (empty)
                }

                // TopicAuthorizedOperations (v8+)
                if (ApiVersion >= 8)
                {
                    writer.WriteInt32(topic.TopicAuthorizedOperations);
                }

                writer.WriteVarInt(0); // Topic tagged fields (empty)
            }
        }
        else
        {
            writer.WriteInt32(Topics.Count);
            foreach (var topic in Topics)
            {
                writer.WriteInt16((short)topic.ErrorCode);
                writer.WriteString(topic.Name);

                // IsInternal (v1+)
                if (ApiVersion >= 1)
                {
                    writer.WriteInt8((sbyte)(topic.IsInternal ? 1 : 0));
                }

                writer.WriteInt32(topic.Partitions.Count);
                foreach (var partition in topic.Partitions)
                {
                    writer.WriteInt16((short)partition.ErrorCode);
                    writer.WriteInt32(partition.PartitionIndex);
                    writer.WriteInt32(partition.LeaderId);

                    // LeaderEpoch (v7+)
                    if (ApiVersion >= 7)
                    {
                        writer.WriteInt32(partition.LeaderEpoch);
                    }

                    writer.WriteInt32(partition.ReplicaNodes.Count);
                    foreach (var replica in partition.ReplicaNodes)
                    {
                        writer.WriteInt32(replica);
                    }

                    writer.WriteInt32(partition.IsrNodes.Count);
                    foreach (var isr in partition.IsrNodes)
                    {
                        writer.WriteInt32(isr);
                    }

                    // OfflineReplicas (v5+)
                    if (ApiVersion >= 5)
                    {
                        var offline = partition.OfflineReplicas ?? [];
                        writer.WriteInt32(offline.Count);
                        foreach (var replica in offline)
                        {
                            writer.WriteInt32(replica);
                        }
                    }
                }

                // TopicAuthorizedOperations (v8+)
                if (ApiVersion >= 8)
                {
                    writer.WriteInt32(topic.TopicAuthorizedOperations);
                }
            }
        }

        // ClusterAuthorizedOperations (v8-10)
        if (ApiVersion >= 8 && ApiVersion <= 10)
        {
            writer.WriteInt32(int.MinValue);
        }

        // ErrorCode (v13+)
        if (ApiVersion >= 13)
        {
            writer.WriteInt16((short)ErrorCode);
        }

        // Response body tagged fields (flexible only)
        if (isFlexible)
        {
            writer.WriteVarInt(0); // Empty tagged fields
        }
    }

    /// <summary>
    /// Deserialize a MetadataResponse from a buffer.
    /// </summary>
    public static MetadataResponse ReadFrom(ReadOnlySpan<byte> buffer, short apiVersion, int correlationId)
    {
        var reader = new KafkaProtocolReader(buffer.ToArray());
        bool isFlexible = apiVersion >= 9;

        // Skip response header tagged fields (flexible only)
        if (isFlexible)
        {
            var headerTagCount = reader.ReadVarInt();
            for (int i = 0; i < headerTagCount; i++)
            {
                reader.ReadVarInt(); // tag
                var size = reader.ReadVarInt();
                reader.Skip(size);
            }
        }

        // ThrottleTimeMs (v3+)
        int throttleTimeMs = 0;
        if (apiVersion >= 3)
        {
            throttleTimeMs = reader.ReadInt32();
        }

        // Brokers
        var brokers = new List<MetadataResponseBroker>();
        int brokerCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
        for (int i = 0; i < brokerCount; i++)
        {
            int nodeId = reader.ReadInt32();
            string host = isFlexible ? reader.ReadCompactString()! : reader.ReadString()!;
            int port = reader.ReadInt32();

            string? rack = null;
            if (apiVersion >= 1)
            {
                rack = isFlexible ? reader.ReadCompactString() : reader.ReadString();
            }

            // Broker tagged fields (flexible only)
            if (isFlexible)
            {
                var brokerTagCount = reader.ReadVarInt();
                for (int j = 0; j < brokerTagCount; j++)
                {
                    reader.ReadVarInt(); // tag
                    var size = reader.ReadVarInt();
                    reader.Skip(size);
                }
            }

            brokers.Add(new MetadataResponseBroker
            {
                NodeId = nodeId,
                Host = host,
                Port = port,
                Rack = rack
            });
        }

        // ClusterId (v2+, nullable)
        string? clusterId = null;
        if (apiVersion >= 2)
        {
            clusterId = isFlexible ? reader.ReadCompactString() : reader.ReadString();
        }

        // ControllerId (v1+)
        int controllerId = -1;
        if (apiVersion >= 1)
        {
            controllerId = reader.ReadInt32();
        }

        // Topics
        var topics = new List<MetadataResponseTopic>();
        int topicCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
        for (int i = 0; i < topicCount; i++)
        {
            var topicErrorCode = (ErrorCode)reader.ReadInt16();
            string? topicName = isFlexible ? reader.ReadCompactString() : reader.ReadString();

            Guid topicId = Guid.Empty;
            if (apiVersion >= 10)
            {
                topicId = reader.ReadUuid();
            }

            bool isInternal = false;
            if (apiVersion >= 1)
            {
                isInternal = reader.ReadInt8() != 0;
            }

            // Partitions
            var partitions = new List<MetadataResponsePartition>();
            int partitionCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
            for (int j = 0; j < partitionCount; j++)
            {
                var partitionErrorCode = (ErrorCode)reader.ReadInt16();
                int partitionIndex = reader.ReadInt32();
                int leaderId = reader.ReadInt32();

                int leaderEpoch = -1;
                if (apiVersion >= 7)
                {
                    leaderEpoch = reader.ReadInt32();
                }

                // ReplicaNodes
                int replicaCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
                var replicaNodes = new List<int>(replicaCount);
                for (int k = 0; k < replicaCount; k++)
                {
                    replicaNodes.Add(reader.ReadInt32());
                }

                // IsrNodes
                int isrCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
                var isrNodes = new List<int>(isrCount);
                for (int k = 0; k < isrCount; k++)
                {
                    isrNodes.Add(reader.ReadInt32());
                }

                // OfflineReplicas (v5+)
                List<int>? offlineReplicas = null;
                if (apiVersion >= 5)
                {
                    int offlineCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
                    offlineReplicas = new List<int>(offlineCount);
                    for (int k = 0; k < offlineCount; k++)
                    {
                        offlineReplicas.Add(reader.ReadInt32());
                    }
                }

                // Partition tagged fields (flexible only)
                if (isFlexible)
                {
                    var partitionTagCount = reader.ReadVarInt();
                    for (int k = 0; k < partitionTagCount; k++)
                    {
                        reader.ReadVarInt(); // tag
                        var size = reader.ReadVarInt();
                        reader.Skip(size);
                    }
                }

                partitions.Add(new MetadataResponsePartition
                {
                    ErrorCode = partitionErrorCode,
                    PartitionIndex = partitionIndex,
                    LeaderId = leaderId,
                    LeaderEpoch = leaderEpoch,
                    ReplicaNodes = replicaNodes,
                    IsrNodes = isrNodes,
                    OfflineReplicas = offlineReplicas
                });
            }

            // TopicAuthorizedOperations (v8+)
            int topicAuthorizedOperations = int.MinValue;
            if (apiVersion >= 8)
            {
                topicAuthorizedOperations = reader.ReadInt32();
            }

            // Topic tagged fields (flexible only)
            if (isFlexible)
            {
                var topicTagCount = reader.ReadVarInt();
                for (int j = 0; j < topicTagCount; j++)
                {
                    reader.ReadVarInt(); // tag
                    var size = reader.ReadVarInt();
                    reader.Skip(size);
                }
            }

            topics.Add(new MetadataResponseTopic
            {
                ErrorCode = topicErrorCode,
                Name = topicName,
                TopicId = topicId,
                IsInternal = isInternal,
                Partitions = partitions,
                TopicAuthorizedOperations = topicAuthorizedOperations
            });
        }

        // ClusterAuthorizedOperations (v8-10, skip)
        if (apiVersion >= 8 && apiVersion <= 10)
        {
            reader.ReadInt32();
        }

        // ErrorCode (v13+)
        ErrorCode errorCode = ErrorCode.None;
        if (apiVersion >= 13)
        {
            errorCode = (ErrorCode)reader.ReadInt16();
        }

        // Response body tagged fields (flexible only)
        if (isFlexible)
        {
            var bodyTagCount = reader.ReadVarInt();
            for (int i = 0; i < bodyTagCount; i++)
            {
                reader.ReadVarInt(); // tag
                var size = reader.ReadVarInt();
                reader.Skip(size);
            }
        }

        return new MetadataResponse
        {
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ThrottleTimeMs = throttleTimeMs,
            Brokers = brokers,
            ClusterId = clusterId,
            ControllerId = controllerId,
            Topics = topics,
            ErrorCode = errorCode
        };
    }
}
