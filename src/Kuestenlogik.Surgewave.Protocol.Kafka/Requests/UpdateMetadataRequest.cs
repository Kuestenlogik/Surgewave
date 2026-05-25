namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka UpdateMetadata request (v0-8) - Inter-broker API
/// Sent by the controller to brokers to update cluster metadata.
/// </summary>
public sealed class UpdateMetadataRequest : KafkaRequest
{
    /// <summary>The controller id.</summary>
    public required int ControllerId { get; init; }
    /// <summary>If KRaft controller, set to true if the request is made by the active controller (v8+).</summary>
    public bool IsKRaftController { get; init; }
    /// <summary>The controller epoch.</summary>
    public required int ControllerEpoch { get; init; }
    /// <summary>The broker epoch (v5+).</summary>
    public long BrokerEpoch { get; init; } = -1;
    /// <summary>The partition states (v5+).</summary>
    public List<UpdateMetadataTopicState>? TopicStates { get; init; }
    /// <summary>The state of each partition (v0-4).</summary>
    public List<UpdateMetadataPartitionStateV0>? UngroupedPartitionStates { get; init; }
    /// <summary>The current live brokers.</summary>
    public required List<UpdateMetadataBroker> LiveBrokers { get; init; }

    public sealed class UpdateMetadataTopicState
    {
        public required string TopicName { get; init; }
        /// <summary>The topic ID (v7+).</summary>
        public Guid TopicId { get; init; }
        public required List<UpdateMetadataPartitionState> PartitionStates { get; init; }
    }

    public sealed class UpdateMetadataPartitionStateV0
    {
        public required string TopicName { get; init; }
        public required int PartitionIndex { get; init; }
        public required int ControllerEpoch { get; init; }
        public required int Leader { get; init; }
        public required int LeaderEpoch { get; init; }
        public required List<int> Isr { get; init; }
        public required int ZkVersion { get; init; }
        public required List<int> Replicas { get; init; }
        public List<int>? OfflineReplicas { get; init; }
    }

    public sealed class UpdateMetadataPartitionState
    {
        public required int PartitionIndex { get; init; }
        public required int ControllerEpoch { get; init; }
        public required int Leader { get; init; }
        public required int LeaderEpoch { get; init; }
        public required List<int> Isr { get; init; }
        public required int ZkVersion { get; init; }
        public required List<int> Replicas { get; init; }
        public List<int>? OfflineReplicas { get; init; }
    }

    public sealed class UpdateMetadataBroker
    {
        public required int Id { get; init; }
        /// <summary>The broker hostname (v0 only).</summary>
        public string? V0Host { get; init; }
        /// <summary>The broker port (v0 only).</summary>
        public int V0Port { get; init; }
        /// <summary>The broker endpoints (v1+).</summary>
        public List<UpdateMetadataEndpoint>? Endpoints { get; init; }
        /// <summary>The rack (v2+).</summary>
        public string? Rack { get; init; }
    }

    public sealed class UpdateMetadataEndpoint
    {
        public required int Port { get; init; }
        public required string Host { get; init; }
        /// <summary>The listener name (v3+).</summary>
        public string? Listener { get; init; }
        public required short SecurityProtocol { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 6;

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

        writer.WriteInt32(ControllerId);

        // IsKRaftController (v8+)
        if (ApiVersion >= 8)
        {
            writer.WriteInt8((sbyte)(IsKRaftController ? 1 : 0));
        }

        writer.WriteInt32(ControllerEpoch);

        // BrokerEpoch (v5+)
        if (ApiVersion >= 5)
        {
            writer.WriteInt64(BrokerEpoch);
        }

        // Partition states
        if (ApiVersion >= 5)
        {
            var topics = TopicStates ?? [];
            if (isFlexible)
            {
                writer.WriteVarInt(topics.Count + 1);
            }
            else
            {
                writer.WriteInt32(topics.Count);
            }

            foreach (var topic in topics)
            {
                if (isFlexible)
                    writer.WriteCompactString(topic.TopicName);
                else
                    writer.WriteString(topic.TopicName);

                // TopicId (v7+)
                if (ApiVersion >= 7)
                {
                    writer.WriteUuid(topic.TopicId);
                }

                if (isFlexible)
                {
                    writer.WriteVarInt(topic.PartitionStates.Count + 1);
                }
                else
                {
                    writer.WriteInt32(topic.PartitionStates.Count);
                }

                foreach (var partition in topic.PartitionStates)
                {
                    WritePartitionState(writer, partition, isFlexible);
                }

                if (isFlexible)
                {
                    writer.WriteVarInt(0); // Topic tagged fields
                }
            }
        }
        else
        {
            var partitions = UngroupedPartitionStates ?? [];
            writer.WriteInt32(partitions.Count);

            foreach (var partition in partitions)
            {
                writer.WriteString(partition.TopicName);
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt32(partition.ControllerEpoch);
                writer.WriteInt32(partition.Leader);
                writer.WriteInt32(partition.LeaderEpoch);
                writer.WriteInt32(partition.Isr.Count);
                foreach (var isr in partition.Isr)
                {
                    writer.WriteInt32(isr);
                }
                writer.WriteInt32(partition.ZkVersion);
                writer.WriteInt32(partition.Replicas.Count);
                foreach (var r in partition.Replicas)
                {
                    writer.WriteInt32(r);
                }

                // OfflineReplicas (v4+)
                if (ApiVersion >= 4)
                {
                    var offline = partition.OfflineReplicas ?? [];
                    writer.WriteInt32(offline.Count);
                    foreach (var r in offline)
                    {
                        writer.WriteInt32(r);
                    }
                }
            }
        }

        // Live brokers
        if (isFlexible)
        {
            writer.WriteVarInt(LiveBrokers.Count + 1);
        }
        else
        {
            writer.WriteInt32(LiveBrokers.Count);
        }

        foreach (var broker in LiveBrokers)
        {
            writer.WriteInt32(broker.Id);

            if (ApiVersion == 0)
            {
                writer.WriteString(broker.V0Host);
                writer.WriteInt32(broker.V0Port);
            }
            else
            {
                var endpoints = broker.Endpoints ?? [];
                if (isFlexible)
                {
                    writer.WriteVarInt(endpoints.Count + 1);
                }
                else
                {
                    writer.WriteInt32(endpoints.Count);
                }

                foreach (var endpoint in endpoints)
                {
                    writer.WriteInt32(endpoint.Port);
                    if (isFlexible)
                        writer.WriteCompactString(endpoint.Host);
                    else
                        writer.WriteString(endpoint.Host);

                    // Listener (v3+)
                    if (ApiVersion >= 3)
                    {
                        if (isFlexible)
                            writer.WriteCompactString(endpoint.Listener);
                        else
                            writer.WriteString(endpoint.Listener);
                    }

                    writer.WriteInt16(endpoint.SecurityProtocol);

                    if (isFlexible)
                    {
                        writer.WriteVarInt(0); // Endpoint tagged fields
                    }
                }

                // Rack (v2+)
                if (ApiVersion >= 2)
                {
                    if (isFlexible)
                        writer.WriteCompactString(broker.Rack);
                    else
                        writer.WriteString(broker.Rack);
                }
            }

            if (isFlexible)
            {
                writer.WriteVarInt(0); // Broker tagged fields
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    private void WritePartitionState(KafkaProtocolWriter writer, UpdateMetadataPartitionState partition, bool isFlexible)
    {
        writer.WriteInt32(partition.PartitionIndex);
        writer.WriteInt32(partition.ControllerEpoch);
        writer.WriteInt32(partition.Leader);
        writer.WriteInt32(partition.LeaderEpoch);

        if (isFlexible)
        {
            writer.WriteVarInt(partition.Isr.Count + 1);
        }
        else
        {
            writer.WriteInt32(partition.Isr.Count);
        }
        foreach (var isr in partition.Isr)
        {
            writer.WriteInt32(isr);
        }

        writer.WriteInt32(partition.ZkVersion);

        if (isFlexible)
        {
            writer.WriteVarInt(partition.Replicas.Count + 1);
        }
        else
        {
            writer.WriteInt32(partition.Replicas.Count);
        }
        foreach (var r in partition.Replicas)
        {
            writer.WriteInt32(r);
        }

        var offline = partition.OfflineReplicas ?? [];
        if (isFlexible)
        {
            writer.WriteVarInt(offline.Count + 1);
        }
        else
        {
            writer.WriteInt32(offline.Count);
        }
        foreach (var r in offline)
        {
            writer.WriteInt32(r);
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Partition tagged fields
        }
    }

    public static UpdateMetadataRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 6;

        var controllerId = reader.ReadInt32();
        var isKRaftController = apiVersion >= 8 && reader.ReadInt8() != 0;
        var controllerEpoch = reader.ReadInt32();
        var brokerEpoch = apiVersion >= 5 ? reader.ReadInt64() : -1L;

        List<UpdateMetadataTopicState>? topicStates = null;
        List<UpdateMetadataPartitionStateV0>? ungroupedPartitions = null;

        if (apiVersion >= 5)
        {
            int topicCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
            topicStates = new List<UpdateMetadataTopicState>(topicCount);

            for (int t = 0; t < topicCount; t++)
            {
                var topicName = isFlexible ? reader.ReadCompactString()! : reader.ReadString()!;
                var topicId = apiVersion >= 7 ? reader.ReadUuid() : Guid.Empty;

                int partCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
                var partitions = new List<UpdateMetadataPartitionState>(partCount);

                for (int p = 0; p < partCount; p++)
                {
                    partitions.Add(ReadPartitionState(reader, isFlexible));
                }

                if (isFlexible)
                {
                    reader.SkipTaggedFields();
                }

                topicStates.Add(new UpdateMetadataTopicState
                {
                    TopicName = topicName,
                    TopicId = topicId,
                    PartitionStates = partitions
                });
            }
        }
        else
        {
            int partCount = reader.ReadInt32();
            ungroupedPartitions = new List<UpdateMetadataPartitionStateV0>(partCount);

            for (int i = 0; i < partCount; i++)
            {
                var topicName = reader.ReadString()!;
                var partitionIndex = reader.ReadInt32();
                var partControllerEpoch = reader.ReadInt32();
                var leader = reader.ReadInt32();
                var leaderEpoch = reader.ReadInt32();

                int isrCount = reader.ReadInt32();
                var isr = new List<int>(isrCount);
                for (int j = 0; j < isrCount; j++)
                {
                    isr.Add(reader.ReadInt32());
                }

                var zkVersion = reader.ReadInt32();

                int replicaCount = reader.ReadInt32();
                var replicas = new List<int>(replicaCount);
                for (int j = 0; j < replicaCount; j++)
                {
                    replicas.Add(reader.ReadInt32());
                }

                List<int>? offlineReplicas = null;
                if (apiVersion >= 4)
                {
                    int offlineCount = reader.ReadInt32();
                    offlineReplicas = new List<int>(offlineCount);
                    for (int j = 0; j < offlineCount; j++)
                    {
                        offlineReplicas.Add(reader.ReadInt32());
                    }
                }

                ungroupedPartitions.Add(new UpdateMetadataPartitionStateV0
                {
                    TopicName = topicName,
                    PartitionIndex = partitionIndex,
                    ControllerEpoch = partControllerEpoch,
                    Leader = leader,
                    LeaderEpoch = leaderEpoch,
                    Isr = isr,
                    ZkVersion = zkVersion,
                    Replicas = replicas,
                    OfflineReplicas = offlineReplicas
                });
            }
        }

        // Live brokers
        int brokerCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
        var liveBrokers = new List<UpdateMetadataBroker>(brokerCount);

        for (int b = 0; b < brokerCount; b++)
        {
            var brokerId = reader.ReadInt32();
            string? v0Host = null;
            int v0Port = 0;
            List<UpdateMetadataEndpoint>? endpoints = null;
            string? rack = null;

            if (apiVersion == 0)
            {
                v0Host = reader.ReadString();
                v0Port = reader.ReadInt32();
            }
            else
            {
                int endpointCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
                endpoints = new List<UpdateMetadataEndpoint>(endpointCount);

                for (int e = 0; e < endpointCount; e++)
                {
                    var port = reader.ReadInt32();
                    var host = isFlexible ? reader.ReadCompactString()! : reader.ReadString()!;
                    string? listener = null;

                    if (apiVersion >= 3)
                    {
                        listener = isFlexible ? reader.ReadCompactString() : reader.ReadString();
                    }

                    var securityProtocol = reader.ReadInt16();

                    if (isFlexible)
                    {
                        reader.SkipTaggedFields();
                    }

                    endpoints.Add(new UpdateMetadataEndpoint
                    {
                        Port = port,
                        Host = host,
                        Listener = listener,
                        SecurityProtocol = securityProtocol
                    });
                }

                if (apiVersion >= 2)
                {
                    rack = isFlexible ? reader.ReadCompactString() : reader.ReadString();
                }
            }

            if (isFlexible)
            {
                reader.SkipTaggedFields();
            }

            liveBrokers.Add(new UpdateMetadataBroker
            {
                Id = brokerId,
                V0Host = v0Host,
                V0Port = v0Port,
                Endpoints = endpoints,
                Rack = rack
            });
        }

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new UpdateMetadataRequest
        {
            ApiKey = ApiKey.UpdateMetadata,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            ControllerId = controllerId,
            IsKRaftController = isKRaftController,
            ControllerEpoch = controllerEpoch,
            BrokerEpoch = brokerEpoch,
            TopicStates = topicStates,
            UngroupedPartitionStates = ungroupedPartitions,
            LiveBrokers = liveBrokers
        };
    }

    private static UpdateMetadataPartitionState ReadPartitionState(KafkaProtocolReader reader, bool isFlexible)
    {
        var partitionIndex = reader.ReadInt32();
        var controllerEpoch = reader.ReadInt32();
        var leader = reader.ReadInt32();
        var leaderEpoch = reader.ReadInt32();

        int isrCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
        var isr = new List<int>(isrCount);
        for (int i = 0; i < isrCount; i++)
        {
            isr.Add(reader.ReadInt32());
        }

        var zkVersion = reader.ReadInt32();

        int replicaCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
        var replicas = new List<int>(replicaCount);
        for (int i = 0; i < replicaCount; i++)
        {
            replicas.Add(reader.ReadInt32());
        }

        int offlineCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
        var offline = new List<int>(offlineCount);
        for (int i = 0; i < offlineCount; i++)
        {
            offline.Add(reader.ReadInt32());
        }

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new UpdateMetadataPartitionState
        {
            PartitionIndex = partitionIndex,
            ControllerEpoch = controllerEpoch,
            Leader = leader,
            LeaderEpoch = leaderEpoch,
            Isr = isr,
            ZkVersion = zkVersion,
            Replicas = replicas,
            OfflineReplicas = offline
        };
    }
}

/// <summary>
/// Kafka UpdateMetadata response (v0-8)
/// </summary>
public sealed class UpdateMetadataResponse : KafkaResponse
{
    /// <summary>The error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 6;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        writer.WriteInt16((short)ErrorCode);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static UpdateMetadataResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        bool isFlexible = apiVersion >= 6;

        if (isFlexible)
        {
            reader.SkipTaggedFields(); // Response header tagged fields
        }

        var errorCode = (ErrorCode)reader.ReadInt16();

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new UpdateMetadataResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = errorCode
        };
    }
}
