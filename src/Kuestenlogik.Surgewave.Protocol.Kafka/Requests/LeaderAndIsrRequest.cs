namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka LeaderAndIsr request (v0-7) - Inter-broker API
/// Sent by the controller to brokers to establish leader/follower state for partitions.
/// </summary>
public sealed class LeaderAndIsrRequest : KafkaRequest
{
    /// <summary>The current controller ID.</summary>
    public required int ControllerId { get; init; }
    /// <summary>If KRaft controller, set to true if the request is made by the active controller (v6+).</summary>
    public bool IsKRaftController { get; init; }
    /// <summary>The current controller epoch.</summary>
    public required int ControllerEpoch { get; init; }
    /// <summary>The current broker epoch (v2+).</summary>
    public long BrokerEpoch { get; init; } = -1;
    /// <summary>The type that indicates whether all topics are included in the request (v5+).</summary>
    public sbyte Type { get; init; }
    /// <summary>Each topic and its state.</summary>
    public required List<LeaderAndIsrTopicState> TopicStates { get; init; }
    /// <summary>The current live leaders.</summary>
    public required List<LeaderAndIsrLiveLeader> LiveLeaders { get; init; }

    public sealed class LeaderAndIsrTopicState
    {
        /// <summary>The topic name.</summary>
        public required string TopicName { get; init; }
        /// <summary>The unique topic ID (v5+).</summary>
        public Guid TopicId { get; init; }
        /// <summary>The state of each partition.</summary>
        public required List<LeaderAndIsrPartitionState> PartitionStates { get; init; }
    }

    public sealed class LeaderAndIsrPartitionState
    {
        /// <summary>The partition index.</summary>
        public required int PartitionIndex { get; init; }
        /// <summary>The controller epoch.</summary>
        public required int ControllerEpoch { get; init; }
        /// <summary>The broker ID of the leader.</summary>
        public required int Leader { get; init; }
        /// <summary>The leader epoch.</summary>
        public required int LeaderEpoch { get; init; }
        /// <summary>The in-sync replica IDs.</summary>
        public required List<int> Isr { get; init; }
        /// <summary>The current epoch for the partition. The epoch is a monotonically increasing value which is incremented after every partition change (v3+).</summary>
        public int PartitionEpoch { get; init; }
        /// <summary>The replica IDs.</summary>
        public required List<int> Replicas { get; init; }
        /// <summary>The replica IDs that we are adding this partition to, or null if no replicas are being added (v3+).</summary>
        public List<int>? AddingReplicas { get; init; }
        /// <summary>The replica IDs that we are removing this partition from, or null if no replicas are being removed (v3+).</summary>
        public List<int>? RemovingReplicas { get; init; }
        /// <summary>Whether the replica should have existed on the broker or not (v1+).</summary>
        public bool IsNew { get; init; }
        /// <summary>The expected leader epoch. For version 6 onwards (v7+).</summary>
        public int LeaderRecoveryState { get; init; }
    }

    public sealed class LeaderAndIsrLiveLeader
    {
        /// <summary>The leader's broker ID.</summary>
        public required int BrokerId { get; init; }
        /// <summary>The leader's hostname.</summary>
        public required string Host { get; init; }
        /// <summary>The leader's port.</summary>
        public required int Port { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 4;

        // Header
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            // Per the Kafka RequestHeader spec, ClientId is ALWAYS a regular
            // (non-compact) NULLABLE_STRING even in flexible request versions;
            // only the header tagged fields are flexible. Writing it compact
            // here made the header unparseable by ReadRequestHeader on the
            // receiving broker (the inter-broker send path was never exercised
            // end-to-end until #69).
            writer.WriteString(ClientId);
            writer.WriteVarInt(0); // Header tagged fields
        }
        else
        {
            writer.WriteString(ClientId);
        }

        // ControllerId
        writer.WriteInt32(ControllerId);

        // IsKRaftController (v6+)
        if (ApiVersion >= 6)
        {
            writer.WriteInt8((sbyte)(IsKRaftController ? 1 : 0));
        }

        // ControllerEpoch
        writer.WriteInt32(ControllerEpoch);

        // BrokerEpoch (v2+)
        if (ApiVersion >= 2)
        {
            writer.WriteInt64(BrokerEpoch);
        }

        // Type (v5+)
        if (ApiVersion >= 5)
        {
            writer.WriteInt8(Type);
        }

        // TopicStates
        if (isFlexible)
        {
            writer.WriteVarInt(TopicStates.Count + 1);
        }
        else
        {
            writer.WriteInt32(TopicStates.Count);
        }

        foreach (var topic in TopicStates)
        {
            if (isFlexible)
                writer.WriteCompactString(topic.TopicName);
            else
                writer.WriteString(topic.TopicName);

            // TopicId (v5+)
            if (ApiVersion >= 5)
            {
                writer.WriteUuid(topic.TopicId);
            }

            // PartitionStates
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
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt32(partition.ControllerEpoch);
                writer.WriteInt32(partition.Leader);
                writer.WriteInt32(partition.LeaderEpoch);

                // ISR
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

                // PartitionEpoch (v3+)
                if (ApiVersion >= 3)
                {
                    writer.WriteInt32(partition.PartitionEpoch);
                }

                // Replicas
                if (isFlexible)
                {
                    writer.WriteVarInt(partition.Replicas.Count + 1);
                }
                else
                {
                    writer.WriteInt32(partition.Replicas.Count);
                }
                foreach (var replica in partition.Replicas)
                {
                    writer.WriteInt32(replica);
                }

                // AddingReplicas (v3+)
                if (ApiVersion >= 3)
                {
                    var adding = partition.AddingReplicas ?? [];
                    if (isFlexible)
                    {
                        writer.WriteVarInt(adding.Count + 1);
                    }
                    else
                    {
                        writer.WriteInt32(adding.Count);
                    }
                    foreach (var r in adding)
                    {
                        writer.WriteInt32(r);
                    }
                }

                // RemovingReplicas (v3+)
                if (ApiVersion >= 3)
                {
                    var removing = partition.RemovingReplicas ?? [];
                    if (isFlexible)
                    {
                        writer.WriteVarInt(removing.Count + 1);
                    }
                    else
                    {
                        writer.WriteInt32(removing.Count);
                    }
                    foreach (var r in removing)
                    {
                        writer.WriteInt32(r);
                    }
                }

                // IsNew (v1+)
                if (ApiVersion >= 1)
                {
                    writer.WriteInt8((sbyte)(partition.IsNew ? 1 : 0));
                }

                // LeaderRecoveryState (v7+)
                if (ApiVersion >= 7)
                {
                    writer.WriteInt8((sbyte)partition.LeaderRecoveryState);
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

        // LiveLeaders
        if (isFlexible)
        {
            writer.WriteVarInt(LiveLeaders.Count + 1);
        }
        else
        {
            writer.WriteInt32(LiveLeaders.Count);
        }

        foreach (var leader in LiveLeaders)
        {
            writer.WriteInt32(leader.BrokerId);
            if (isFlexible)
                writer.WriteCompactString(leader.Host);
            else
                writer.WriteString(leader.Host);
            writer.WriteInt32(leader.Port);

            if (isFlexible)
            {
                writer.WriteVarInt(0); // Leader tagged fields
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static LeaderAndIsrRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 4;

        var controllerId = reader.ReadInt32();
        var isKRaftController = apiVersion >= 6 && reader.ReadInt8() != 0;
        var controllerEpoch = reader.ReadInt32();
        var brokerEpoch = apiVersion >= 2 ? reader.ReadInt64() : -1L;
        var type = apiVersion >= 5 ? reader.ReadInt8() : (sbyte)0;

        // TopicStates
        int topicCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
        var topicStates = new List<LeaderAndIsrTopicState>(topicCount);

        for (int t = 0; t < topicCount; t++)
        {
            var topicName = isFlexible ? reader.ReadCompactString()! : reader.ReadString()!;
            var topicId = apiVersion >= 5 ? reader.ReadUuid() : Guid.Empty;

            int partitionCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
            var partitionStates = new List<LeaderAndIsrPartitionState>(partitionCount);

            for (int p = 0; p < partitionCount; p++)
            {
                var partitionIndex = reader.ReadInt32();
                var partControllerEpoch = reader.ReadInt32();
                var leader = reader.ReadInt32();
                var leaderEpoch = reader.ReadInt32();

                int isrCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
                var isr = new List<int>(isrCount);
                for (int i = 0; i < isrCount; i++)
                {
                    isr.Add(reader.ReadInt32());
                }

                var partitionEpoch = apiVersion >= 3 ? reader.ReadInt32() : 0;

                int replicaCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
                var replicas = new List<int>(replicaCount);
                for (int i = 0; i < replicaCount; i++)
                {
                    replicas.Add(reader.ReadInt32());
                }

                List<int>? addingReplicas = null;
                List<int>? removingReplicas = null;

                if (apiVersion >= 3)
                {
                    int addingCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
                    addingReplicas = new List<int>(addingCount);
                    for (int i = 0; i < addingCount; i++)
                    {
                        addingReplicas.Add(reader.ReadInt32());
                    }

                    int removingCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
                    removingReplicas = new List<int>(removingCount);
                    for (int i = 0; i < removingCount; i++)
                    {
                        removingReplicas.Add(reader.ReadInt32());
                    }
                }

                var isNew = apiVersion >= 1 && reader.ReadInt8() != 0;
                var leaderRecoveryState = apiVersion >= 7 ? reader.ReadInt8() : 0;

                if (isFlexible)
                {
                    reader.SkipTaggedFields();
                }

                partitionStates.Add(new LeaderAndIsrPartitionState
                {
                    PartitionIndex = partitionIndex,
                    ControllerEpoch = partControllerEpoch,
                    Leader = leader,
                    LeaderEpoch = leaderEpoch,
                    Isr = isr,
                    PartitionEpoch = partitionEpoch,
                    Replicas = replicas,
                    AddingReplicas = addingReplicas,
                    RemovingReplicas = removingReplicas,
                    IsNew = isNew,
                    LeaderRecoveryState = leaderRecoveryState
                });
            }

            if (isFlexible)
            {
                reader.SkipTaggedFields();
            }

            topicStates.Add(new LeaderAndIsrTopicState
            {
                TopicName = topicName,
                TopicId = topicId,
                PartitionStates = partitionStates
            });
        }

        // LiveLeaders
        int leaderCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
        var liveLeaders = new List<LeaderAndIsrLiveLeader>(leaderCount);

        for (int l = 0; l < leaderCount; l++)
        {
            var brokerId = reader.ReadInt32();
            var host = isFlexible ? reader.ReadCompactString()! : reader.ReadString()!;
            var port = reader.ReadInt32();

            if (isFlexible)
            {
                reader.SkipTaggedFields();
            }

            liveLeaders.Add(new LeaderAndIsrLiveLeader
            {
                BrokerId = brokerId,
                Host = host,
                Port = port
            });
        }

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new LeaderAndIsrRequest
        {
            ApiKey = ApiKey.LeaderAndIsr,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            ControllerId = controllerId,
            IsKRaftController = isKRaftController,
            ControllerEpoch = controllerEpoch,
            BrokerEpoch = brokerEpoch,
            Type = type,
            TopicStates = topicStates,
            LiveLeaders = liveLeaders
        };
    }
}

/// <summary>
/// Kafka LeaderAndIsr response (v0-7)
/// </summary>
public sealed class LeaderAndIsrResponse : KafkaResponse
{
    /// <summary>The error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }
    /// <summary>Each partition result (v0-4).</summary>
    public List<LeaderAndIsrPartitionError>? PartitionErrors { get; init; }
    /// <summary>Each topic result (v5+).</summary>
    public List<LeaderAndIsrTopicError>? Topics { get; init; }

    public sealed class LeaderAndIsrPartitionError
    {
        public required string TopicName { get; init; }
        public required int PartitionIndex { get; init; }
        public required ErrorCode ErrorCode { get; init; }
    }

    public sealed class LeaderAndIsrTopicError
    {
        public Guid TopicId { get; init; }
        public required List<LeaderAndIsrPartitionResult> PartitionErrors { get; init; }
    }

    public sealed class LeaderAndIsrPartitionResult
    {
        public required int PartitionIndex { get; init; }
        public required ErrorCode ErrorCode { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 4;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        writer.WriteInt16((short)ErrorCode);

        if (ApiVersion >= 5)
        {
            // Topic-based response (v5+)
            var topics = Topics ?? [];
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
                writer.WriteUuid(topic.TopicId);

                if (isFlexible)
                {
                    writer.WriteVarInt(topic.PartitionErrors.Count + 1);
                }
                else
                {
                    writer.WriteInt32(topic.PartitionErrors.Count);
                }

                foreach (var partition in topic.PartitionErrors)
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
        }
        else
        {
            // Partition-based response (v0-4)
            var partitions = PartitionErrors ?? [];
            if (isFlexible)
            {
                writer.WriteVarInt(partitions.Count + 1);
            }
            else
            {
                writer.WriteInt32(partitions.Count);
            }

            foreach (var partition in partitions)
            {
                if (isFlexible)
                    writer.WriteCompactString(partition.TopicName);
                else
                    writer.WriteString(partition.TopicName);
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt16((short)partition.ErrorCode);

                if (isFlexible)
                {
                    writer.WriteVarInt(0); // Partition tagged fields
                }
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static LeaderAndIsrResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        bool isFlexible = apiVersion >= 4;

        if (isFlexible)
        {
            reader.SkipTaggedFields(); // Response header tagged fields
        }

        var errorCode = (ErrorCode)reader.ReadInt16();

        List<LeaderAndIsrPartitionError>? partitionErrors = null;
        List<LeaderAndIsrTopicError>? topics = null;

        if (apiVersion >= 5)
        {
            // Topic-based response (v5+)
            int topicCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
            topics = new List<LeaderAndIsrTopicError>(topicCount);

            for (int t = 0; t < topicCount; t++)
            {
                var topicId = reader.ReadUuid();
                int partCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
                var partErrors = new List<LeaderAndIsrPartitionResult>(partCount);

                for (int p = 0; p < partCount; p++)
                {
                    var partitionIndex = reader.ReadInt32();
                    var partErrorCode = (ErrorCode)reader.ReadInt16();

                    if (isFlexible)
                    {
                        reader.SkipTaggedFields();
                    }

                    partErrors.Add(new LeaderAndIsrPartitionResult
                    {
                        PartitionIndex = partitionIndex,
                        ErrorCode = partErrorCode
                    });
                }

                if (isFlexible)
                {
                    reader.SkipTaggedFields();
                }

                topics.Add(new LeaderAndIsrTopicError
                {
                    TopicId = topicId,
                    PartitionErrors = partErrors
                });
            }
        }
        else
        {
            // Partition-based response (v0-4)
            int partCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
            partitionErrors = new List<LeaderAndIsrPartitionError>(partCount);

            for (int p = 0; p < partCount; p++)
            {
                var topicName = isFlexible ? reader.ReadCompactString()! : reader.ReadString()!;
                var partitionIndex = reader.ReadInt32();
                var partErrorCode = (ErrorCode)reader.ReadInt16();

                if (isFlexible)
                {
                    reader.SkipTaggedFields();
                }

                partitionErrors.Add(new LeaderAndIsrPartitionError
                {
                    TopicName = topicName,
                    PartitionIndex = partitionIndex,
                    ErrorCode = partErrorCode
                });
            }
        }

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new LeaderAndIsrResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = errorCode,
            PartitionErrors = partitionErrors,
            Topics = topics
        };
    }
}
