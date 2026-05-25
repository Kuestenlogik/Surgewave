namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka StopReplica request (v0-4) - Inter-broker API
/// Sent by the controller to brokers to stop replicating partitions.
/// </summary>
public sealed class StopReplicaRequest : KafkaRequest
{
    /// <summary>The controller id.</summary>
    public required int ControllerId { get; init; }
    /// <summary>If KRaft controller, set to true if the request is made by the active controller (v3+).</summary>
    public bool IsKRaftController { get; init; }
    /// <summary>The controller epoch.</summary>
    public required int ControllerEpoch { get; init; }
    /// <summary>The broker epoch (v1+).</summary>
    public long BrokerEpoch { get; init; } = -1;
    /// <summary>Whether these partitions should be deleted (v0-2).</summary>
    public bool DeletePartitions { get; init; }
    /// <summary>The partitions to stop (v0-2).</summary>
    public List<StopReplicaPartitionV0>? UngroupedPartitions { get; init; }
    /// <summary>The topics to stop (v1-2).</summary>
    public List<StopReplicaTopicV1>? Topics { get; init; }
    /// <summary>The topic states (v3+).</summary>
    public List<StopReplicaTopicState>? TopicStates { get; init; }

    public sealed class StopReplicaPartitionV0
    {
        public required string TopicName { get; init; }
        public required int PartitionIndex { get; init; }
    }

    public sealed class StopReplicaTopicV1
    {
        public required string Name { get; init; }
        public required List<int> PartitionIndexes { get; init; }
    }

    public sealed class StopReplicaTopicState
    {
        public required string TopicName { get; init; }
        public required List<StopReplicaPartitionState> PartitionStates { get; init; }
    }

    public sealed class StopReplicaPartitionState
    {
        public required int PartitionIndex { get; init; }
        public int LeaderEpoch { get; init; } = -1;
        /// <summary>Whether this partition should be deleted.</summary>
        public bool DeletePartition { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

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

        // IsKRaftController (v3+)
        if (ApiVersion >= 3)
        {
            writer.WriteInt8((sbyte)(IsKRaftController ? 1 : 0));
        }

        writer.WriteInt32(ControllerEpoch);

        // BrokerEpoch (v1+)
        if (ApiVersion >= 1)
        {
            writer.WriteInt64(BrokerEpoch);
        }

        // DeletePartitions (v0-2)
        if (ApiVersion <= 2)
        {
            writer.WriteInt8((sbyte)(DeletePartitions ? 1 : 0));
        }

        if (ApiVersion == 0)
        {
            // v0: ungrouped partitions
            var parts = UngroupedPartitions ?? [];
            writer.WriteInt32(parts.Count);
            foreach (var p in parts)
            {
                writer.WriteString(p.TopicName);
                writer.WriteInt32(p.PartitionIndex);
            }
        }
        else if (ApiVersion <= 2)
        {
            // v1-2: topics array
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
                if (isFlexible)
                    writer.WriteCompactString(topic.Name);
                else
                    writer.WriteString(topic.Name);

                if (isFlexible)
                {
                    writer.WriteVarInt(topic.PartitionIndexes.Count + 1);
                }
                else
                {
                    writer.WriteInt32(topic.PartitionIndexes.Count);
                }

                foreach (var idx in topic.PartitionIndexes)
                {
                    writer.WriteInt32(idx);
                }

                if (isFlexible)
                {
                    writer.WriteVarInt(0); // Topic tagged fields
                }
            }
        }
        else
        {
            // v3+: topic states
            var states = TopicStates ?? [];
            writer.WriteVarInt(states.Count + 1);

            foreach (var topic in states)
            {
                writer.WriteCompactString(topic.TopicName);

                writer.WriteVarInt(topic.PartitionStates.Count + 1);
                foreach (var partition in topic.PartitionStates)
                {
                    writer.WriteInt32(partition.PartitionIndex);
                    writer.WriteInt32(partition.LeaderEpoch);
                    writer.WriteInt8((sbyte)(partition.DeletePartition ? 1 : 0));

                    writer.WriteVarInt(0); // Partition tagged fields
                }

                writer.WriteVarInt(0); // Topic tagged fields
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static StopReplicaRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 2;

        var controllerId = reader.ReadInt32();
        var isKRaftController = apiVersion >= 3 && reader.ReadInt8() != 0;
        var controllerEpoch = reader.ReadInt32();
        var brokerEpoch = apiVersion >= 1 ? reader.ReadInt64() : -1L;
        var deletePartitions = apiVersion <= 2 && reader.ReadInt8() != 0;

        List<StopReplicaPartitionV0>? ungroupedPartitions = null;
        List<StopReplicaTopicV1>? topics = null;
        List<StopReplicaTopicState>? topicStates = null;

        if (apiVersion == 0)
        {
            int count = reader.ReadInt32();
            ungroupedPartitions = new List<StopReplicaPartitionV0>(count);
            for (int i = 0; i < count; i++)
            {
                var topicName = reader.ReadString()!;
                var partitionIndex = reader.ReadInt32();
                ungroupedPartitions.Add(new StopReplicaPartitionV0
                {
                    TopicName = topicName,
                    PartitionIndex = partitionIndex
                });
            }
        }
        else if (apiVersion <= 2)
        {
            int topicCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
            topics = new List<StopReplicaTopicV1>(topicCount);

            for (int t = 0; t < topicCount; t++)
            {
                var name = isFlexible ? reader.ReadCompactString()! : reader.ReadString()!;
                int partCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
                var partitions = new List<int>(partCount);

                for (int p = 0; p < partCount; p++)
                {
                    partitions.Add(reader.ReadInt32());
                }

                if (isFlexible)
                {
                    reader.SkipTaggedFields();
                }

                topics.Add(new StopReplicaTopicV1
                {
                    Name = name,
                    PartitionIndexes = partitions
                });
            }
        }
        else
        {
            int stateCount = reader.ReadVarInt() - 1;
            topicStates = new List<StopReplicaTopicState>(stateCount);

            for (int t = 0; t < stateCount; t++)
            {
                var topicName = reader.ReadCompactString()!;
                int partCount = reader.ReadVarInt() - 1;
                var partitionStates = new List<StopReplicaPartitionState>(partCount);

                for (int p = 0; p < partCount; p++)
                {
                    var partitionIndex = reader.ReadInt32();
                    var leaderEpoch = reader.ReadInt32();
                    var delete = reader.ReadInt8() != 0;

                    reader.SkipTaggedFields();

                    partitionStates.Add(new StopReplicaPartitionState
                    {
                        PartitionIndex = partitionIndex,
                        LeaderEpoch = leaderEpoch,
                        DeletePartition = delete
                    });
                }

                reader.SkipTaggedFields();

                topicStates.Add(new StopReplicaTopicState
                {
                    TopicName = topicName,
                    PartitionStates = partitionStates
                });
            }
        }

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new StopReplicaRequest
        {
            ApiKey = ApiKey.StopReplica,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            ControllerId = controllerId,
            IsKRaftController = isKRaftController,
            ControllerEpoch = controllerEpoch,
            BrokerEpoch = brokerEpoch,
            DeletePartitions = deletePartitions,
            UngroupedPartitions = ungroupedPartitions,
            Topics = topics,
            TopicStates = topicStates
        };
    }
}

/// <summary>
/// Kafka StopReplica response (v0-4)
/// </summary>
public sealed class StopReplicaResponse : KafkaResponse
{
    /// <summary>The top-level error code, or 0 if there was no top-level error.</summary>
    public ErrorCode ErrorCode { get; init; }
    /// <summary>The responses for each partition.</summary>
    public required List<StopReplicaPartitionError> PartitionErrors { get; init; }

    public sealed class StopReplicaPartitionError
    {
        public required string TopicName { get; init; }
        public required int PartitionIndex { get; init; }
        public required ErrorCode ErrorCode { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 2;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        writer.WriteInt16((short)ErrorCode);

        if (isFlexible)
        {
            writer.WriteVarInt(PartitionErrors.Count + 1);
        }
        else
        {
            writer.WriteInt32(PartitionErrors.Count);
        }

        foreach (var partition in PartitionErrors)
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

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static StopReplicaResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        bool isFlexible = apiVersion >= 2;

        if (isFlexible)
        {
            reader.SkipTaggedFields(); // Response header tagged fields
        }

        var errorCode = (ErrorCode)reader.ReadInt16();

        int partCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
        var partitionErrors = new List<StopReplicaPartitionError>(partCount);

        for (int p = 0; p < partCount; p++)
        {
            var topicName = isFlexible ? reader.ReadCompactString()! : reader.ReadString()!;
            var partitionIndex = reader.ReadInt32();
            var partErrorCode = (ErrorCode)reader.ReadInt16();

            if (isFlexible)
            {
                reader.SkipTaggedFields();
            }

            partitionErrors.Add(new StopReplicaPartitionError
            {
                TopicName = topicName,
                PartitionIndex = partitionIndex,
                ErrorCode = partErrorCode
            });
        }

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new StopReplicaResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = errorCode,
            PartitionErrors = partitionErrors
        };
    }
}
