namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

// ────────────────────────────────────────────────────────────────
// InitializeShareGroupState (API Key 83)
// ────────────────────────────────────────────────────────────────

/// <summary>
/// Kafka InitializeShareGroupState request (API Key 83, v0-0).
/// KIP-932: Queues for Kafka — initialize share group partition state.
/// </summary>
public sealed class InitializeShareGroupStateRequest : KafkaRequest
{
    /// <summary>The group identifier.</summary>
    public required string GroupId { get; init; }

    /// <summary>The data for the topics.</summary>
    public required List<InitializeStateData> Topics { get; init; }

    public sealed class InitializeStateData
    {
        /// <summary>The topic identifier.</summary>
        public required Guid TopicId { get; init; }

        /// <summary>The data for the partitions.</summary>
        public required List<PartitionData> Partitions { get; init; }
    }

    public sealed class PartitionData
    {
        /// <summary>The partition index.</summary>
        public int Partition { get; init; }

        /// <summary>The state epoch for this share-partition.</summary>
        public int StateEpoch { get; init; }

        /// <summary>The share-partition start offset, or -1 if the start offset is not being initialized.</summary>
        public long StartOffset { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteCompactString(GroupId);

        writer.WriteVarInt(Topics.Count + 1);
        foreach (var topic in Topics)
        {
            writer.WriteUuid(topic.TopicId);
            writer.WriteVarInt(topic.Partitions.Count + 1);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.Partition);
                writer.WriteInt32(partition.StateEpoch);
                writer.WriteInt64(partition.StartOffset);
                writer.WriteVarInt(0); // Partition tagged fields
            }
            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static InitializeShareGroupStateRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var groupId = reader.ReadCompactString() ?? "";

        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<InitializeStateData>(topicCount);
        for (int i = 0; i < topicCount; i++)
        {
            var topicId = reader.ReadUuid();
            var partCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionData>(partCount);
            for (int j = 0; j < partCount; j++)
            {
                var partition = reader.ReadInt32();
                var stateEpoch = reader.ReadInt32();
                var startOffset = reader.ReadInt64();
                reader.SkipTaggedFields();

                partitions.Add(new PartitionData
                {
                    Partition = partition,
                    StateEpoch = stateEpoch,
                    StartOffset = startOffset
                });
            }
            reader.SkipTaggedFields();

            topics.Add(new InitializeStateData
            {
                TopicId = topicId,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new InitializeShareGroupStateRequest
        {
            ApiKey = ApiKey.InitializeShareGroupState,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            GroupId = groupId,
            Topics = topics
        };
    }
}

/// <summary>
/// Kafka InitializeShareGroupState response (API Key 83, v0-0).
/// </summary>
public sealed class InitializeShareGroupStateResponse : KafkaResponse
{
    /// <summary>The initialization results.</summary>
    public required List<InitializeStateResult> Results { get; init; }

    public sealed class InitializeStateResult
    {
        /// <summary>The topic identifier.</summary>
        public required Guid TopicId { get; init; }

        /// <summary>The results for the partitions.</summary>
        public required List<PartitionResult> Partitions { get; init; }
    }

    public sealed class PartitionResult
    {
        /// <summary>The partition index.</summary>
        public int Partition { get; init; }

        /// <summary>The error code, or 0 if there was no error.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The error message, or null if there was no error.</summary>
        public string? ErrorMessage { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteVarInt(Results.Count + 1);
        foreach (var result in Results)
        {
            writer.WriteUuid(result.TopicId);
            writer.WriteVarInt(result.Partitions.Count + 1);
            foreach (var partition in result.Partitions)
            {
                writer.WriteInt32(partition.Partition);
                writer.WriteInt16((short)partition.ErrorCode);
                writer.WriteCompactString(partition.ErrorMessage);
                writer.WriteVarInt(0); // Partition tagged fields
            }
            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static InitializeShareGroupStateResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var resultCount = reader.ReadVarInt() - 1;
        var results = new List<InitializeStateResult>(resultCount);
        for (int i = 0; i < resultCount; i++)
        {
            var topicId = reader.ReadUuid();
            var partCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionResult>(partCount);
            for (int j = 0; j < partCount; j++)
            {
                var partition = reader.ReadInt32();
                var errorCode = (ErrorCode)reader.ReadInt16();
                var errorMessage = reader.ReadCompactString();
                reader.SkipTaggedFields();

                partitions.Add(new PartitionResult
                {
                    Partition = partition,
                    ErrorCode = errorCode,
                    ErrorMessage = errorMessage
                });
            }
            reader.SkipTaggedFields();

            results.Add(new InitializeStateResult
            {
                TopicId = topicId,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new InitializeShareGroupStateResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            Results = results
        };
    }
}

// ────────────────────────────────────────────────────────────────
// ReadShareGroupState (API Key 84)
// ────────────────────────────────────────────────────────────────

/// <summary>
/// Kafka ReadShareGroupState request (API Key 84, v0-0).
/// KIP-932: Queues for Kafka — read share group partition state.
/// </summary>
public sealed class ReadShareGroupStateRequest : KafkaRequest
{
    /// <summary>The group identifier.</summary>
    public required string GroupId { get; init; }

    /// <summary>The data for the topics.</summary>
    public required List<ReadStateData> Topics { get; init; }

    public sealed class ReadStateData
    {
        /// <summary>The topic identifier.</summary>
        public required Guid TopicId { get; init; }

        /// <summary>The data for the partitions.</summary>
        public required List<PartitionData> Partitions { get; init; }
    }

    public sealed class PartitionData
    {
        /// <summary>The partition index.</summary>
        public int Partition { get; init; }

        /// <summary>The leader epoch of the share-partition.</summary>
        public int LeaderEpoch { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteCompactString(GroupId);

        writer.WriteVarInt(Topics.Count + 1);
        foreach (var topic in Topics)
        {
            writer.WriteUuid(topic.TopicId);
            writer.WriteVarInt(topic.Partitions.Count + 1);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.Partition);
                writer.WriteInt32(partition.LeaderEpoch);
                writer.WriteVarInt(0); // Partition tagged fields
            }
            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static ReadShareGroupStateRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var groupId = reader.ReadCompactString() ?? "";

        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<ReadStateData>(topicCount);
        for (int i = 0; i < topicCount; i++)
        {
            var topicId = reader.ReadUuid();
            var partCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionData>(partCount);
            for (int j = 0; j < partCount; j++)
            {
                var partition = reader.ReadInt32();
                var leaderEpoch = reader.ReadInt32();
                reader.SkipTaggedFields();

                partitions.Add(new PartitionData
                {
                    Partition = partition,
                    LeaderEpoch = leaderEpoch
                });
            }
            reader.SkipTaggedFields();

            topics.Add(new ReadStateData
            {
                TopicId = topicId,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new ReadShareGroupStateRequest
        {
            ApiKey = ApiKey.ReadShareGroupState,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            GroupId = groupId,
            Topics = topics
        };
    }
}

/// <summary>
/// Kafka ReadShareGroupState response (API Key 84, v0-0).
/// </summary>
public sealed class ReadShareGroupStateResponse : KafkaResponse
{
    /// <summary>The read results.</summary>
    public required List<ReadStateResult> Results { get; init; }

    public sealed class ReadStateResult
    {
        /// <summary>The topic identifier.</summary>
        public required Guid TopicId { get; init; }

        /// <summary>The results for the partitions.</summary>
        public required List<PartitionResult> Partitions { get; init; }
    }

    public sealed class PartitionResult
    {
        /// <summary>The partition index.</summary>
        public int Partition { get; init; }

        /// <summary>The error code, or 0 if there was no error.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The error message, or null if there was no error.</summary>
        public string? ErrorMessage { get; init; }

        /// <summary>The state epoch of the share-partition.</summary>
        public int StateEpoch { get; init; }

        /// <summary>The share-partition start offset, which can be -1 if not yet initialized.</summary>
        public long StartOffset { get; init; }

        /// <summary>The state batches for this share-partition.</summary>
        public required List<StateBatch> StateBatches { get; init; }
    }

    public sealed class StateBatch
    {
        /// <summary>The first offset of this state batch.</summary>
        public long FirstOffset { get; init; }

        /// <summary>The last offset of this state batch.</summary>
        public long LastOffset { get; init; }

        /// <summary>The delivery state — 0:Available, 2:Acked, 4:Archived.</summary>
        public sbyte DeliveryState { get; init; }

        /// <summary>The delivery count.</summary>
        public short DeliveryCount { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteVarInt(Results.Count + 1);
        foreach (var result in Results)
        {
            writer.WriteUuid(result.TopicId);
            writer.WriteVarInt(result.Partitions.Count + 1);
            foreach (var partition in result.Partitions)
            {
                writer.WriteInt32(partition.Partition);
                writer.WriteInt16((short)partition.ErrorCode);
                writer.WriteCompactString(partition.ErrorMessage);
                writer.WriteInt32(partition.StateEpoch);
                writer.WriteInt64(partition.StartOffset);

                writer.WriteVarInt(partition.StateBatches.Count + 1);
                foreach (var batch in partition.StateBatches)
                {
                    writer.WriteInt64(batch.FirstOffset);
                    writer.WriteInt64(batch.LastOffset);
                    writer.WriteInt8(batch.DeliveryState);
                    writer.WriteInt16(batch.DeliveryCount);
                    writer.WriteVarInt(0); // StateBatch tagged fields
                }

                writer.WriteVarInt(0); // Partition tagged fields
            }
            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static ReadShareGroupStateResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var resultCount = reader.ReadVarInt() - 1;
        var results = new List<ReadStateResult>(resultCount);
        for (int i = 0; i < resultCount; i++)
        {
            var topicId = reader.ReadUuid();
            var partCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionResult>(partCount);
            for (int j = 0; j < partCount; j++)
            {
                var partition = reader.ReadInt32();
                var errorCode = (ErrorCode)reader.ReadInt16();
                var errorMessage = reader.ReadCompactString();
                var stateEpoch = reader.ReadInt32();
                var startOffset = reader.ReadInt64();

                var batchCount = reader.ReadVarInt() - 1;
                var stateBatches = new List<StateBatch>(batchCount);
                for (int k = 0; k < batchCount; k++)
                {
                    var firstOffset = reader.ReadInt64();
                    var lastOffset = reader.ReadInt64();
                    var deliveryState = reader.ReadInt8();
                    var deliveryCount = reader.ReadInt16();
                    reader.SkipTaggedFields();

                    stateBatches.Add(new StateBatch
                    {
                        FirstOffset = firstOffset,
                        LastOffset = lastOffset,
                        DeliveryState = deliveryState,
                        DeliveryCount = deliveryCount
                    });
                }

                reader.SkipTaggedFields();

                partitions.Add(new PartitionResult
                {
                    Partition = partition,
                    ErrorCode = errorCode,
                    ErrorMessage = errorMessage,
                    StateEpoch = stateEpoch,
                    StartOffset = startOffset,
                    StateBatches = stateBatches
                });
            }
            reader.SkipTaggedFields();

            results.Add(new ReadStateResult
            {
                TopicId = topicId,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new ReadShareGroupStateResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            Results = results
        };
    }
}

// ────────────────────────────────────────────────────────────────
// WriteShareGroupState (API Key 85)
// ────────────────────────────────────────────────────────────────

/// <summary>
/// Kafka WriteShareGroupState request (API Key 85, v0-1).
/// KIP-932: Queues for Kafka — write share group partition state.
/// </summary>
public sealed class WriteShareGroupStateRequest : KafkaRequest
{
    /// <summary>The group identifier.</summary>
    public required string GroupId { get; init; }

    /// <summary>The data for the topics.</summary>
    public required List<WriteStateData> Topics { get; init; }

    public sealed class WriteStateData
    {
        /// <summary>The topic identifier.</summary>
        public required Guid TopicId { get; init; }

        /// <summary>The data for the partitions.</summary>
        public required List<PartitionData> Partitions { get; init; }
    }

    public sealed class PartitionData
    {
        /// <summary>The partition index.</summary>
        public int Partition { get; init; }

        /// <summary>The state epoch of the share-partition.</summary>
        public int StateEpoch { get; init; }

        /// <summary>The leader epoch of the share-partition.</summary>
        public int LeaderEpoch { get; init; }

        /// <summary>The share-partition start offset, or -1 if the start offset is not being written.</summary>
        public long StartOffset { get; init; }

        /// <summary>The number of offsets for which delivery has been completed (v1+). -1 if not provided.</summary>
        public int DeliveryCompleteCount { get; init; } = -1;

        /// <summary>The state batches for the share-partition.</summary>
        public required List<StateBatch> StateBatches { get; init; }
    }

    public sealed class StateBatch
    {
        /// <summary>The first offset of this state batch.</summary>
        public long FirstOffset { get; init; }

        /// <summary>The last offset of this state batch.</summary>
        public long LastOffset { get; init; }

        /// <summary>The delivery state — 0:Available, 2:Acked, 4:Archived.</summary>
        public sbyte DeliveryState { get; init; }

        /// <summary>The delivery count.</summary>
        public short DeliveryCount { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0+ is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteCompactString(GroupId);

        writer.WriteVarInt(Topics.Count + 1);
        foreach (var topic in Topics)
        {
            writer.WriteUuid(topic.TopicId);
            writer.WriteVarInt(topic.Partitions.Count + 1);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.Partition);
                writer.WriteInt32(partition.StateEpoch);
                writer.WriteInt32(partition.LeaderEpoch);
                writer.WriteInt64(partition.StartOffset);

                if (ApiVersion >= 1)
                {
                    writer.WriteInt32(partition.DeliveryCompleteCount);
                }

                writer.WriteVarInt(partition.StateBatches.Count + 1);
                foreach (var batch in partition.StateBatches)
                {
                    writer.WriteInt64(batch.FirstOffset);
                    writer.WriteInt64(batch.LastOffset);
                    writer.WriteInt8(batch.DeliveryState);
                    writer.WriteInt16(batch.DeliveryCount);
                    writer.WriteVarInt(0); // StateBatch tagged fields
                }

                writer.WriteVarInt(0); // Partition tagged fields
            }
            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static WriteShareGroupStateRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var groupId = reader.ReadCompactString() ?? "";

        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<WriteStateData>(topicCount);
        for (int i = 0; i < topicCount; i++)
        {
            var topicId = reader.ReadUuid();
            var partCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionData>(partCount);
            for (int j = 0; j < partCount; j++)
            {
                var partition = reader.ReadInt32();
                var stateEpoch = reader.ReadInt32();
                var leaderEpoch = reader.ReadInt32();
                var startOffset = reader.ReadInt64();

                var deliveryCompleteCount = -1;
                if (apiVersion >= 1)
                {
                    deliveryCompleteCount = reader.ReadInt32();
                }

                var batchCount = reader.ReadVarInt() - 1;
                var stateBatches = new List<StateBatch>(batchCount);
                for (int k = 0; k < batchCount; k++)
                {
                    var firstOffset = reader.ReadInt64();
                    var lastOffset = reader.ReadInt64();
                    var deliveryState = reader.ReadInt8();
                    var deliveryCount = reader.ReadInt16();
                    reader.SkipTaggedFields();

                    stateBatches.Add(new StateBatch
                    {
                        FirstOffset = firstOffset,
                        LastOffset = lastOffset,
                        DeliveryState = deliveryState,
                        DeliveryCount = deliveryCount
                    });
                }

                reader.SkipTaggedFields();

                partitions.Add(new PartitionData
                {
                    Partition = partition,
                    StateEpoch = stateEpoch,
                    LeaderEpoch = leaderEpoch,
                    StartOffset = startOffset,
                    DeliveryCompleteCount = deliveryCompleteCount,
                    StateBatches = stateBatches
                });
            }
            reader.SkipTaggedFields();

            topics.Add(new WriteStateData
            {
                TopicId = topicId,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new WriteShareGroupStateRequest
        {
            ApiKey = ApiKey.WriteShareGroupState,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            GroupId = groupId,
            Topics = topics
        };
    }
}

/// <summary>
/// Kafka WriteShareGroupState response (API Key 85, v0-1).
/// </summary>
public sealed class WriteShareGroupStateResponse : KafkaResponse
{
    /// <summary>The write results.</summary>
    public required List<WriteStateResult> Results { get; init; }

    public sealed class WriteStateResult
    {
        /// <summary>The topic identifier.</summary>
        public required Guid TopicId { get; init; }

        /// <summary>The results for the partitions.</summary>
        public required List<PartitionResult> Partitions { get; init; }
    }

    public sealed class PartitionResult
    {
        /// <summary>The partition index.</summary>
        public int Partition { get; init; }

        /// <summary>The error code, or 0 if there was no error.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The error message, or null if there was no error.</summary>
        public string? ErrorMessage { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteVarInt(Results.Count + 1);
        foreach (var result in Results)
        {
            writer.WriteUuid(result.TopicId);
            writer.WriteVarInt(result.Partitions.Count + 1);
            foreach (var partition in result.Partitions)
            {
                writer.WriteInt32(partition.Partition);
                writer.WriteInt16((short)partition.ErrorCode);
                writer.WriteCompactString(partition.ErrorMessage);
                writer.WriteVarInt(0); // Partition tagged fields
            }
            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static WriteShareGroupStateResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var resultCount = reader.ReadVarInt() - 1;
        var results = new List<WriteStateResult>(resultCount);
        for (int i = 0; i < resultCount; i++)
        {
            var topicId = reader.ReadUuid();
            var partCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionResult>(partCount);
            for (int j = 0; j < partCount; j++)
            {
                var partition = reader.ReadInt32();
                var errorCode = (ErrorCode)reader.ReadInt16();
                var errorMessage = reader.ReadCompactString();
                reader.SkipTaggedFields();

                partitions.Add(new PartitionResult
                {
                    Partition = partition,
                    ErrorCode = errorCode,
                    ErrorMessage = errorMessage
                });
            }
            reader.SkipTaggedFields();

            results.Add(new WriteStateResult
            {
                TopicId = topicId,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new WriteShareGroupStateResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            Results = results
        };
    }
}

// ────────────────────────────────────────────────────────────────
// DeleteShareGroupState (API Key 86)
// ────────────────────────────────────────────────────────────────

/// <summary>
/// Kafka DeleteShareGroupState request (API Key 86, v0-0).
/// KIP-932: Queues for Kafka — delete share group partition state.
/// </summary>
public sealed class DeleteShareGroupStateRequest : KafkaRequest
{
    /// <summary>The group identifier.</summary>
    public required string GroupId { get; init; }

    /// <summary>The data for the topics.</summary>
    public required List<DeleteStateData> Topics { get; init; }

    public sealed class DeleteStateData
    {
        /// <summary>The topic identifier.</summary>
        public required Guid TopicId { get; init; }

        /// <summary>The data for the partitions.</summary>
        public required List<PartitionData> Partitions { get; init; }
    }

    public sealed class PartitionData
    {
        /// <summary>The partition index.</summary>
        public int Partition { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteCompactString(GroupId);

        writer.WriteVarInt(Topics.Count + 1);
        foreach (var topic in Topics)
        {
            writer.WriteUuid(topic.TopicId);
            writer.WriteVarInt(topic.Partitions.Count + 1);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.Partition);
                writer.WriteVarInt(0); // Partition tagged fields
            }
            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static DeleteShareGroupStateRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var groupId = reader.ReadCompactString() ?? "";

        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<DeleteStateData>(topicCount);
        for (int i = 0; i < topicCount; i++)
        {
            var topicId = reader.ReadUuid();
            var partCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionData>(partCount);
            for (int j = 0; j < partCount; j++)
            {
                var partition = reader.ReadInt32();
                reader.SkipTaggedFields();

                partitions.Add(new PartitionData
                {
                    Partition = partition
                });
            }
            reader.SkipTaggedFields();

            topics.Add(new DeleteStateData
            {
                TopicId = topicId,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new DeleteShareGroupStateRequest
        {
            ApiKey = ApiKey.DeleteShareGroupState,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            GroupId = groupId,
            Topics = topics
        };
    }
}

/// <summary>
/// Kafka DeleteShareGroupState response (API Key 86, v0-0).
/// </summary>
public sealed class DeleteShareGroupStateResponse : KafkaResponse
{
    /// <summary>The delete results.</summary>
    public required List<DeleteStateResult> Results { get; init; }

    public sealed class DeleteStateResult
    {
        /// <summary>The topic identifier.</summary>
        public required Guid TopicId { get; init; }

        /// <summary>The results for the partitions.</summary>
        public required List<PartitionResult> Partitions { get; init; }
    }

    public sealed class PartitionResult
    {
        /// <summary>The partition index.</summary>
        public int Partition { get; init; }

        /// <summary>The error code, or 0 if there was no error.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The error message, or null if there was no error.</summary>
        public string? ErrorMessage { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteVarInt(Results.Count + 1);
        foreach (var result in Results)
        {
            writer.WriteUuid(result.TopicId);
            writer.WriteVarInt(result.Partitions.Count + 1);
            foreach (var partition in result.Partitions)
            {
                writer.WriteInt32(partition.Partition);
                writer.WriteInt16((short)partition.ErrorCode);
                writer.WriteCompactString(partition.ErrorMessage);
                writer.WriteVarInt(0); // Partition tagged fields
            }
            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static DeleteShareGroupStateResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var resultCount = reader.ReadVarInt() - 1;
        var results = new List<DeleteStateResult>(resultCount);
        for (int i = 0; i < resultCount; i++)
        {
            var topicId = reader.ReadUuid();
            var partCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionResult>(partCount);
            for (int j = 0; j < partCount; j++)
            {
                var partition = reader.ReadInt32();
                var errorCode = (ErrorCode)reader.ReadInt16();
                var errorMessage = reader.ReadCompactString();
                reader.SkipTaggedFields();

                partitions.Add(new PartitionResult
                {
                    Partition = partition,
                    ErrorCode = errorCode,
                    ErrorMessage = errorMessage
                });
            }
            reader.SkipTaggedFields();

            results.Add(new DeleteStateResult
            {
                TopicId = topicId,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new DeleteShareGroupStateResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            Results = results
        };
    }
}

// ────────────────────────────────────────────────────────────────
// ReadShareGroupStateSummary (API Key 87)
// ────────────────────────────────────────────────────────────────

/// <summary>
/// Kafka ReadShareGroupStateSummary request (API Key 87, v0-1).
/// KIP-932: Queues for Kafka — read share group state summary.
/// </summary>
public sealed class ReadShareGroupStateSummaryRequest : KafkaRequest
{
    /// <summary>The group identifier.</summary>
    public required string GroupId { get; init; }

    /// <summary>The data for the topics.</summary>
    public required List<ReadStateSummaryData> Topics { get; init; }

    public sealed class ReadStateSummaryData
    {
        /// <summary>The topic identifier.</summary>
        public required Guid TopicId { get; init; }

        /// <summary>The data for the partitions.</summary>
        public required List<PartitionData> Partitions { get; init; }
    }

    public sealed class PartitionData
    {
        /// <summary>The partition index.</summary>
        public int Partition { get; init; }

        /// <summary>The leader epoch of the share-partition.</summary>
        public int LeaderEpoch { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0+ is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteCompactString(GroupId);

        writer.WriteVarInt(Topics.Count + 1);
        foreach (var topic in Topics)
        {
            writer.WriteUuid(topic.TopicId);
            writer.WriteVarInt(topic.Partitions.Count + 1);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.Partition);
                writer.WriteInt32(partition.LeaderEpoch);
                writer.WriteVarInt(0); // Partition tagged fields
            }
            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static ReadShareGroupStateSummaryRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var groupId = reader.ReadCompactString() ?? "";

        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<ReadStateSummaryData>(topicCount);
        for (int i = 0; i < topicCount; i++)
        {
            var topicId = reader.ReadUuid();
            var partCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionData>(partCount);
            for (int j = 0; j < partCount; j++)
            {
                var partition = reader.ReadInt32();
                var leaderEpoch = reader.ReadInt32();
                reader.SkipTaggedFields();

                partitions.Add(new PartitionData
                {
                    Partition = partition,
                    LeaderEpoch = leaderEpoch
                });
            }
            reader.SkipTaggedFields();

            topics.Add(new ReadStateSummaryData
            {
                TopicId = topicId,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new ReadShareGroupStateSummaryRequest
        {
            ApiKey = ApiKey.ReadShareGroupStateSummary,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            GroupId = groupId,
            Topics = topics
        };
    }
}

/// <summary>
/// Kafka ReadShareGroupStateSummary response (API Key 87, v0-1).
/// </summary>
public sealed class ReadShareGroupStateSummaryResponse : KafkaResponse
{
    /// <summary>The read results.</summary>
    public required List<ReadStateSummaryResult> Results { get; init; }

    public sealed class ReadStateSummaryResult
    {
        /// <summary>The topic identifier.</summary>
        public required Guid TopicId { get; init; }

        /// <summary>The results for the partitions.</summary>
        public required List<PartitionResult> Partitions { get; init; }
    }

    public sealed class PartitionResult
    {
        /// <summary>The partition index.</summary>
        public int Partition { get; init; }

        /// <summary>The error code, or 0 if there was no error.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The error message, or null if there was no error.</summary>
        public string? ErrorMessage { get; init; }

        /// <summary>The state epoch of the share-partition.</summary>
        public int StateEpoch { get; init; }

        /// <summary>The leader epoch of the share-partition.</summary>
        public int LeaderEpoch { get; init; }

        /// <summary>The share-partition start offset.</summary>
        public long StartOffset { get; init; }

        /// <summary>The number of offsets for which delivery has been completed (v1+). -1 if not provided.</summary>
        public int DeliveryCompleteCount { get; init; } = -1;
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteVarInt(Results.Count + 1);
        foreach (var result in Results)
        {
            writer.WriteUuid(result.TopicId);
            writer.WriteVarInt(result.Partitions.Count + 1);
            foreach (var partition in result.Partitions)
            {
                writer.WriteInt32(partition.Partition);
                writer.WriteInt16((short)partition.ErrorCode);
                writer.WriteCompactString(partition.ErrorMessage);
                writer.WriteInt32(partition.StateEpoch);
                writer.WriteInt32(partition.LeaderEpoch);
                writer.WriteInt64(partition.StartOffset);

                if (ApiVersion >= 1)
                {
                    writer.WriteInt32(partition.DeliveryCompleteCount);
                }

                writer.WriteVarInt(0); // Partition tagged fields
            }
            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static ReadShareGroupStateSummaryResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var resultCount = reader.ReadVarInt() - 1;
        var results = new List<ReadStateSummaryResult>(resultCount);
        for (int i = 0; i < resultCount; i++)
        {
            var topicId = reader.ReadUuid();
            var partCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionResult>(partCount);
            for (int j = 0; j < partCount; j++)
            {
                var partition = reader.ReadInt32();
                var errorCode = (ErrorCode)reader.ReadInt16();
                var errorMessage = reader.ReadCompactString();
                var stateEpoch = reader.ReadInt32();
                var leaderEpoch = reader.ReadInt32();
                var startOffset = reader.ReadInt64();

                var deliveryCompleteCount = -1;
                if (apiVersion >= 1)
                {
                    deliveryCompleteCount = reader.ReadInt32();
                }

                reader.SkipTaggedFields();

                partitions.Add(new PartitionResult
                {
                    Partition = partition,
                    ErrorCode = errorCode,
                    ErrorMessage = errorMessage,
                    StateEpoch = stateEpoch,
                    LeaderEpoch = leaderEpoch,
                    StartOffset = startOffset,
                    DeliveryCompleteCount = deliveryCompleteCount
                });
            }
            reader.SkipTaggedFields();

            results.Add(new ReadStateSummaryResult
            {
                TopicId = topicId,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new ReadShareGroupStateSummaryResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            Results = results
        };
    }
}
