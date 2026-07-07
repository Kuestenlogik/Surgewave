namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka AlterPartition request (API Key 56, v0-3).
/// Used by brokers to notify the controller about partition changes.
/// </summary>
public sealed class AlterPartitionRequest : KafkaRequest
{
    /// <summary>The ID of the requesting broker.</summary>
    public int BrokerId { get; init; }

    /// <summary>The epoch of the requesting broker.</summary>
    public long BrokerEpoch { get; init; } = -1;

    /// <summary>Each topic to alter.</summary>
    public required List<TopicData> Topics { get; init; }

    public sealed class TopicData
    {
        /// <summary>The topic name (v0-1).</summary>
        public string? TopicName { get; init; }

        /// <summary>The topic ID (v2+).</summary>
        public Guid TopicId { get; init; }

        /// <summary>Each partition to alter.</summary>
        public required List<PartitionData> Partitions { get; init; }
    }

    public sealed class PartitionData
    {
        /// <summary>The partition index.</summary>
        public int PartitionIndex { get; init; }

        /// <summary>The leader epoch of this partition.</summary>
        public int LeaderEpoch { get; init; }

        /// <summary>The ISR for this partition (v0-2).</summary>
        public List<int>? NewIsr { get; init; }

        /// <summary>The expected epoch of the ISR (v0-2).</summary>
        public int LeaderRecoveryState { get; init; }

        /// <summary>The partition epoch (v0-2).</summary>
        public int PartitionEpoch { get; init; }

        /// <summary>The new ISR with epochs (v3+).</summary>
        public List<BrokerState>? NewIsrWithEpochs { get; init; }
    }

    public sealed class BrokerState
    {
        /// <summary>The ID of the broker.</summary>
        public int BrokerId { get; init; }

        /// <summary>The epoch of the broker.</summary>
        public long BrokerEpoch { get; init; } = -1;
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0+ is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        // ClientId in a Kafka RequestHeader is ALWAYS a regular (int16-length)
        // NULLABLE_STRING even in flexible request versions — only the header's
        // trailing tagged fields are flexible. Writing it compact makes the
        // header unparseable by ReadRequestHeader (which uses ReadString), so
        // the inter-broker AlterPartition send would never round-trip (#69,
        // same fix as LeaderAndIsr/StopReplica/UpdateMetadata).
        writer.WriteString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteInt32(BrokerId);
        writer.WriteInt64(BrokerEpoch);

        writer.WriteVarInt(Topics.Count + 1);
        foreach (var topic in Topics)
        {
            if (ApiVersion >= 2)
            {
                writer.WriteUuid(topic.TopicId);
            }
            else
            {
                writer.WriteCompactString(topic.TopicName);
            }

            writer.WriteVarInt(topic.Partitions.Count + 1);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt32(partition.LeaderEpoch);

                if (ApiVersion >= 3)
                {
                    writer.WriteVarInt((partition.NewIsrWithEpochs?.Count ?? 0) + 1);
                    if (partition.NewIsrWithEpochs != null)
                    {
                        foreach (var broker in partition.NewIsrWithEpochs)
                        {
                            writer.WriteInt32(broker.BrokerId);
                            writer.WriteInt64(broker.BrokerEpoch);
                            writer.WriteVarInt(0); // Broker state tagged fields
                        }
                    }
                }
                else
                {
                    writer.WriteVarInt((partition.NewIsr?.Count ?? 0) + 1);
                    if (partition.NewIsr != null)
                    {
                        foreach (var replica in partition.NewIsr)
                        {
                            writer.WriteInt32(replica);
                        }
                    }
                }

                writer.WriteInt32(partition.LeaderRecoveryState);
                writer.WriteInt32(partition.PartitionEpoch);
                writer.WriteVarInt(0); // Partition tagged fields
            }

            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static AlterPartitionRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var brokerId = reader.ReadInt32();
        var brokerEpoch = reader.ReadInt64();

        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<TopicData>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            string? topicName = null;
            Guid topicId = Guid.Empty;

            if (apiVersion >= 2)
            {
                topicId = reader.ReadUuid();
            }
            else
            {
                topicName = reader.ReadCompactString();
            }

            var partitionCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionData>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                var partitionIndex = reader.ReadInt32();
                var leaderEpoch = reader.ReadInt32();

                List<int>? newIsr = null;
                List<BrokerState>? newIsrWithEpochs = null;

                if (apiVersion >= 3)
                {
                    var brokerCount = reader.ReadVarInt() - 1;
                    newIsrWithEpochs = new List<BrokerState>(brokerCount);
                    for (int k = 0; k < brokerCount; k++)
                    {
                        var bId = reader.ReadInt32();
                        var bEpoch = reader.ReadInt64();
                        reader.SkipTaggedFields();

                        newIsrWithEpochs.Add(new BrokerState
                        {
                            BrokerId = bId,
                            BrokerEpoch = bEpoch
                        });
                    }
                }
                else
                {
                    var isrCount = reader.ReadVarInt() - 1;
                    newIsr = new List<int>(isrCount);
                    for (int k = 0; k < isrCount; k++)
                    {
                        newIsr.Add(reader.ReadInt32());
                    }
                }

                var leaderRecoveryState = reader.ReadInt32();
                var partitionEpoch = reader.ReadInt32();
                reader.SkipTaggedFields();

                partitions.Add(new PartitionData
                {
                    PartitionIndex = partitionIndex,
                    LeaderEpoch = leaderEpoch,
                    NewIsr = newIsr,
                    NewIsrWithEpochs = newIsrWithEpochs,
                    LeaderRecoveryState = leaderRecoveryState,
                    PartitionEpoch = partitionEpoch
                });
            }

            reader.SkipTaggedFields();

            topics.Add(new TopicData
            {
                TopicName = topicName,
                TopicId = topicId,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new AlterPartitionRequest
        {
            ApiKey = ApiKey.AlterPartition,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            BrokerId = brokerId,
            BrokerEpoch = brokerEpoch,
            Topics = topics
        };
    }
}

/// <summary>
/// Kafka AlterPartition response (API Key 56, v0-3).
/// </summary>
public sealed class AlterPartitionResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The top level response error code.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>Each topic.</summary>
    public required List<TopicData> Topics { get; init; }

    public sealed class TopicData
    {
        /// <summary>The topic name (v0-1).</summary>
        public string? TopicName { get; init; }

        /// <summary>The topic ID (v2+).</summary>
        public Guid TopicId { get; init; }

        /// <summary>Each partition.</summary>
        public required List<PartitionData> Partitions { get; init; }
    }

    public sealed class PartitionData
    {
        /// <summary>The partition index.</summary>
        public int PartitionIndex { get; init; }

        /// <summary>The partition level error code.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The broker ID of the leader.</summary>
        public int LeaderId { get; init; }

        /// <summary>The leader epoch.</summary>
        public int LeaderEpoch { get; init; }

        /// <summary>The ISR (v0-2).</summary>
        public List<int>? Isr { get; init; }

        /// <summary>The ISR with epochs (v3+).</summary>
        public List<BrokerState>? IsrWithEpochs { get; init; }

        /// <summary>The leader recovery state (1 = recovering, 0 = recovered).</summary>
        public int LeaderRecoveryState { get; init; }

        /// <summary>The current epoch of the partition.</summary>
        public int PartitionEpoch { get; init; }
    }

    public sealed class BrokerState
    {
        /// <summary>The ID of the broker.</summary>
        public int BrokerId { get; init; }

        /// <summary>The epoch of the broker.</summary>
        public long BrokerEpoch { get; init; } = -1;
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);

        writer.WriteVarInt(Topics.Count + 1);
        foreach (var topic in Topics)
        {
            if (ApiVersion >= 2)
            {
                writer.WriteUuid(topic.TopicId);
            }
            else
            {
                writer.WriteCompactString(topic.TopicName);
            }

            writer.WriteVarInt(topic.Partitions.Count + 1);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt16((short)partition.ErrorCode);
                writer.WriteInt32(partition.LeaderId);
                writer.WriteInt32(partition.LeaderEpoch);

                if (ApiVersion >= 3)
                {
                    writer.WriteVarInt((partition.IsrWithEpochs?.Count ?? 0) + 1);
                    if (partition.IsrWithEpochs != null)
                    {
                        foreach (var broker in partition.IsrWithEpochs)
                        {
                            writer.WriteInt32(broker.BrokerId);
                            writer.WriteInt64(broker.BrokerEpoch);
                            writer.WriteVarInt(0); // Broker state tagged fields
                        }
                    }
                }
                else
                {
                    writer.WriteVarInt((partition.Isr?.Count ?? 0) + 1);
                    if (partition.Isr != null)
                    {
                        foreach (var replica in partition.Isr)
                        {
                            writer.WriteInt32(replica);
                        }
                    }
                }

                writer.WriteInt32(partition.LeaderRecoveryState);
                writer.WriteInt32(partition.PartitionEpoch);
                writer.WriteVarInt(0); // Partition tagged fields
            }

            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static AlterPartitionResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();

        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<TopicData>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            string? topicName = null;
            Guid topicId = Guid.Empty;

            if (apiVersion >= 2)
            {
                topicId = reader.ReadUuid();
            }
            else
            {
                topicName = reader.ReadCompactString();
            }

            var partitionCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionData>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                var partitionIndex = reader.ReadInt32();
                var partErrorCode = (ErrorCode)reader.ReadInt16();
                var leaderId = reader.ReadInt32();
                var leaderEpoch = reader.ReadInt32();

                List<int>? isr = null;
                List<BrokerState>? isrWithEpochs = null;

                if (apiVersion >= 3)
                {
                    var brokerCount = reader.ReadVarInt() - 1;
                    isrWithEpochs = new List<BrokerState>(brokerCount);
                    for (int k = 0; k < brokerCount; k++)
                    {
                        var bId = reader.ReadInt32();
                        var bEpoch = reader.ReadInt64();
                        reader.SkipTaggedFields();

                        isrWithEpochs.Add(new BrokerState
                        {
                            BrokerId = bId,
                            BrokerEpoch = bEpoch
                        });
                    }
                }
                else
                {
                    var isrCount = reader.ReadVarInt() - 1;
                    isr = new List<int>(isrCount);
                    for (int k = 0; k < isrCount; k++)
                    {
                        isr.Add(reader.ReadInt32());
                    }
                }

                var leaderRecoveryState = reader.ReadInt32();
                var partitionEpoch = reader.ReadInt32();
                reader.SkipTaggedFields();

                partitions.Add(new PartitionData
                {
                    PartitionIndex = partitionIndex,
                    ErrorCode = partErrorCode,
                    LeaderId = leaderId,
                    LeaderEpoch = leaderEpoch,
                    Isr = isr,
                    IsrWithEpochs = isrWithEpochs,
                    LeaderRecoveryState = leaderRecoveryState,
                    PartitionEpoch = partitionEpoch
                });
            }

            reader.SkipTaggedFields();

            topics.Add(new TopicData
            {
                TopicName = topicName,
                TopicId = topicId,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new AlterPartitionResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            Topics = topics
        };
    }
}
