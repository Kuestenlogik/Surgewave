namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka ControlledShutdown request (v0-3) - Inter-broker API
/// Sent by a broker to the controller when initiating a graceful shutdown.
/// This allows the controller to move leadership away from the shutting down broker.
/// </summary>
public sealed class ControlledShutdownRequest : KafkaRequest
{
    /// <summary>The id of the broker for which controlled shutdown has been requested.</summary>
    public required int BrokerId { get; init; }
    /// <summary>The broker epoch (v2+).</summary>
    public long BrokerEpoch { get; init; } = -1;

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 3;

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

        writer.WriteInt32(BrokerId);

        // BrokerEpoch (v2+)
        if (ApiVersion >= 2)
        {
            writer.WriteInt64(BrokerEpoch);
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static ControlledShutdownRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var brokerId = reader.ReadInt32();
        var brokerEpoch = apiVersion >= 2 ? reader.ReadInt64() : -1L;

        bool isFlexible = apiVersion >= 3;
        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new ControlledShutdownRequest
        {
            ApiKey = ApiKey.ControlledShutdown,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            BrokerId = brokerId,
            BrokerEpoch = brokerEpoch
        };
    }
}

/// <summary>
/// Kafka ControlledShutdown response (v0-3)
/// </summary>
public sealed class ControlledShutdownResponse : KafkaResponse
{
    /// <summary>The top-level error code.</summary>
    public ErrorCode ErrorCode { get; init; }
    /// <summary>The partitions that the broker still leads.</summary>
    public required List<RemainingPartition> RemainingPartitions { get; init; }

    public sealed class RemainingPartition
    {
        /// <summary>The name of the topic.</summary>
        public required string TopicName { get; init; }
        /// <summary>The index of the partition.</summary>
        public required int PartitionIndex { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 3;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        writer.WriteInt16((short)ErrorCode);

        if (isFlexible)
        {
            writer.WriteVarInt(RemainingPartitions.Count + 1);
        }
        else
        {
            writer.WriteInt32(RemainingPartitions.Count);
        }

        foreach (var partition in RemainingPartitions)
        {
            if (isFlexible)
                writer.WriteCompactString(partition.TopicName);
            else
                writer.WriteString(partition.TopicName);
            writer.WriteInt32(partition.PartitionIndex);

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

    public static ControlledShutdownResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        bool isFlexible = apiVersion >= 3;

        if (isFlexible)
        {
            reader.SkipTaggedFields(); // Response header tagged fields
        }

        var errorCode = (ErrorCode)reader.ReadInt16();

        int partCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
        var remainingPartitions = new List<RemainingPartition>(partCount);

        for (int p = 0; p < partCount; p++)
        {
            var topicName = isFlexible ? reader.ReadCompactString()! : reader.ReadString()!;
            var partitionIndex = reader.ReadInt32();

            if (isFlexible)
            {
                reader.SkipTaggedFields();
            }

            remainingPartitions.Add(new RemainingPartition
            {
                TopicName = topicName,
                PartitionIndex = partitionIndex
            });
        }

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new ControlledShutdownResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = errorCode,
            RemainingPartitions = remainingPartitions
        };
    }
}
