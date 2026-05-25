namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka DescribeProducers request (API Key 61, v0-0).
/// Describes the active producers for a set of partitions.
/// Used for monitoring transaction state and producer activity.
/// </summary>
public sealed class DescribeProducersRequest : KafkaRequest
{
    /// <summary>The topics to describe producers for.</summary>
    public required List<TopicRequest> Topics { get; init; }

    public sealed class TopicRequest
    {
        /// <summary>The topic name.</summary>
        public required string Name { get; init; }

        /// <summary>The partition indices to describe producers for.</summary>
        public required List<int> PartitionIndexes { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // All versions are flexible (v0+)
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        // Topics array (compact)
        writer.WriteVarInt(Topics.Count + 1);
        foreach (var topic in Topics)
        {
            writer.WriteCompactString(topic.Name);
            writer.WriteVarInt(topic.PartitionIndexes.Count + 1);
            foreach (var partition in topic.PartitionIndexes)
            {
                writer.WriteInt32(partition);
            }
            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static DescribeProducersRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<TopicRequest>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var topicName = reader.ReadCompactString() ?? "";
            var partitionCount = reader.ReadVarInt() - 1;
            var partitions = new List<int>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                partitions.Add(reader.ReadInt32());
            }

            reader.SkipTaggedFields();

            topics.Add(new TopicRequest
            {
                Name = topicName,
                PartitionIndexes = partitions
            });
        }

        reader.SkipTaggedFields();

        return new DescribeProducersRequest
        {
            ApiKey = ApiKey.DescribeProducers,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Topics = topics
        };
    }
}

/// <summary>
/// Kafka DescribeProducers response (API Key 61, v0-0).
/// </summary>
public sealed class DescribeProducersResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>Each topic in the response.</summary>
    public required List<TopicResponse> Topics { get; init; }

    public sealed class TopicResponse
    {
        /// <summary>The topic name.</summary>
        public required string Name { get; init; }

        /// <summary>Each partition in the response.</summary>
        public required List<PartitionResponse> Partitions { get; init; }
    }

    public sealed class PartitionResponse
    {
        /// <summary>The partition index.</summary>
        public required int PartitionIndex { get; init; }

        /// <summary>The partition error code, or 0 if there was no error.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The partition error message, which may be null if no additional details are available.</summary>
        public string? ErrorMessage { get; init; }

        /// <summary>The active producers.</summary>
        public required List<ProducerState> ActiveProducers { get; init; }
    }

    public sealed class ProducerState
    {
        /// <summary>The producer ID.</summary>
        public required long ProducerId { get; init; }

        /// <summary>The producer epoch.</summary>
        public required int ProducerEpoch { get; init; }

        /// <summary>The last sequence number written by this producer to this partition.</summary>
        public int LastSequence { get; init; } = -1;

        /// <summary>The last timestamp written by this producer to this partition.</summary>
        public long LastTimestamp { get; init; } = -1;

        /// <summary>
        /// The ID of the current coordinator transaction if one exists, or -1.
        /// </summary>
        public int CoordinatorEpoch { get; init; } = -1;

        /// <summary>
        /// The current offset of the transaction. Present only if there is an active transaction.
        /// </summary>
        public long CurrentTxnStartOffset { get; init; } = -1;
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // All versions are flexible
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);

        // Topics array (compact)
        writer.WriteVarInt(Topics.Count + 1);
        foreach (var topic in Topics)
        {
            writer.WriteCompactString(topic.Name);
            writer.WriteVarInt(topic.Partitions.Count + 1);

            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt16((short)partition.ErrorCode);
                writer.WriteCompactString(partition.ErrorMessage);

                // ActiveProducers array (compact)
                writer.WriteVarInt(partition.ActiveProducers.Count + 1);
                foreach (var producer in partition.ActiveProducers)
                {
                    writer.WriteInt64(producer.ProducerId);
                    writer.WriteInt32(producer.ProducerEpoch);
                    writer.WriteInt32(producer.LastSequence);
                    writer.WriteInt64(producer.LastTimestamp);
                    writer.WriteInt32(producer.CoordinatorEpoch);
                    writer.WriteInt64(producer.CurrentTxnStartOffset);
                    writer.WriteVarInt(0); // Producer tagged fields
                }

                writer.WriteVarInt(0); // Partition tagged fields
            }

            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static DescribeProducersResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();

        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<TopicResponse>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var topicName = reader.ReadCompactString() ?? "";
            var partitionCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionResponse>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                var partitionIndex = reader.ReadInt32();
                var errorCode = (ErrorCode)reader.ReadInt16();
                var errorMessage = reader.ReadCompactString();

                var producerCount = reader.ReadVarInt() - 1;
                var producers = new List<ProducerState>(producerCount);

                for (int k = 0; k < producerCount; k++)
                {
                    var producerId = reader.ReadInt64();
                    var producerEpoch = reader.ReadInt32();
                    var lastSequence = reader.ReadInt32();
                    var lastTimestamp = reader.ReadInt64();
                    var coordinatorEpoch = reader.ReadInt32();
                    var currentTxnStartOffset = reader.ReadInt64();
                    reader.SkipTaggedFields();

                    producers.Add(new ProducerState
                    {
                        ProducerId = producerId,
                        ProducerEpoch = producerEpoch,
                        LastSequence = lastSequence,
                        LastTimestamp = lastTimestamp,
                        CoordinatorEpoch = coordinatorEpoch,
                        CurrentTxnStartOffset = currentTxnStartOffset
                    });
                }

                reader.SkipTaggedFields();

                partitions.Add(new PartitionResponse
                {
                    PartitionIndex = partitionIndex,
                    ErrorCode = errorCode,
                    ErrorMessage = errorMessage,
                    ActiveProducers = producers
                });
            }

            reader.SkipTaggedFields();

            topics.Add(new TopicResponse
            {
                Name = topicName,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new DescribeProducersResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            Topics = topics
        };
    }
}
