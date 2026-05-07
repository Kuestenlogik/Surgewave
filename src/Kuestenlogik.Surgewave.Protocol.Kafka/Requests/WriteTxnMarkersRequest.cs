namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka WriteTxnMarkers request (API Key 27, v0-1).
/// Used by the transaction coordinator to write transaction markers
/// (COMMIT or ABORT) to multiple partitions atomically.
/// This is an internal broker-to-broker API.
/// </summary>
public sealed class WriteTxnMarkersRequest : KafkaRequest
{
    /// <summary>The transaction markers to be written.</summary>
    public required List<MarkerEntry> Markers { get; init; }

    public sealed class MarkerEntry
    {
        /// <summary>The producer ID.</summary>
        public required long ProducerId { get; init; }

        /// <summary>The producer epoch.</summary>
        public required short ProducerEpoch { get; init; }

        /// <summary>
        /// The result of the transaction to write to the partitions
        /// (false = abort, true = commit).
        /// </summary>
        public required bool TransactionResult { get; init; }

        /// <summary>The partitions to write markers for.</summary>
        public required List<TopicPartition> Topics { get; init; }

        /// <summary>
        /// Epoch associated with the transaction state partition hosted by
        /// this transaction coordinator (v1+).
        /// </summary>
        public int CoordinatorEpoch { get; init; }
    }

    public sealed class TopicPartition
    {
        /// <summary>The topic name.</summary>
        public required string Topic { get; init; }

        /// <summary>The partition indices to write markers for.</summary>
        public required List<int> PartitionIndexes { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        var isFlexible = ApiVersion >= 1;

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

        // Markers array
        if (isFlexible)
        {
            writer.WriteVarInt(Markers.Count + 1);
        }
        else
        {
            writer.WriteInt32(Markers.Count);
        }

        foreach (var marker in Markers)
        {
            writer.WriteInt64(marker.ProducerId);
            writer.WriteInt16(marker.ProducerEpoch);

            // v1+: CoordinatorEpoch
            if (ApiVersion >= 1)
            {
                writer.WriteInt32(marker.CoordinatorEpoch);
            }

            writer.WriteBoolean(marker.TransactionResult);

            // Topics array
            if (isFlexible)
            {
                writer.WriteVarInt(marker.Topics.Count + 1);
            }
            else
            {
                writer.WriteInt32(marker.Topics.Count);
            }

            foreach (var topic in marker.Topics)
            {
                if (isFlexible)
                {
                    writer.WriteCompactString(topic.Topic);
                    writer.WriteVarInt(topic.PartitionIndexes.Count + 1);
                }
                else
                {
                    writer.WriteString(topic.Topic);
                    writer.WriteInt32(topic.PartitionIndexes.Count);
                }

                foreach (var partitionIndex in topic.PartitionIndexes)
                {
                    writer.WriteInt32(partitionIndex);
                }

                if (isFlexible)
                {
                    writer.WriteVarInt(0); // Topic tagged fields
                }
            }

            if (isFlexible)
            {
                writer.WriteVarInt(0); // Marker tagged fields
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static WriteTxnMarkersRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var isFlexible = apiVersion >= 1;

        var markerCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
        var markers = new List<MarkerEntry>(markerCount);

        for (int i = 0; i < markerCount; i++)
        {
            var producerId = reader.ReadInt64();
            var producerEpoch = reader.ReadInt16();
            var coordinatorEpoch = apiVersion >= 1 ? reader.ReadInt32() : 0;
            var transactionResult = reader.ReadBoolean();

            var topicCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
            var topics = new List<TopicPartition>(topicCount);

            for (int j = 0; j < topicCount; j++)
            {
                var topicName = isFlexible ? reader.ReadCompactString() ?? "" : reader.ReadString() ?? "";
                var partitionCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
                var partitionIndexes = new List<int>(partitionCount);

                for (int k = 0; k < partitionCount; k++)
                {
                    partitionIndexes.Add(reader.ReadInt32());
                }

                if (isFlexible)
                {
                    reader.SkipTaggedFields();
                }

                topics.Add(new TopicPartition
                {
                    Topic = topicName,
                    PartitionIndexes = partitionIndexes
                });
            }

            if (isFlexible)
            {
                reader.SkipTaggedFields();
            }

            markers.Add(new MarkerEntry
            {
                ProducerId = producerId,
                ProducerEpoch = producerEpoch,
                CoordinatorEpoch = coordinatorEpoch,
                TransactionResult = transactionResult,
                Topics = topics
            });
        }

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new WriteTxnMarkersRequest
        {
            ApiKey = ApiKey.WriteTxnMarkers,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Markers = markers
        };
    }
}

/// <summary>
/// Kafka WriteTxnMarkers response (API Key 27, v0-1).
/// </summary>
public sealed class WriteTxnMarkersResponse : KafkaResponse
{
    /// <summary>The results for writing transaction markers.</summary>
    public required List<MarkerResult> Markers { get; init; }

    public sealed class MarkerResult
    {
        /// <summary>The producer ID.</summary>
        public required long ProducerId { get; init; }

        /// <summary>The results by topic.</summary>
        public required List<TopicResult> Topics { get; init; }
    }

    public sealed class TopicResult
    {
        /// <summary>The topic name.</summary>
        public required string Topic { get; init; }

        /// <summary>The results by partition.</summary>
        public required List<PartitionResult> Partitions { get; init; }
    }

    public sealed class PartitionResult
    {
        /// <summary>The partition index.</summary>
        public required int PartitionIndex { get; init; }

        /// <summary>The error code, or 0 if there was no error.</summary>
        public ErrorCode ErrorCode { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        var isFlexible = ApiVersion >= 1;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        // Markers array
        if (isFlexible)
        {
            writer.WriteVarInt(Markers.Count + 1);
        }
        else
        {
            writer.WriteInt32(Markers.Count);
        }

        foreach (var marker in Markers)
        {
            writer.WriteInt64(marker.ProducerId);

            // Topics array
            if (isFlexible)
            {
                writer.WriteVarInt(marker.Topics.Count + 1);
            }
            else
            {
                writer.WriteInt32(marker.Topics.Count);
            }

            foreach (var topic in marker.Topics)
            {
                if (isFlexible)
                {
                    writer.WriteCompactString(topic.Topic);
                    writer.WriteVarInt(topic.Partitions.Count + 1);
                }
                else
                {
                    writer.WriteString(topic.Topic);
                    writer.WriteInt32(topic.Partitions.Count);
                }

                foreach (var partition in topic.Partitions)
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

            if (isFlexible)
            {
                writer.WriteVarInt(0); // Marker tagged fields
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // Body tagged fields
        }
    }

    public static WriteTxnMarkersResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        var isFlexible = apiVersion >= 1;

        if (isFlexible)
        {
            reader.SkipTaggedFields(); // Response header tagged fields
        }

        var markerCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
        var markers = new List<MarkerResult>(markerCount);

        for (int i = 0; i < markerCount; i++)
        {
            var producerId = reader.ReadInt64();

            var topicCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
            var topics = new List<TopicResult>(topicCount);

            for (int j = 0; j < topicCount; j++)
            {
                var topicName = isFlexible ? reader.ReadCompactString() ?? "" : reader.ReadString() ?? "";
                var partitionCount = isFlexible ? reader.ReadVarInt() - 1 : reader.ReadInt32();
                var partitions = new List<PartitionResult>(partitionCount);

                for (int k = 0; k < partitionCount; k++)
                {
                    var partitionIndex = reader.ReadInt32();
                    var errorCode = (ErrorCode)reader.ReadInt16();

                    if (isFlexible)
                    {
                        reader.SkipTaggedFields();
                    }

                    partitions.Add(new PartitionResult
                    {
                        PartitionIndex = partitionIndex,
                        ErrorCode = errorCode
                    });
                }

                if (isFlexible)
                {
                    reader.SkipTaggedFields();
                }

                topics.Add(new TopicResult
                {
                    Topic = topicName,
                    Partitions = partitions
                });
            }

            if (isFlexible)
            {
                reader.SkipTaggedFields();
            }

            markers.Add(new MarkerResult
            {
                ProducerId = producerId,
                Topics = topics
            });
        }

        if (isFlexible)
        {
            reader.SkipTaggedFields();
        }

        return new WriteTxnMarkersResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            Markers = markers
        };
    }
}
