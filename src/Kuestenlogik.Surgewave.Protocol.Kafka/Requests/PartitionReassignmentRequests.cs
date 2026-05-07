namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka AlterPartitionReassignments request (API Key 45, v0-0).
/// Alters partition reassignments.
/// </summary>
public sealed class AlterPartitionReassignmentsRequest : KafkaRequest
{
    /// <summary>The time in ms to wait for the request to complete.</summary>
    public int TimeoutMs { get; init; }

    /// <summary>The topics to reassign.</summary>
    public required List<ReassignableTopic> Topics { get; init; }

    public sealed class ReassignableTopic
    {
        /// <summary>The topic name.</summary>
        public required string Name { get; init; }

        /// <summary>The partitions to reassign.</summary>
        public required List<ReassignablePartition> Partitions { get; init; }
    }

    public sealed class ReassignablePartition
    {
        /// <summary>The partition index.</summary>
        public required int PartitionIndex { get; init; }

        /// <summary>
        /// The replicas to place the partitions on, or null to cancel a pending reassignment.
        /// </summary>
        public List<int>? Replicas { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteInt32(TimeoutMs);

        writer.WriteVarInt(Topics.Count + 1);
        foreach (var topic in Topics)
        {
            writer.WriteCompactString(topic.Name);

            writer.WriteVarInt(topic.Partitions.Count + 1);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);

                if (partition.Replicas == null)
                {
                    writer.WriteVarInt(-1); // Null compact array
                }
                else
                {
                    writer.WriteVarInt(partition.Replicas.Count + 1);
                    foreach (var replica in partition.Replicas)
                    {
                        writer.WriteInt32(replica);
                    }
                }

                writer.WriteVarInt(0); // Partition tagged fields
            }

            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static AlterPartitionReassignmentsRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var timeoutMs = reader.ReadInt32();

        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<ReassignableTopic>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var topicName = reader.ReadCompactString() ?? "";

            var partitionCount = reader.ReadVarInt() - 1;
            var partitions = new List<ReassignablePartition>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                var partitionIndex = reader.ReadInt32();

                List<int>? replicas = null;
                var replicaCount = reader.ReadVarInt() - 1;
                if (replicaCount >= 0)
                {
                    replicas = new List<int>(replicaCount);
                    for (int k = 0; k < replicaCount; k++)
                    {
                        replicas.Add(reader.ReadInt32());
                    }
                }

                reader.SkipTaggedFields();

                partitions.Add(new ReassignablePartition
                {
                    PartitionIndex = partitionIndex,
                    Replicas = replicas
                });
            }

            reader.SkipTaggedFields();

            topics.Add(new ReassignableTopic
            {
                Name = topicName,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new AlterPartitionReassignmentsRequest
        {
            ApiKey = ApiKey.AlterPartitionReassignments,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            TimeoutMs = timeoutMs,
            Topics = topics
        };
    }
}

/// <summary>
/// Kafka AlterPartitionReassignments response (API Key 45, v0-0).
/// </summary>
public sealed class AlterPartitionReassignmentsResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The top-level error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The top-level error message, or null if there was no error.</summary>
    public string? ErrorMessage { get; init; }

    /// <summary>The responses to topics to reassign.</summary>
    public required List<ReassignableTopicResponse> Responses { get; init; }

    public sealed class ReassignableTopicResponse
    {
        /// <summary>The topic name.</summary>
        public required string Name { get; init; }

        /// <summary>The responses to partitions to reassign.</summary>
        public required List<ReassignablePartitionResponse> Partitions { get; init; }
    }

    public sealed class ReassignablePartitionResponse
    {
        /// <summary>The partition index.</summary>
        public required int PartitionIndex { get; init; }

        /// <summary>The error code for this partition, or 0 if there was no error.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The error message for this partition, or null if there was no error.</summary>
        public string? ErrorMessage { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);
        writer.WriteCompactString(ErrorMessage);

        writer.WriteVarInt(Responses.Count + 1);
        foreach (var response in Responses)
        {
            writer.WriteCompactString(response.Name);

            writer.WriteVarInt(response.Partitions.Count + 1);
            foreach (var partition in response.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt16((short)partition.ErrorCode);
                writer.WriteCompactString(partition.ErrorMessage);
                writer.WriteVarInt(0); // Partition tagged fields
            }

            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static AlterPartitionReassignmentsResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadCompactString();

        var responseCount = reader.ReadVarInt() - 1;
        var responses = new List<ReassignableTopicResponse>(responseCount);

        for (int i = 0; i < responseCount; i++)
        {
            var topicName = reader.ReadCompactString() ?? "";

            var partitionCount = reader.ReadVarInt() - 1;
            var partitions = new List<ReassignablePartitionResponse>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                partitions.Add(new ReassignablePartitionResponse
                {
                    PartitionIndex = reader.ReadInt32(),
                    ErrorCode = (ErrorCode)reader.ReadInt16(),
                    ErrorMessage = reader.ReadCompactString()
                });
                reader.SkipTaggedFields();
            }

            reader.SkipTaggedFields();

            responses.Add(new ReassignableTopicResponse
            {
                Name = topicName,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new AlterPartitionReassignmentsResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            Responses = responses
        };
    }
}

/// <summary>
/// Kafka ListPartitionReassignments request (API Key 46, v0-0).
/// Lists ongoing partition reassignments.
/// </summary>
public sealed class ListPartitionReassignmentsRequest : KafkaRequest
{
    /// <summary>The time in ms to wait for the request to complete.</summary>
    public int TimeoutMs { get; init; }

    /// <summary>
    /// The topics to list partition reassignments for, or null to list everything.
    /// </summary>
    public List<ListPartitionReassignmentsTopic>? Topics { get; init; }

    public sealed class ListPartitionReassignmentsTopic
    {
        /// <summary>The topic name.</summary>
        public required string Name { get; init; }

        /// <summary>The partitions to list partition reassignments for.</summary>
        public required List<int> PartitionIndexes { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0 is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        writer.WriteInt32(TimeoutMs);

        if (Topics == null)
        {
            writer.WriteVarInt(-1); // Null compact array
        }
        else
        {
            writer.WriteVarInt(Topics.Count + 1);
            foreach (var topic in Topics)
            {
                writer.WriteCompactString(topic.Name);

                writer.WriteVarInt(topic.PartitionIndexes.Count + 1);
                foreach (var partitionIndex in topic.PartitionIndexes)
                {
                    writer.WriteInt32(partitionIndex);
                }

                writer.WriteVarInt(0); // Topic tagged fields
            }
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static ListPartitionReassignmentsRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        var timeoutMs = reader.ReadInt32();

        List<ListPartitionReassignmentsTopic>? topics = null;
        var topicCount = reader.ReadVarInt() - 1;
        if (topicCount >= 0)
        {
            topics = new List<ListPartitionReassignmentsTopic>(topicCount);
            for (int i = 0; i < topicCount; i++)
            {
                var topicName = reader.ReadCompactString() ?? "";

                var partitionCount = reader.ReadVarInt() - 1;
                var partitionIndexes = new List<int>(partitionCount);
                for (int j = 0; j < partitionCount; j++)
                {
                    partitionIndexes.Add(reader.ReadInt32());
                }

                reader.SkipTaggedFields();

                topics.Add(new ListPartitionReassignmentsTopic
                {
                    Name = topicName,
                    PartitionIndexes = partitionIndexes
                });
            }
        }

        reader.SkipTaggedFields();

        return new ListPartitionReassignmentsRequest
        {
            ApiKey = ApiKey.ListPartitionReassignments,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            TimeoutMs = timeoutMs,
            Topics = topics
        };
    }
}

/// <summary>
/// Kafka ListPartitionReassignments response (API Key 46, v0-0).
/// </summary>
public sealed class ListPartitionReassignmentsResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The top-level error code, or 0 if there was no error.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The top-level error message, or null if there was no error.</summary>
    public string? ErrorMessage { get; init; }

    /// <summary>The ongoing reassignments for each topic.</summary>
    public required List<OngoingTopicReassignment> Topics { get; init; }

    public sealed class OngoingTopicReassignment
    {
        /// <summary>The topic name.</summary>
        public required string Name { get; init; }

        /// <summary>The ongoing reassignments for each partition.</summary>
        public required List<OngoingPartitionReassignment> Partitions { get; init; }
    }

    public sealed class OngoingPartitionReassignment
    {
        /// <summary>The partition index.</summary>
        public required int PartitionIndex { get; init; }

        /// <summary>The current replica set.</summary>
        public required List<int> Replicas { get; init; }

        /// <summary>The set of replicas we are currently adding.</summary>
        public required List<int> AddingReplicas { get; init; }

        /// <summary>The set of replicas we are currently removing.</summary>
        public required List<int> RemovingReplicas { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt16((short)ErrorCode);
        writer.WriteCompactString(ErrorMessage);

        writer.WriteVarInt(Topics.Count + 1);
        foreach (var topic in Topics)
        {
            writer.WriteCompactString(topic.Name);

            writer.WriteVarInt(topic.Partitions.Count + 1);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);

                writer.WriteVarInt(partition.Replicas.Count + 1);
                foreach (var replica in partition.Replicas)
                {
                    writer.WriteInt32(replica);
                }

                writer.WriteVarInt(partition.AddingReplicas.Count + 1);
                foreach (var replica in partition.AddingReplicas)
                {
                    writer.WriteInt32(replica);
                }

                writer.WriteVarInt(partition.RemovingReplicas.Count + 1);
                foreach (var replica in partition.RemovingReplicas)
                {
                    writer.WriteInt32(replica);
                }

                writer.WriteVarInt(0); // Partition tagged fields
            }

            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static ListPartitionReassignmentsResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadCompactString();

        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<OngoingTopicReassignment>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var topicName = reader.ReadCompactString() ?? "";

            var partitionCount = reader.ReadVarInt() - 1;
            var partitions = new List<OngoingPartitionReassignment>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                var partitionIndex = reader.ReadInt32();

                var replicaCount = reader.ReadVarInt() - 1;
                var replicas = new List<int>(replicaCount);
                for (int k = 0; k < replicaCount; k++)
                {
                    replicas.Add(reader.ReadInt32());
                }

                var addingCount = reader.ReadVarInt() - 1;
                var addingReplicas = new List<int>(addingCount);
                for (int k = 0; k < addingCount; k++)
                {
                    addingReplicas.Add(reader.ReadInt32());
                }

                var removingCount = reader.ReadVarInt() - 1;
                var removingReplicas = new List<int>(removingCount);
                for (int k = 0; k < removingCount; k++)
                {
                    removingReplicas.Add(reader.ReadInt32());
                }

                reader.SkipTaggedFields();

                partitions.Add(new OngoingPartitionReassignment
                {
                    PartitionIndex = partitionIndex,
                    Replicas = replicas,
                    AddingReplicas = addingReplicas,
                    RemovingReplicas = removingReplicas
                });
            }

            reader.SkipTaggedFields();

            topics.Add(new OngoingTopicReassignment
            {
                Name = topicName,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new ListPartitionReassignmentsResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            Topics = topics
        };
    }
}
