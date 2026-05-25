namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka FetchSnapshot request (API Key 59, v0-1).
/// KRaft - fetch snapshots from the leader for log truncation.
/// </summary>
public sealed class FetchSnapshotRequest : KafkaRequest
{
    /// <summary>The cluster ID if known (v1+).</summary>
    public string? ClusterId { get; init; }

    /// <summary>The broker ID of the follower.</summary>
    public int ReplicaId { get; init; } = -1;

    /// <summary>The maximum bytes to fetch from all partitions.</summary>
    public int MaxBytes { get; init; } = 0x7fffffff;

    /// <summary>The topics to fetch.</summary>
    public required List<TopicSnapshot> Topics { get; init; }

    public sealed class TopicSnapshot
    {
        /// <summary>The name of the topic to fetch.</summary>
        public required string Name { get; init; }

        /// <summary>The partitions to fetch.</summary>
        public required List<PartitionSnapshot> Partitions { get; init; }
    }

    public sealed class PartitionSnapshot
    {
        /// <summary>The partition index.</summary>
        public int Partition { get; init; }

        /// <summary>The current leader epoch of the partition, used to fence stale requests.</summary>
        public int CurrentLeaderEpoch { get; init; }

        /// <summary>The snapshot endOffset and epoch to fetch.</summary>
        public required SnapshotId SnapshotId { get; init; }

        /// <summary>The byte position within the snapshot to start fetching from.</summary>
        public long Position { get; init; }
    }

    public sealed class SnapshotId
    {
        /// <summary>The end offset of the snapshot.</summary>
        public long EndOffset { get; init; }

        /// <summary>The epoch of the snapshot.</summary>
        public int Epoch { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // v0+ is flexible
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        if (ApiVersion >= 1)
        {
            writer.WriteCompactString(ClusterId);
        }

        writer.WriteInt32(ReplicaId);
        writer.WriteInt32(MaxBytes);

        writer.WriteVarInt(Topics.Count + 1);
        foreach (var topic in Topics)
        {
            writer.WriteCompactString(topic.Name);

            writer.WriteVarInt(topic.Partitions.Count + 1);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.Partition);
                writer.WriteInt32(partition.CurrentLeaderEpoch);

                // SnapshotId
                writer.WriteInt64(partition.SnapshotId.EndOffset);
                writer.WriteInt32(partition.SnapshotId.Epoch);
                writer.WriteVarInt(0); // SnapshotId tagged fields

                writer.WriteInt64(partition.Position);
                writer.WriteVarInt(0); // Partition tagged fields
            }

            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static FetchSnapshotRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        string? clusterId = null;
        if (apiVersion >= 1)
        {
            clusterId = reader.ReadCompactString();
        }

        var replicaId = reader.ReadInt32();
        var maxBytes = reader.ReadInt32();

        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<TopicSnapshot>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var name = reader.ReadCompactString() ?? "";

            var partitionCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionSnapshot>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                var partition = reader.ReadInt32();
                var currentLeaderEpoch = reader.ReadInt32();

                // SnapshotId
                var endOffset = reader.ReadInt64();
                var epoch = reader.ReadInt32();
                reader.SkipTaggedFields();

                var position = reader.ReadInt64();
                reader.SkipTaggedFields();

                partitions.Add(new PartitionSnapshot
                {
                    Partition = partition,
                    CurrentLeaderEpoch = currentLeaderEpoch,
                    SnapshotId = new SnapshotId
                    {
                        EndOffset = endOffset,
                        Epoch = epoch
                    },
                    Position = position
                });
            }

            reader.SkipTaggedFields();

            topics.Add(new TopicSnapshot
            {
                Name = name,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new FetchSnapshotRequest
        {
            ApiKey = ApiKey.FetchSnapshot,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            ClusterId = clusterId,
            ReplicaId = replicaId,
            MaxBytes = maxBytes,
            Topics = topics
        };
    }
}

/// <summary>
/// Kafka FetchSnapshot response (API Key 59, v0-1).
/// </summary>
public sealed class FetchSnapshotResponse : KafkaResponse
{
    /// <summary>Duration in milliseconds for which the request was throttled.</summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>The top level response error code.</summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>The topics to fetch.</summary>
    public required List<TopicSnapshot> Topics { get; init; }

    public sealed class TopicSnapshot
    {
        /// <summary>The name of the topic.</summary>
        public required string Name { get; init; }

        /// <summary>The partitions.</summary>
        public required List<PartitionSnapshot> Partitions { get; init; }
    }

    public sealed class PartitionSnapshot
    {
        /// <summary>The partition index.</summary>
        public int Index { get; init; }

        /// <summary>The error code, or 0 if there was no fetch error.</summary>
        public ErrorCode ErrorCode { get; init; }

        /// <summary>The snapshot endOffset and epoch fetched.</summary>
        public required SnapshotId SnapshotId { get; init; }

        /// <summary>The current leader info.</summary>
        public LeaderIdAndEpoch? CurrentLeader { get; init; }

        /// <summary>The total size of the snapshot.</summary>
        public long Size { get; init; }

        /// <summary>The starting byte position within the snapshot included in the Bytes field.</summary>
        public long Position { get; init; }

        /// <summary>Snapshot data.</summary>
        public byte[]? UnalignedRecords { get; init; }
    }

    public sealed class SnapshotId
    {
        /// <summary>The end offset of the snapshot.</summary>
        public long EndOffset { get; init; }

        /// <summary>The epoch of the snapshot.</summary>
        public int Epoch { get; init; }
    }

    public sealed class LeaderIdAndEpoch
    {
        /// <summary>The ID of the current leader or -1 if the leader is unknown.</summary>
        public int LeaderId { get; init; }

        /// <summary>The latest known leader epoch.</summary>
        public int LeaderEpoch { get; init; }
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
            writer.WriteCompactString(topic.Name);

            writer.WriteVarInt(topic.Partitions.Count + 1);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.Index);
                writer.WriteInt16((short)partition.ErrorCode);

                // SnapshotId
                writer.WriteInt64(partition.SnapshotId.EndOffset);
                writer.WriteInt32(partition.SnapshotId.Epoch);
                writer.WriteVarInt(0); // SnapshotId tagged fields

                // CurrentLeader (tagged field 0)
                if (partition.CurrentLeader != null)
                {
                    // Write tagged fields manually
                    writer.WriteVarInt(1); // 1 tagged field
                    writer.WriteVarInt(0); // Tag 0
                    // Calculate size: leaderId (4) + leaderEpoch (4) + tagged fields (1) = 9
                    writer.WriteVarInt(9);
                    writer.WriteInt32(partition.CurrentLeader.LeaderId);
                    writer.WriteInt32(partition.CurrentLeader.LeaderEpoch);
                    writer.WriteVarInt(0); // Leader tagged fields
                }
                else
                {
                    writer.WriteVarInt(0); // No tagged fields for CurrentLeader placeholder
                }

                writer.WriteInt64(partition.Size);
                writer.WriteInt64(partition.Position);
                writer.WriteCompactBytes(partition.UnalignedRecords);
                writer.WriteVarInt(0); // Partition tagged fields
            }

            writer.WriteVarInt(0); // Topic tagged fields
        }

        writer.WriteVarInt(0); // Body tagged fields
    }

    public static FetchSnapshotResponse ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId)
    {
        reader.SkipTaggedFields(); // Response header tagged fields

        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();

        var topicCount = reader.ReadVarInt() - 1;
        var topics = new List<TopicSnapshot>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var name = reader.ReadCompactString() ?? "";

            var partitionCount = reader.ReadVarInt() - 1;
            var partitions = new List<PartitionSnapshot>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                var index = reader.ReadInt32();
                var partErrorCode = (ErrorCode)reader.ReadInt16();

                // SnapshotId
                var endOffset = reader.ReadInt64();
                var epoch = reader.ReadInt32();
                reader.SkipTaggedFields();

                // Skip CurrentLeader tagged field
                reader.SkipTaggedFields();

                var size = reader.ReadInt64();
                var position = reader.ReadInt64();
                var unalignedRecords = reader.ReadCompactBytes();
                reader.SkipTaggedFields();

                partitions.Add(new PartitionSnapshot
                {
                    Index = index,
                    ErrorCode = partErrorCode,
                    SnapshotId = new SnapshotId
                    {
                        EndOffset = endOffset,
                        Epoch = epoch
                    },
                    Size = size,
                    Position = position,
                    UnalignedRecords = unalignedRecords
                });
            }

            reader.SkipTaggedFields();

            topics.Add(new TopicSnapshot
            {
                Name = name,
                Partitions = partitions
            });
        }

        reader.SkipTaggedFields();

        return new FetchSnapshotResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            Topics = topics
        };
    }
}
