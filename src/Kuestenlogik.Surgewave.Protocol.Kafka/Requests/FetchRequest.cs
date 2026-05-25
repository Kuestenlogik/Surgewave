namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka Fetch request (v4-18)
/// v15: Deprecated ReplicaId (use ReplicaState tagged field instead)
/// v17: Adds ReplicaDirectoryId (UUID) for KIP-858 tiered storage
/// </summary>
public sealed partial class FetchRequest : KafkaRequest
{
    public const byte ReadUncommitted = 0;
    public const byte ReadCommitted = 1;

    public required int ReplicaId { get; init; }
    public required int MaxWaitMs { get; init; }
    public required int MinBytes { get; init; }
    public required int MaxBytes { get; init; }
    public byte IsolationLevel { get; init; } = ReadUncommitted; // 0 = READ_UNCOMMITTED, 1 = READ_COMMITTED
    /// <summary>The fetch session ID (v7+)</summary>
    public int SessionId { get; init; }
    /// <summary>The fetch session epoch (v7+)</summary>
    public int SessionEpoch { get; init; } = -1;
    /// <summary>Rack ID of the consumer (v11+)</summary>
    public string? RackId { get; init; }
    public required List<FetchTopic> Topics { get; init; }
    /// <summary>Partitions to remove from incremental fetch (v7+)</summary>
    public List<ForgottenTopic>? ForgottenTopicsData { get; init; }

    /// <summary>
    /// Cluster ID for cluster verification (v12+ tagged field, tag 0).
    /// Used to reject fetch requests destined for wrong clusters.
    /// </summary>
    public string? ClusterId { get; init; }

    /// <summary>
    /// Replica state information for follower fetches (v12+ tagged field, tag 1).
    /// Replaces ReplicaId for specifying follower state in v15+.
    /// </summary>
    public ReplicaStateInfo? ReplicaState { get; init; }

    /// <summary>
    /// Replica state for follower fetches.
    /// </summary>
    public sealed class ReplicaStateInfo
    {
        /// <summary>The replica ID of the follower, or -1 if this is a consumer.</summary>
        public required int ReplicaId { get; init; }
        /// <summary>The epoch of the replica.</summary>
        public required long ReplicaEpoch { get; init; }
    }

    public sealed class FetchTopic
    {
        /// <summary>Topic name (v0-12), null for v13+</summary>
        public string? Topic { get; init; }
        /// <summary>Topic ID (v13+)</summary>
        public Guid TopicId { get; init; }
        public required List<FetchPartition> Partitions { get; init; }
    }

    public sealed class FetchPartition
    {
        public required int Partition { get; init; }
        /// <summary>The current leader epoch (v9+), -1 if not specified</summary>
        public int CurrentLeaderEpoch { get; init; } = -1;
        public required long FetchOffset { get; init; }
        /// <summary>The epoch of the last fetched record (v12+), -1 if none</summary>
        public int LastFetchedEpoch { get; init; } = -1;
        /// <summary>The earliest available offset of the follower replica (v5+)</summary>
        public long LogStartOffset { get; init; } = -1;
        public required int MaxBytes { get; init; }
        /// <summary>The directory ID of the replica (v17+, KIP-858)</summary>
        public Guid ReplicaDirectoryId { get; init; }
    }

    public sealed class ForgottenTopic
    {
        public string? Topic { get; init; }
        public Guid TopicId { get; init; }
        public required List<int> Partitions { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 12;
        bool usesTopicId = ApiVersion >= 13;

        // Request header
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

        // Request body
        writer.WriteInt32(ReplicaId);
        writer.WriteInt32(MaxWaitMs);
        writer.WriteInt32(MinBytes);
        writer.WriteInt32(MaxBytes);

        if (ApiVersion >= 4)
            writer.WriteInt8((sbyte)IsolationLevel);

        if (ApiVersion >= 7)
        {
            writer.WriteInt32(SessionId);
            writer.WriteInt32(SessionEpoch);
        }

        // Topics array
        if (isFlexible)
            writer.WriteVarInt(Topics.Count + 1);
        else
            writer.WriteInt32(Topics.Count);

        foreach (var topic in Topics)
        {
            if (usesTopicId)
            {
                writer.WriteUuid(topic.TopicId);
            }
            else if (isFlexible)
            {
                writer.WriteCompactString(topic.Topic);
            }
            else
            {
                writer.WriteString(topic.Topic);
            }

            // Partitions array
            if (isFlexible)
                writer.WriteVarInt(topic.Partitions.Count + 1);
            else
                writer.WriteInt32(topic.Partitions.Count);

            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.Partition);

                if (ApiVersion >= 9)
                    writer.WriteInt32(partition.CurrentLeaderEpoch);

                writer.WriteInt64(partition.FetchOffset);

                if (ApiVersion >= 12)
                    writer.WriteInt32(partition.LastFetchedEpoch);

                if (ApiVersion >= 5)
                    writer.WriteInt64(partition.LogStartOffset);

                writer.WriteInt32(partition.MaxBytes);

                // ReplicaDirectoryId (v17+, KIP-858)
                if (ApiVersion >= 17)
                    writer.WriteUuid(partition.ReplicaDirectoryId);

                // Partition tagged fields
                if (isFlexible)
                    writer.WriteVarInt(0);
            }

            // Topic tagged fields
            if (isFlexible)
                writer.WriteVarInt(0);
        }

        // ForgottenTopicsData (v7+)
        if (ApiVersion >= 7)
        {
            var forgotten = ForgottenTopicsData ?? [];
            if (isFlexible)
                writer.WriteVarInt(forgotten.Count + 1);
            else
                writer.WriteInt32(forgotten.Count);

            foreach (var forgottenTopic in forgotten)
            {
                if (usesTopicId)
                {
                    writer.WriteUuid(forgottenTopic.TopicId);
                }
                else if (isFlexible)
                {
                    writer.WriteCompactString(forgottenTopic.Topic);
                }
                else
                {
                    writer.WriteString(forgottenTopic.Topic);
                }

                // Partitions array
                if (isFlexible)
                    writer.WriteVarInt(forgottenTopic.Partitions.Count + 1);
                else
                    writer.WriteInt32(forgottenTopic.Partitions.Count);

                foreach (var part in forgottenTopic.Partitions)
                    writer.WriteInt32(part);

                // Forgotten topic tagged fields
                if (isFlexible)
                    writer.WriteVarInt(0);
            }
        }

        // RackId (v11+)
        if (ApiVersion >= 11)
        {
            if (isFlexible)
                writer.WriteCompactString(RackId ?? "");
            else
                writer.WriteString(RackId ?? "");
        }

        // Body tagged fields (v12+: ClusterId = tag 0, ReplicaState = tag 1)
        if (isFlexible)
        {
            var tagCount = 0;
            if (ClusterId != null) tagCount++;
            if (ReplicaState != null) tagCount++;

            writer.WriteVarInt(tagCount);

            // Tag 0: ClusterId (nullable string)
            if (ClusterId != null)
            {
                writer.WriteVarInt(0); // Tag 0
                using var clusterWriter = new KafkaProtocolWriter(64);
                clusterWriter.WriteCompactString(ClusterId);
                var clusterData = clusterWriter.WrittenSpan;
                writer.WriteVarInt(clusterData.Length);
                writer.WriteRaw(clusterData);
            }

            // Tag 1: ReplicaState
            if (ReplicaState != null)
            {
                writer.WriteVarInt(1); // Tag 1
                using var replicaWriter = new KafkaProtocolWriter(16);
                replicaWriter.WriteInt32(ReplicaState.ReplicaId);
                replicaWriter.WriteInt64(ReplicaState.ReplicaEpoch);
                replicaWriter.WriteVarInt(0); // No nested tagged fields
                var replicaData = replicaWriter.WrittenSpan;
                writer.WriteVarInt(replicaData.Length);
                writer.WriteRaw(replicaData);
            }
        }
    }

}

/// <summary>
/// Kafka Fetch response (v4-18)
/// v16+: NodeEndpoints tagged field support
/// v17: Adds ReplicaDirectoryId to partition (KIP-858)
/// v18: Adds HighWatermark to AbortedTransaction (KIP-405)
/// </summary>
public sealed class FetchResponse : KafkaResponse
{
    public required int ThrottleTimeMs { get; init; }
    /// <summary>Top level response error code (v7+)</summary>
    public ErrorCode ErrorCode { get; init; } = ErrorCode.None;
    /// <summary>The fetch session ID (v7+)</summary>
    public int SessionId { get; init; }
    public required List<FetchableTopicResponse> Responses { get; init; }

    /// <summary>
    /// Broker endpoint information for direct connections (v16+ tagged field).
    /// Allows clients to connect directly to partition leaders.
    /// </summary>
    public List<NodeEndpoint>? NodeEndpoints { get; init; }

    public sealed class FetchableTopicResponse
    {
        /// <summary>Topic name (v0-12)</summary>
        public string? Topic { get; init; }
        /// <summary>Topic ID (v13+)</summary>
        public Guid TopicId { get; init; }
        public required List<PartitionResponse> Partitions { get; init; }
    }

    public sealed class PartitionResponse
    {
        public required int Partition { get; init; }
        public required ErrorCode ErrorCode { get; init; }
        public required long HighWatermark { get; init; }
        /// <summary>Last stable offset for transactional reads (v4+)</summary>
        public long LastStableOffset { get; init; } = -1;
        /// <summary>Log start offset (v5+)</summary>
        public long LogStartOffset { get; init; } = -1;
        /// <summary>Preferred read replica for follower fetching (v11+)</summary>
        public int PreferredReadReplica { get; init; } = -1;
        public required byte[] RecordSet { get; init; }

        /// <summary>
        /// Diverging epoch information for truncation detection (v12+ tagged field, tag 0).
        /// Sent when follower needs to truncate its log due to leader change.
        /// </summary>
        public EpochEndOffset? DivergingEpoch { get; init; }

        /// <summary>
        /// Current leader information for this partition (v12+ tagged field, tag 1).
        /// Allows clients to refresh metadata more efficiently.
        /// </summary>
        public FetchLeaderInfo? CurrentLeader { get; init; }

        /// <summary>
        /// Snapshot ID reference for KRaft (v12+ tagged field, tag 2).
        /// Points to a snapshot for log recovery.
        /// </summary>
        public SnapshotIdInfo? SnapshotId { get; init; }
    }

    /// <summary>
    /// Epoch and end offset for diverging epoch detection.
    /// </summary>
    public sealed class EpochEndOffset
    {
        /// <summary>The epoch of the diverging point.</summary>
        public required int Epoch { get; init; }
        /// <summary>The end offset of the diverging epoch.</summary>
        public required long EndOffset { get; init; }
    }

    /// <summary>
    /// Current leader information in FetchResponse.
    /// </summary>
    public sealed class FetchLeaderInfo
    {
        /// <summary>The ID of the current leader, or -1 if unknown.</summary>
        public required int LeaderId { get; init; }
        /// <summary>The latest known leader epoch.</summary>
        public required int LeaderEpoch { get; init; }
    }

    /// <summary>
    /// Snapshot ID for KRaft log recovery.
    /// </summary>
    public sealed class SnapshotIdInfo
    {
        /// <summary>The epoch of the snapshot.</summary>
        public required long EndOffset { get; init; }
        /// <summary>The end offset of the snapshot.</summary>
        public required int Epoch { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // Version 12+ uses flexible format (compact strings/arrays, tagged fields)
        bool isFlexible = ApiVersion >= 12;
        // Version 13+ uses TopicId instead of topic name
        bool usesTopicId = ApiVersion >= 13;

        // Response header
        writer.WriteInt32(CorrelationId);

        // Response header tagged fields (v12+)
        if (isFlexible)
        {
            writer.WriteVarInt(0); // No header tagged fields
        }

        // Response body
        writer.WriteInt32(ThrottleTimeMs);

        // ErrorCode (v7+)
        if (ApiVersion >= 7)
        {
            writer.WriteInt16((short)ErrorCode);
        }

        // SessionId (v7+)
        if (ApiVersion >= 7)
        {
            writer.WriteInt32(SessionId);
        }

        // Responses array
        if (isFlexible)
        {
            // Compact array: write length+1 as varint
            writer.WriteVarInt(Responses.Count + 1);
        }
        else
        {
            writer.WriteInt32(Responses.Count);
        }

        foreach (var topicResponse in Responses)
        {
            if (usesTopicId)
            {
                writer.WriteUuid(topicResponse.TopicId);
            }
            else if (isFlexible)
            {
                writer.WriteCompactString(topicResponse.Topic);
            }
            else
            {
                writer.WriteString(topicResponse.Topic);
            }

            // Partitions array
            if (isFlexible)
            {
                writer.WriteVarInt(topicResponse.Partitions.Count + 1);
            }
            else
            {
                writer.WriteInt32(topicResponse.Partitions.Count);
            }

            foreach (var partition in topicResponse.Partitions)
            {
                writer.WriteInt32(partition.Partition);
                writer.WriteInt16((short)partition.ErrorCode);
                writer.WriteInt64(partition.HighWatermark);

                // LastStableOffset (v4+)
                if (ApiVersion >= 4)
                {
                    writer.WriteInt64(partition.LastStableOffset);
                }

                // LogStartOffset (v5+)
                if (ApiVersion >= 5)
                {
                    writer.WriteInt64(partition.LogStartOffset);
                }

                // AbortedTransactions (v4+) - empty array
                if (ApiVersion >= 4)
                {
                    if (isFlexible)
                    {
                        writer.WriteVarInt(1); // Empty compact nullable array (length+1=1 means 0 elements)
                    }
                    else
                    {
                        writer.WriteInt32(0); // Empty array
                    }
                }

                // PreferredReadReplica (v11+)
                if (ApiVersion >= 11)
                {
                    writer.WriteInt32(partition.PreferredReadReplica);
                }

                // Records field - in flexible versions uses compact bytes (varint length), otherwise int32
                if (isFlexible)
                {
                    writer.WriteVarInt(partition.RecordSet.Length + 1); // +1 for compact bytes encoding
                }
                else
                {
                    writer.WriteInt32(partition.RecordSet.Length);
                }
                writer.WriteRaw(partition.RecordSet);

                // Partition tagged fields (v12+)
                // Tags: 0=DivergingEpoch, 1=CurrentLeader, 2=SnapshotId
                if (isFlexible)
                {
                    var partTagCount = 0;
                    if (partition.DivergingEpoch != null) partTagCount++;
                    if (partition.CurrentLeader != null) partTagCount++;
                    if (partition.SnapshotId != null) partTagCount++;

                    writer.WriteVarInt(partTagCount);

                    // Tag 0: DivergingEpoch
                    if (partition.DivergingEpoch != null)
                    {
                        writer.WriteVarInt(0); // Tag 0
                        using var epochWriter = new KafkaProtocolWriter(16);
                        epochWriter.WriteInt32(partition.DivergingEpoch.Epoch);
                        epochWriter.WriteInt64(partition.DivergingEpoch.EndOffset);
                        epochWriter.WriteVarInt(0); // No nested tagged fields
                        var epochData = epochWriter.WrittenSpan;
                        writer.WriteVarInt(epochData.Length);
                        writer.WriteRaw(epochData);
                    }

                    // Tag 1: CurrentLeader
                    if (partition.CurrentLeader != null)
                    {
                        writer.WriteVarInt(1); // Tag 1
                        using var leaderWriter = new KafkaProtocolWriter(16);
                        leaderWriter.WriteInt32(partition.CurrentLeader.LeaderId);
                        leaderWriter.WriteInt32(partition.CurrentLeader.LeaderEpoch);
                        leaderWriter.WriteVarInt(0); // No nested tagged fields
                        var leaderData = leaderWriter.WrittenSpan;
                        writer.WriteVarInt(leaderData.Length);
                        writer.WriteRaw(leaderData);
                    }

                    // Tag 2: SnapshotId
                    if (partition.SnapshotId != null)
                    {
                        writer.WriteVarInt(2); // Tag 2
                        using var snapWriter = new KafkaProtocolWriter(16);
                        snapWriter.WriteInt64(partition.SnapshotId.EndOffset);
                        snapWriter.WriteInt32(partition.SnapshotId.Epoch);
                        snapWriter.WriteVarInt(0); // No nested tagged fields
                        var snapData = snapWriter.WrittenSpan;
                        writer.WriteVarInt(snapData.Length);
                        writer.WriteRaw(snapData);
                    }
                }
            }

            // Topic tagged fields (v12+)
            if (isFlexible)
            {
                writer.WriteVarInt(0); // No tagged fields
            }
        }

        // Response body tagged fields (v12+)
        // Tag 0 = NodeEndpoints (v16+)
        if (isFlexible)
        {
            if (ApiVersion >= 16 && NodeEndpoints is { Count: > 0 })
            {
                // Write tag 0 = NodeEndpoints
                writer.WriteVarInt(1); // 1 tagged field
                writer.WriteVarInt(0); // Tag 0

                // Calculate field size for NodeEndpoints
                // Size = varint(count+1) + each endpoint
                using var sizeWriter = new KafkaProtocolWriter(256);
                sizeWriter.WriteVarInt(NodeEndpoints.Count + 1);
                foreach (var endpoint in NodeEndpoints)
                {
                    sizeWriter.WriteInt32(endpoint.NodeId);
                    sizeWriter.WriteCompactString(endpoint.Host);
                    sizeWriter.WriteInt32(endpoint.Port);
                    sizeWriter.WriteCompactString(endpoint.Rack); // Nullable compact string
                    sizeWriter.WriteVarInt(0); // No endpoint tagged fields
                }
                var nodeEndpointsData = sizeWriter.WrittenSpan;
                writer.WriteVarInt(nodeEndpointsData.Length);
                writer.WriteRaw(nodeEndpointsData);
            }
            else
            {
                writer.WriteVarInt(0); // No tagged fields
            }
        }
    }
}

/// <summary>
/// Broker endpoint information for NodeEndpoints tagged field.
/// Used in FetchResponse v16+ and ProduceResponse v10+.
/// </summary>
public sealed class NodeEndpoint
{
    /// <summary>The broker ID</summary>
    public required int NodeId { get; init; }
    /// <summary>The broker hostname</summary>
    public required string Host { get; init; }
    /// <summary>The broker port</summary>
    public required int Port { get; init; }
    /// <summary>The broker rack (optional)</summary>
    public string? Rack { get; init; }
}
