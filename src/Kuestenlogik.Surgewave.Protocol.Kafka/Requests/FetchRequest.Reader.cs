namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

public sealed partial class FetchRequest
{
    /// <summary>
    /// Deserialize a FetchRequest from binary data.
    /// </summary>
    public static FetchRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        // Zero-copy: reuse the MemoryStream's internal buffer directly instead of
        // allocating + copying all remaining bytes. The buffer is pooled and outlives
        // the parse (returned in ProcessKafkaRequestsAsync's finally block).
        var stream = (MemoryStream)reader.BaseStream;
        var position = (int)stream.Position;
        var remainingSize = (int)(stream.Length - position);
        KafkaProtocolReader protocolReader;
        if (stream.TryGetBuffer(out var segment))
        {
            protocolReader = new KafkaProtocolReader(segment.Array!, segment.Offset + position, remainingSize);
        }
        else
        {
            var remainingBytes = reader.ReadBytes(remainingSize);
            protocolReader = new KafkaProtocolReader(remainingBytes);
        }

        return ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    /// <summary>
    /// Hot-path overload: parses straight out of the caller's (pooled) request buffer.
    /// </summary>
    public static FetchRequest ReadFrom(KafkaProtocolReader protocolReader, short apiVersion, int correlationId, string clientId)
    {
        // Version 12+ uses flexible format (compact strings/arrays, tagged fields)
        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.Fetch, apiVersion);
        // Version 13+ uses TopicId instead of topic name
        bool usesTopicId = apiVersion >= 13;

        // Read fields according to version. KIP-903 (Fetch v15+) removed the top-level
        // ReplicaId field — it now lives only inside the ReplicaState tagged field.
        // Reading those 4 bytes when the client doesn't send them mis-aligns every
        // subsequent field and the request decodes as garbage (which is silently
        // dropped by the dispatcher).
        var replicaId = apiVersion <= 14 ? protocolReader.ReadInt32() : -1;
        var maxWaitMs = protocolReader.ReadInt32();
        var minBytes = protocolReader.ReadInt32();
        var maxBytes = apiVersion >= ProtocolVersions.Features.Fetch.MaxBytesVersion ? protocolReader.ReadInt32() : int.MaxValue;

        // Read IsolationLevel (v4+)
        byte isolationLevel = ReadUncommitted;
        if (apiVersion >= ProtocolVersions.Features.Fetch.IsolationLevelVersion)
            isolationLevel = (byte)protocolReader.ReadInt8();

        // Read SessionId (v7+), SessionEpoch (v7+)
        int sessionId = 0;
        int sessionEpoch = -1;
        if (apiVersion >= ProtocolVersions.Features.Fetch.SessionVersion)
        {
            sessionId = protocolReader.ReadInt32();
            sessionEpoch = protocolReader.ReadInt32();
        }

        // Read Topics array
        var topics = isFlexible
            ? ReadTopicsFlexible(protocolReader, apiVersion, usesTopicId)
            : ReadTopicsNonFlexible(protocolReader, apiVersion);

        // Read ForgottenTopicsData (v7+)
        var forgottenTopicsData = apiVersion >= 7
            ? ReadForgottenTopics(protocolReader, apiVersion, isFlexible, usesTopicId)
            : [];

        // Read RackId (v11+)
        string? rackId = null;
        if (apiVersion >= 11)
        {
            rackId = isFlexible ? protocolReader.ReadCompactString() : protocolReader.ReadString();
        }

        // Top-level tagged fields: ClusterId (tag 0, v12+) and ReplicaState (tag 1, v15+). From v15 a follower
        // names itself only in ReplicaState; skipped, every follower fetch read as a consumer's (ReplicaId -1).
        string? clusterId = null;
        ReplicaStateInfo? replicaState = null;
        if (isFlexible)
        {
            var taggedFieldCount = protocolReader.ReadVarInt();
            for (int i = 0; i < taggedFieldCount; i++)
            {
                var tag = protocolReader.ReadVarInt();
                var size = protocolReader.ReadVarInt();
                if (tag == 0)
                {
                    clusterId = protocolReader.ReadCompactString();
                }
                else if (tag == 1 && apiVersion >= 15)
                {
                    var stateReplicaId = protocolReader.ReadInt32();
                    var stateReplicaEpoch = protocolReader.ReadInt64();
                    SkipTaggedFields(protocolReader);
                    replicaState = new ReplicaStateInfo { ReplicaId = stateReplicaId, ReplicaEpoch = stateReplicaEpoch };
                    replicaId = stateReplicaId;
                }
                else
                {
                    protocolReader.Skip(size);
                }
            }
        }

        return new FetchRequest
        {
            ClusterId = clusterId,
            ReplicaState = replicaState,
            ApiKey = ApiKey.Fetch,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            ReplicaId = replicaId,
            MaxWaitMs = maxWaitMs,
            MinBytes = minBytes,
            MaxBytes = maxBytes,
            IsolationLevel = isolationLevel,
            SessionId = sessionId,
            SessionEpoch = sessionEpoch,
            RackId = rackId,
            Topics = topics,
            ForgottenTopicsData = forgottenTopicsData.Count > 0 ? forgottenTopicsData : null
        };
    }

    private static List<FetchTopic> ReadTopicsFlexible(KafkaProtocolReader protocolReader, short apiVersion, bool usesTopicId)
    {
        var topicCount = protocolReader.ReadVarInt() - 1;
        // Clamped: a corrupt/hostile count must not turn into a huge pre-allocation.
        var topics = new List<FetchTopic>(Math.Clamp(topicCount, 0, 1024));

        for (int i = 0; i < topicCount; i++)
        {
            string? topicName = null;
            Guid topicId = Guid.Empty;

            if (usesTopicId)
            {
                topicId = protocolReader.ReadUuid();
            }
            else
            {
                topicName = protocolReader.ReadCompactString() ?? string.Empty;
            }

            var partitions = ReadPartitionsFlexible(protocolReader, apiVersion);
            SkipTaggedFields(protocolReader);

            topics.Add(new FetchTopic
            {
                Topic = topicName,
                TopicId = topicId,
                Partitions = partitions
            });
        }

        return topics;
    }

    private static List<FetchPartition> ReadPartitionsFlexible(KafkaProtocolReader protocolReader, short apiVersion)
    {
        var partitionCount = protocolReader.ReadVarInt() - 1;
        var partitions = new List<FetchPartition>(Math.Clamp(partitionCount, 0, 1024));

        for (int j = 0; j < partitionCount; j++)
        {
            var partition = protocolReader.ReadInt32();

            int currentLeaderEpoch = -1;
            if (apiVersion >= ProtocolVersions.Features.Fetch.CurrentLeaderEpochVersion)
                currentLeaderEpoch = protocolReader.ReadInt32();

            var fetchOffset = protocolReader.ReadInt64();

            int lastFetchedEpoch = -1;
            if (apiVersion >= ProtocolVersions.Features.Fetch.LastFetchedEpochVersion)
                lastFetchedEpoch = protocolReader.ReadInt32();

            long logStartOffset = -1;
            if (apiVersion >= ProtocolVersions.Features.Fetch.LogStartOffsetVersion)
                logStartOffset = protocolReader.ReadInt64();

            var maxBytesPerPartition = protocolReader.ReadInt32();

            // ReplicaDirectoryId (v17+, tag 0) and HighWatermark (v18+, tag 1) are tagged fields of the
            // partition, not fixed ones. Read as a fixed UUID, the next partition's bytes were taken for it
            // and a Java 4.x consumer's fetch failed to parse ("UUID requires 16 bytes").
            var replicaDirectoryId = ReadPartitionTaggedFields(protocolReader);

            partitions.Add(new FetchPartition
            {
                Partition = partition,
                CurrentLeaderEpoch = currentLeaderEpoch,
                FetchOffset = fetchOffset,
                LastFetchedEpoch = lastFetchedEpoch,
                LogStartOffset = logStartOffset,
                MaxBytes = maxBytesPerPartition,
                ReplicaDirectoryId = replicaDirectoryId
            });
        }

        return partitions;
    }

    private static List<FetchTopic> ReadTopicsNonFlexible(KafkaProtocolReader protocolReader, short apiVersion)
    {
        var topicCount = protocolReader.ReadInt32();
        var topics = new List<FetchTopic>(Math.Clamp(topicCount, 0, 1024));

        for (int i = 0; i < topicCount; i++)
        {
            var topicName = protocolReader.ReadString() ?? string.Empty;
            var partitionCount = protocolReader.ReadInt32();
            var partitions = new List<FetchPartition>(Math.Clamp(partitionCount, 0, 1024));

            for (int j = 0; j < partitionCount; j++)
            {
                var partition = protocolReader.ReadInt32();

                int currentLeaderEpoch = -1;
                if (apiVersion >= ProtocolVersions.Features.Fetch.CurrentLeaderEpochVersion)
                    currentLeaderEpoch = protocolReader.ReadInt32();

                var fetchOffset = protocolReader.ReadInt64();

                long logStartOffset = -1;
                if (apiVersion >= ProtocolVersions.Features.Fetch.LogStartOffsetVersion)
                    logStartOffset = protocolReader.ReadInt64();

                var maxBytesPerPartition = protocolReader.ReadInt32();

                partitions.Add(new FetchPartition
                {
                    Partition = partition,
                    CurrentLeaderEpoch = currentLeaderEpoch,
                    FetchOffset = fetchOffset,
                    LogStartOffset = logStartOffset,
                    MaxBytes = maxBytesPerPartition
                });
            }

            topics.Add(new FetchTopic
            {
                Topic = topicName,
                Partitions = partitions
            });
        }

        return topics;
    }

    private static List<ForgottenTopic> ReadForgottenTopics(KafkaProtocolReader protocolReader, short apiVersion, bool isFlexible, bool usesTopicId)
    {
        var forgottenTopicsData = new List<ForgottenTopic>();

        if (isFlexible)
        {
            var forgottenCount = protocolReader.ReadVarInt() - 1;
            for (int i = 0; i < forgottenCount; i++)
            {
                string? forgottenTopic = null;
                Guid forgottenTopicId = Guid.Empty;

                if (usesTopicId)
                {
                    forgottenTopicId = protocolReader.ReadUuid();
                }
                else
                {
                    forgottenTopic = protocolReader.ReadCompactString();
                }

                var partCount = protocolReader.ReadVarInt() - 1;
                var forgottenPartitions = new List<int>(Math.Clamp(partCount, 0, 1024));
                for (int j = 0; j < partCount; j++)
                {
                    forgottenPartitions.Add(protocolReader.ReadInt32());
                }

                SkipTaggedFields(protocolReader);

                forgottenTopicsData.Add(new ForgottenTopic
                {
                    Topic = forgottenTopic,
                    TopicId = forgottenTopicId,
                    Partitions = forgottenPartitions
                });
            }
        }
        else
        {
            var forgottenCount = protocolReader.ReadInt32();
            for (int i = 0; i < forgottenCount; i++)
            {
                var forgottenTopic = protocolReader.ReadString();
                var partCount = protocolReader.ReadInt32();
                var forgottenPartitions = new List<int>(Math.Clamp(partCount, 0, 1024));
                for (int j = 0; j < partCount; j++)
                {
                    forgottenPartitions.Add(protocolReader.ReadInt32());
                }

                forgottenTopicsData.Add(new ForgottenTopic
                {
                    Topic = forgottenTopic,
                    Partitions = forgottenPartitions
                });
            }
        }

        return forgottenTopicsData;
    }

    /// <summary>
    /// A fetch partition's tagged fields: ReplicaDirectoryId (tag 0, a follower's log directory) is kept, the
    /// rest — HighWatermark (tag 1, the leader's view of a follower) and unknown tags — skipped.
    /// </summary>
    private static Guid ReadPartitionTaggedFields(KafkaProtocolReader protocolReader)
    {
        var replicaDirectoryId = Guid.Empty;
        var taggedFieldCount = protocolReader.ReadVarInt();
        for (int i = 0; i < taggedFieldCount; i++)
        {
            var tag = protocolReader.ReadVarInt();
            var size = protocolReader.ReadVarInt();
            if (tag == 0 && size == 16)
                replicaDirectoryId = protocolReader.ReadUuid();
            else
                protocolReader.Skip(size);
        }
        return replicaDirectoryId;
    }

    private static void SkipTaggedFields(KafkaProtocolReader protocolReader)
    {
        var taggedFieldCount = protocolReader.ReadVarInt();
        for (int i = 0; i < taggedFieldCount; i++)
        {
            var tag = protocolReader.ReadVarInt();
            var size = protocolReader.ReadVarInt();
            protocolReader.Skip(size);
        }
    }
}
