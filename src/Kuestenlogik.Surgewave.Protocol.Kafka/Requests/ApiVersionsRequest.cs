namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// ApiVersions request - First request sent by Kafka clients to discover supported API versions
/// </summary>
public sealed class ApiVersionsRequest : KafkaRequest
{
    public string? ClientSoftwareName { get; init; }
    public string? ClientSoftwareVersion { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // For ApiVersions, clients often send version 0-3
        // Version 3+ uses flexible format with tagged fields

        if (ApiVersion >= 3)
        {
            writer.WriteCompactString(ClientSoftwareName);
            writer.WriteCompactString(ClientSoftwareVersion);
            writer.WriteVarInt(0); // Tagged fields (empty)
        }
        // Versions 0-2 have no body
    }

    public static ApiVersionsRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string? clientId)
    {
        string? clientSoftwareName = null;
        string? clientSoftwareVersion = null;

        if (apiVersion >= 3)
        {
            clientSoftwareName = reader.ReadCompactString();
            clientSoftwareVersion = reader.ReadCompactString();
            // Skip tagged fields
            var taggedFieldCount = reader.ReadVarInt();
            for (int i = 0; i < taggedFieldCount; i++)
            {
                var tag = reader.ReadVarInt();
                var size = reader.ReadVarInt();
                reader.Skip(size);
            }
        }

        return new ApiVersionsRequest
        {
            ApiKey = ApiKey.ApiVersions,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId ?? string.Empty,
            ClientSoftwareName = clientSoftwareName,
            ClientSoftwareVersion = clientSoftwareVersion
        };
    }
}

/// <summary>
/// ApiVersions response - Tells clients which API versions are supported
/// </summary>
public sealed class ApiVersionsResponse : KafkaResponse
{
    public required ErrorCode ErrorCode { get; init; }
    public required SupportedApiVersion[] ApiVersions { get; init; }
    public int ThrottleTimeMs { get; init; }

    /// <summary>
    /// Supported features in the cluster (v3+ tagged field, tag 0).
    /// Lists KIP features and their version ranges.
    /// </summary>
    public List<SupportedFeature>? SupportedFeatures { get; init; }

    /// <summary>
    /// The epoch of finalized features (v3+ tagged field, tag 1).
    /// -1 if finalized features are not available.
    /// </summary>
    public long FinalizedFeaturesEpoch { get; init; } = -1;

    /// <summary>
    /// Finalized features in the cluster (v3+ tagged field, tag 2).
    /// Lists features that have been finalized and their active versions.
    /// </summary>
    public List<FinalizedFeature>? FinalizedFeatures { get; init; }

    /// <summary>
    /// Whether the cluster is ready for ZK to KRaft migration (v3+ tagged field, tag 3).
    /// </summary>
    public bool ZkMigrationReady { get; init; }

    public sealed class SupportedApiVersion
    {
        public required short ApiKey { get; init; }
        public required short MinVersion { get; init; }
        public required short MaxVersion { get; init; }
    }

    /// <summary>
    /// A supported feature with its version range.
    /// </summary>
    public sealed class SupportedFeature
    {
        /// <summary>The name of the feature.</summary>
        public required string Name { get; init; }
        /// <summary>The minimum supported version for this feature.</summary>
        public required short MinVersion { get; init; }
        /// <summary>The maximum supported version for this feature.</summary>
        public required short MaxVersion { get; init; }
    }

    /// <summary>
    /// A finalized feature with its active version.
    /// </summary>
    public sealed class FinalizedFeature
    {
        /// <summary>The name of the feature.</summary>
        public required string Name { get; init; }
        /// <summary>The minimum active version for this feature.</summary>
        public required short MinVersionLevel { get; init; }
        /// <summary>The maximum active version for this feature.</summary>
        public required short MaxVersionLevel { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // Response Header v0 (ApiVersionsResponse uses non-flexible header for ALL versions!)
        // See ApiVersionsResponse.json: "Tagged fields are only supported in the body but not in the header"
        // "The length of the header must not change in order to guarantee the backward compatibility."
        writer.WriteInt32(CorrelationId);
        // NO header tagged fields for ApiVersions - ever!

        // Response Body
        writer.WriteInt16((short)ErrorCode);

        // API versions array - flexible format (COMPACT_ARRAY) only for v3+
        bool isFlexible = ApiVersion >= 3;
        var arrayLength = ApiVersions?.Length ?? 0;

        if (isFlexible)
        {
            writer.WriteVarInt(arrayLength + 1); // COMPACT_ARRAY: length + 1
        }
        else
        {
            writer.WriteInt32(arrayLength);
        }

        if (ApiVersions != null)
        {
            foreach (var apiVersion in ApiVersions)
            {
                writer.WriteInt16(apiVersion.ApiKey);
                writer.WriteInt16(apiVersion.MinVersion);
                writer.WriteInt16(apiVersion.MaxVersion);
                if (isFlexible)
                {
                    writer.WriteVarInt(0); // Tagged fields (empty) - only in flexible
                }
            }
        }

        // Throttle time (added in version 1)
        if (ApiVersion >= 1)
        {
            writer.WriteInt32(ThrottleTimeMs);
        }

        // Response body tagged fields - only in flexible (v3+)
        // Tags: 0=SupportedFeatures, 1=FinalizedFeaturesEpoch, 2=FinalizedFeatures, 3=ZkMigrationReady
        if (isFlexible)
        {
            var tagCount = 0;
            if (SupportedFeatures is { Count: > 0 }) tagCount++;
            if (FinalizedFeaturesEpoch >= 0) tagCount++;
            if (FinalizedFeatures is { Count: > 0 }) tagCount++;
            if (ZkMigrationReady) tagCount++;

            writer.WriteVarInt(tagCount);

            // Tag 0: SupportedFeatures
            if (SupportedFeatures is { Count: > 0 })
            {
                writer.WriteVarInt(0); // Tag 0
                using var featureWriter = new KafkaProtocolWriter(256);
                featureWriter.WriteVarInt(SupportedFeatures.Count + 1);
                foreach (var feature in SupportedFeatures)
                {
                    featureWriter.WriteCompactString(feature.Name);
                    featureWriter.WriteInt16(feature.MinVersion);
                    featureWriter.WriteInt16(feature.MaxVersion);
                    featureWriter.WriteVarInt(0); // No nested tagged fields
                }
                var featureData = featureWriter.ToArray();
                writer.WriteVarInt(featureData.Length);
                writer.WriteRaw(featureData);
            }

            // Tag 1: FinalizedFeaturesEpoch
            if (FinalizedFeaturesEpoch >= 0)
            {
                writer.WriteVarInt(1); // Tag 1
                using var epochWriter = new KafkaProtocolWriter(8);
                epochWriter.WriteInt64(FinalizedFeaturesEpoch);
                var epochData = epochWriter.ToArray();
                writer.WriteVarInt(epochData.Length);
                writer.WriteRaw(epochData);
            }

            // Tag 2: FinalizedFeatures
            if (FinalizedFeatures is { Count: > 0 })
            {
                writer.WriteVarInt(2); // Tag 2
                using var finalWriter = new KafkaProtocolWriter(256);
                finalWriter.WriteVarInt(FinalizedFeatures.Count + 1);
                foreach (var feature in FinalizedFeatures)
                {
                    finalWriter.WriteCompactString(feature.Name);
                    finalWriter.WriteInt16(feature.MinVersionLevel);
                    finalWriter.WriteInt16(feature.MaxVersionLevel);
                    finalWriter.WriteVarInt(0); // No nested tagged fields
                }
                var finalData = finalWriter.ToArray();
                writer.WriteVarInt(finalData.Length);
                writer.WriteRaw(finalData);
            }

            // Tag 3: ZkMigrationReady
            if (ZkMigrationReady)
            {
                writer.WriteVarInt(3); // Tag 3
                writer.WriteVarInt(1); // Size: 1 byte for bool
                writer.WriteInt8(1); // true
            }
        }
    }

    /// <summary>
    /// Create a default response with supported API versions.
    /// These ranges match Kafka 4.2 (trunk) for drop-in compatibility.
    /// Source: C:\Projekte\kafka\clients\src\main\resources\common\message\*Request.json
    /// </summary>
    public static ApiVersionsResponse CreateDefault(int correlationId, short apiVersion)
    {
        return new ApiVersionsResponse
        {
            CorrelationId = correlationId,
            ApiVersion = apiVersion,
            ErrorCode = ErrorCode.None,
            ThrottleTimeMs = 0,
            ApiVersions =
            [
                // Core Client APIs
                // Note: MinVersion=0 enables compression support detection in librdkafka.
                new SupportedApiVersion { ApiKey = (short)ApiKey.Produce, MinVersion = 0, MaxVersion = 13 },              // Kafka: 3-13, 0+ for librdkafka compression
                new SupportedApiVersion { ApiKey = (short)ApiKey.Fetch, MinVersion = 4, MaxVersion = 18 },                // Kafka: 4-18
                new SupportedApiVersion { ApiKey = (short)ApiKey.ListOffsets, MinVersion = 1, MaxVersion = 11 },           // Kafka: 1-11
                new SupportedApiVersion { ApiKey = (short)ApiKey.Metadata, MinVersion = 0, MaxVersion = 13 },              // Kafka: 0-13

                // Consumer Group APIs
                new SupportedApiVersion { ApiKey = (short)ApiKey.OffsetCommit, MinVersion = 2, MaxVersion = 10 },          // Kafka: 2-10
                new SupportedApiVersion { ApiKey = (short)ApiKey.OffsetFetch, MinVersion = 1, MaxVersion = 10 },           // Kafka: 1-10
                new SupportedApiVersion { ApiKey = (short)ApiKey.FindCoordinator, MinVersion = 0, MaxVersion = 6 },        // Kafka: 0-6
                new SupportedApiVersion { ApiKey = (short)ApiKey.JoinGroup, MinVersion = 0, MaxVersion = 9 },              // Kafka: 0-9
                new SupportedApiVersion { ApiKey = (short)ApiKey.Heartbeat, MinVersion = 0, MaxVersion = 4 },              // Kafka: 0-4
                new SupportedApiVersion { ApiKey = (short)ApiKey.LeaveGroup, MinVersion = 0, MaxVersion = 5 },             // Kafka: 0-5
                new SupportedApiVersion { ApiKey = (short)ApiKey.SyncGroup, MinVersion = 0, MaxVersion = 5 },              // Kafka: 0-5
                new SupportedApiVersion { ApiKey = (short)ApiKey.DescribeGroups, MinVersion = 0, MaxVersion = 6 },         // Kafka: 0-6
                new SupportedApiVersion { ApiKey = (short)ApiKey.ListGroups, MinVersion = 0, MaxVersion = 5 },             // Kafka: 0-5
                new SupportedApiVersion { ApiKey = (short)ApiKey.DeleteGroups, MinVersion = 0, MaxVersion = 2 },           // Kafka: 0-2

                // Auth APIs
                new SupportedApiVersion { ApiKey = (short)ApiKey.SaslHandshake, MinVersion = 0, MaxVersion = 1 },          // Kafka: 0-1
                new SupportedApiVersion { ApiKey = (short)ApiKey.ApiVersions, MinVersion = 0, MaxVersion = 5 },            // Kafka: 0-5 (was 0-4)
                new SupportedApiVersion { ApiKey = (short)ApiKey.SaslAuthenticate, MinVersion = 0, MaxVersion = 2 },       // Kafka: 0-2

                // Topic & Partition Admin APIs
                new SupportedApiVersion { ApiKey = (short)ApiKey.CreateTopics, MinVersion = 2, MaxVersion = 7 },           // Kafka: 2-7
                new SupportedApiVersion { ApiKey = (short)ApiKey.DeleteTopics, MinVersion = 1, MaxVersion = 6 },           // Kafka: 1-6
                new SupportedApiVersion { ApiKey = (short)ApiKey.DeleteRecords, MinVersion = 0, MaxVersion = 2 },          // Kafka: 0-2
                new SupportedApiVersion { ApiKey = (short)ApiKey.CreatePartitions, MinVersion = 0, MaxVersion = 3 },       // Kafka: 0-3
                new SupportedApiVersion { ApiKey = (short)ApiKey.DescribeTopicPartitions, MinVersion = 0, MaxVersion = 0 }, // Kafka: 0

                // Transaction APIs
                new SupportedApiVersion { ApiKey = (short)ApiKey.InitProducerId, MinVersion = 0, MaxVersion = 6 },         // Kafka: 0-6 (was 0-4)
                new SupportedApiVersion { ApiKey = (short)ApiKey.AddPartitionsToTxn, MinVersion = 0, MaxVersion = 5 },     // Kafka: 0-5 (was 0-3)
                new SupportedApiVersion { ApiKey = (short)ApiKey.AddOffsetsToTxn, MinVersion = 0, MaxVersion = 4 },        // Kafka: 0-4
                new SupportedApiVersion { ApiKey = (short)ApiKey.EndTxn, MinVersion = 0, MaxVersion = 5 },                 // Kafka: 0-5 (was 0-3)
                new SupportedApiVersion { ApiKey = (short)ApiKey.TxnOffsetCommit, MinVersion = 0, MaxVersion = 5 },        // Kafka: 0-5
                new SupportedApiVersion { ApiKey = (short)ApiKey.DescribeTransactions, MinVersion = 0, MaxVersion = 0 },    // Kafka: 0
                new SupportedApiVersion { ApiKey = (short)ApiKey.ListTransactions, MinVersion = 0, MaxVersion = 2 },        // Kafka: 0-2

                // Config APIs
                new SupportedApiVersion { ApiKey = (short)ApiKey.DescribeConfigs, MinVersion = 1, MaxVersion = 4 },        // Kafka: 1-4 (min was 0)
                new SupportedApiVersion { ApiKey = (short)ApiKey.AlterConfigs, MinVersion = 0, MaxVersion = 2 },           // Kafka: 0-2
                new SupportedApiVersion { ApiKey = (short)ApiKey.IncrementalAlterConfigs, MinVersion = 0, MaxVersion = 1 }, // Kafka: 0-1
                new SupportedApiVersion { ApiKey = (short)ApiKey.ListConfigResources, MinVersion = 0, MaxVersion = 1 },    // Kafka: 0-1

                // ACL APIs
                new SupportedApiVersion { ApiKey = (short)ApiKey.DescribeAcls, MinVersion = 1, MaxVersion = 3 },           // Kafka: 1-3 (was 0-2)
                new SupportedApiVersion { ApiKey = (short)ApiKey.CreateAcls, MinVersion = 1, MaxVersion = 3 },             // Kafka: 1-3 (was 0-2)
                new SupportedApiVersion { ApiKey = (short)ApiKey.DeleteAcls, MinVersion = 1, MaxVersion = 3 },             // Kafka: 1-3 (was 0-2)

                // Delegation Token APIs
                new SupportedApiVersion { ApiKey = (short)ApiKey.CreateDelegationToken, MinVersion = 1, MaxVersion = 3 },  // Kafka: 1-3
                new SupportedApiVersion { ApiKey = (short)ApiKey.RenewDelegationToken, MinVersion = 1, MaxVersion = 2 },   // Kafka: 1-2
                new SupportedApiVersion { ApiKey = (short)ApiKey.ExpireDelegationToken, MinVersion = 1, MaxVersion = 2 },  // Kafka: 1-2
                new SupportedApiVersion { ApiKey = (short)ApiKey.DescribeDelegationToken, MinVersion = 1, MaxVersion = 3 },// Kafka: 1-3

                // Quota APIs
                new SupportedApiVersion { ApiKey = (short)ApiKey.DescribeClientQuotas, MinVersion = 0, MaxVersion = 1 },   // Kafka: 0-1
                new SupportedApiVersion { ApiKey = (short)ApiKey.AlterClientQuotas, MinVersion = 0, MaxVersion = 1 },      // Kafka: 0-1

                // SCRAM APIs
                new SupportedApiVersion { ApiKey = (short)ApiKey.DescribeUserScramCredentials, MinVersion = 0, MaxVersion = 0 }, // Kafka: 0
                new SupportedApiVersion { ApiKey = (short)ApiKey.AlterUserScramCredentials, MinVersion = 0, MaxVersion = 0 },   // Kafka: 0

                // Cluster & Log APIs
                new SupportedApiVersion { ApiKey = (short)ApiKey.DescribeCluster, MinVersion = 0, MaxVersion = 2 },        // Kafka: 0-2
                new SupportedApiVersion { ApiKey = (short)ApiKey.DescribeProducers, MinVersion = 0, MaxVersion = 0 },      // Kafka: 0
                new SupportedApiVersion { ApiKey = (short)ApiKey.DescribeLogDirs, MinVersion = 1, MaxVersion = 5 },        // Kafka: 1-5
                new SupportedApiVersion { ApiKey = (short)ApiKey.AlterReplicaLogDirs, MinVersion = 1, MaxVersion = 2 },    // Kafka: 1-2
                new SupportedApiVersion { ApiKey = (short)ApiKey.ElectLeaders, MinVersion = 0, MaxVersion = 2 },           // Kafka: 0-2

                // Partition Reassignment APIs
                new SupportedApiVersion { ApiKey = (short)ApiKey.AlterPartitionReassignments, MinVersion = 0, MaxVersion = 1 }, // Kafka: 0-1
                new SupportedApiVersion { ApiKey = (short)ApiKey.ListPartitionReassignments, MinVersion = 0, MaxVersion = 0 }, // Kafka: 0
                new SupportedApiVersion { ApiKey = (short)ApiKey.OffsetDelete, MinVersion = 0, MaxVersion = 0 },           // Kafka: 0

                // Consumer Group v2 (KIP-848). librdkafka 2.4+ refuses to start the
                // next-gen consumer flow unless ConsumerGroupHeartbeatRequest v1 is
                // advertised — its check is `Required feature not supported by broker`.
                new SupportedApiVersion { ApiKey = (short)ApiKey.ConsumerGroupHeartbeat, MinVersion = 0, MaxVersion = 1 },
                new SupportedApiVersion { ApiKey = (short)ApiKey.ConsumerGroupDescribe, MinVersion = 0, MaxVersion = 1 },

                // Client Telemetry (KIP-714)
                new SupportedApiVersion { ApiKey = (short)ApiKey.GetTelemetrySubscriptions, MinVersion = 0, MaxVersion = 0 }, // Kafka: 0
                new SupportedApiVersion { ApiKey = (short)ApiKey.PushTelemetry, MinVersion = 0, MaxVersion = 0 },          // Kafka: 0

                // Share Groups (KIP-932)
                new SupportedApiVersion { ApiKey = (short)ApiKey.ShareGroupHeartbeat, MinVersion = 1, MaxVersion = 1 },    // Kafka: 1
                new SupportedApiVersion { ApiKey = (short)ApiKey.ShareGroupDescribe, MinVersion = 1, MaxVersion = 1 },     // Kafka: 1
                new SupportedApiVersion { ApiKey = (short)ApiKey.ShareFetch, MinVersion = 1, MaxVersion = 2 },             // Kafka: 1-2
                new SupportedApiVersion { ApiKey = (short)ApiKey.ShareAcknowledge, MinVersion = 1, MaxVersion = 2 },       // Kafka: 1-2
                new SupportedApiVersion { ApiKey = (short)ApiKey.DescribeShareGroupOffsets, MinVersion = 0, MaxVersion = 1 }, // Kafka: 0-1
                new SupportedApiVersion { ApiKey = (short)ApiKey.AlterShareGroupOffsets, MinVersion = 0, MaxVersion = 0 },   // Kafka: 0
                new SupportedApiVersion { ApiKey = (short)ApiKey.DeleteShareGroupOffsets, MinVersion = 0, MaxVersion = 0 },   // Kafka: 0

                // Streams Group Protocol (KIP-1071)
                new SupportedApiVersion { ApiKey = (short)ApiKey.StreamsGroupHeartbeat, MinVersion = 0, MaxVersion = 0 },  // Kafka: 0
                new SupportedApiVersion { ApiKey = (short)ApiKey.StreamsGroupDescribe, MinVersion = 0, MaxVersion = 0 },   // Kafka: 0
            ],
            // KIP-848 capability advertisement: librdkafka 2.4+ refuses to start the
            // ConsumerGroupHeartbeat loop unless `group.version >= 1` is offered both
            // in SupportedFeatures (range) and FinalizedFeatures (active level).
            // Without these tagged fields the next-gen consumer hangs in join-state
            // init forever even though the broker advertises every API key correctly.
            SupportedFeatures =
            [
                new SupportedFeature { Name = "group.version", MinVersion = 0, MaxVersion = 1 },
                new SupportedFeature { Name = "transaction.version", MinVersion = 0, MaxVersion = 2 },
                new SupportedFeature { Name = "metadata.version", MinVersion = 14, MaxVersion = 22 },
                // KAFKA-20415 / KIP-1191: share.version=2 fuegt DLQ-Support fuer Share-Groups
                // hinzu (gated bei IBP_4_4_IV0). Surgewave hat aktuell keine DLQ-Implementierung,
                // daher advertised wir nur 0-1. Sobald KIP-1191 in ShareGroupCoordinator
                // umgesetzt ist, MaxVersion auf 2 erhoehen.
                new SupportedFeature { Name = "share.version", MinVersion = 0, MaxVersion = 1 },
            ],
            FinalizedFeaturesEpoch = 1,
            FinalizedFeatures =
            [
                new FinalizedFeature { Name = "group.version", MinVersionLevel = 1, MaxVersionLevel = 1 },
                new FinalizedFeature { Name = "transaction.version", MinVersionLevel = 0, MaxVersionLevel = 2 },
                new FinalizedFeature { Name = "metadata.version", MinVersionLevel = 22, MaxVersionLevel = 22 },
                new FinalizedFeature { Name = "share.version", MinVersionLevel = 1, MaxVersionLevel = 1 },
            ],
        };
    }
}
