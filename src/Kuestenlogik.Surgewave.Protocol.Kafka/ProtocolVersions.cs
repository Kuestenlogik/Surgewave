namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Centralized Kafka protocol version information and feature detection.
/// This class provides a single source of truth for protocol version thresholds
/// to avoid scattered version checks throughout the codebase.
/// </summary>
public static class ProtocolVersions
{
    /// <summary>
    /// Determines if a given API version uses the flexible format (compact strings/arrays, tagged fields).
    /// These thresholds are derived from Kafka's JSON schema definitions:
    /// kafka/clients/src/main/resources/common/message/*.json
    /// </summary>
    public static bool IsFlexible(ApiKey apiKey, short apiVersion)
    {
        return apiKey switch
        {
            // Core Client APIs
            ApiKey.Produce => apiVersion >= 9,           // flexibleVersions: "9+"
            ApiKey.Fetch => apiVersion >= 12,            // flexibleVersions: "12+"
            ApiKey.ListOffsets => apiVersion >= 6,       // flexibleVersions: "6+"
            ApiKey.Metadata => apiVersion >= 9,          // flexibleVersions: "9+"

            // Consumer Group APIs
            ApiKey.OffsetCommit => apiVersion >= 8,      // flexibleVersions: "8+"
            ApiKey.OffsetFetch => apiVersion >= 6,       // flexibleVersions: "6+"
            ApiKey.FindCoordinator => apiVersion >= 3,   // flexibleVersions: "3+"
            ApiKey.JoinGroup => apiVersion >= 6,         // flexibleVersions: "6+"
            ApiKey.Heartbeat => apiVersion >= 4,         // flexibleVersions: "4+"
            ApiKey.LeaveGroup => apiVersion >= 4,        // flexibleVersions: "4+"
            ApiKey.SyncGroup => apiVersion >= 4,         // flexibleVersions: "4+"
            ApiKey.DescribeGroups => apiVersion >= 5,    // flexibleVersions: "5+"
            ApiKey.ListGroups => apiVersion >= 3,        // flexibleVersions: "3+"

            // Admin APIs
            ApiKey.CreateTopics => apiVersion >= 5,      // flexibleVersions: "5+"
            ApiKey.DeleteTopics => apiVersion >= 4,      // flexibleVersions: "4+"
            ApiKey.DeleteRecords => apiVersion >= 2,     // flexibleVersions: "2+"
            ApiKey.CreatePartitions => apiVersion >= 2,  // flexibleVersions: "2+"
            ApiKey.DescribeConfigs => apiVersion >= 4,   // flexibleVersions: "4+"
            ApiKey.AlterConfigs => apiVersion >= 2,      // flexibleVersions: "2+"
            ApiKey.IncrementalAlterConfigs => apiVersion >= 1, // flexibleVersions: "1+"
            ApiKey.DescribeAcls => apiVersion >= 2,      // flexibleVersions: "2+"
            ApiKey.CreateAcls => apiVersion >= 2,        // flexibleVersions: "2+"
            ApiKey.DeleteAcls => apiVersion >= 2,        // flexibleVersions: "2+"

            // Transaction APIs
            ApiKey.InitProducerId => apiVersion >= 2,    // flexibleVersions: "2+"
            ApiKey.AddPartitionsToTxn => apiVersion >= 3,// flexibleVersions: "3+"
            ApiKey.AddOffsetsToTxn => apiVersion >= 3,   // flexibleVersions: "3+"
            ApiKey.EndTxn => apiVersion >= 3,            // flexibleVersions: "3+"
            ApiKey.TxnOffsetCommit => apiVersion >= 3,   // flexibleVersions: "3+"

            // Auth APIs
            ApiKey.SaslHandshake => false,               // flexibleVersions: "none"
            ApiKey.SaslAuthenticate => apiVersion >= 2,  // flexibleVersions: "2+"
            ApiKey.ApiVersions => apiVersion >= 3,       // flexibleVersions: "3+"

            // Inter-broker APIs
            ApiKey.LeaderAndIsr => apiVersion >= 4,      // flexibleVersions: "4+"
            ApiKey.StopReplica => apiVersion >= 2,       // flexibleVersions: "2+"
            ApiKey.UpdateMetadata => apiVersion >= 6,    // flexibleVersions: "6+"
            ApiKey.ControlledShutdown => apiVersion >= 3,// flexibleVersions: "3+"
            ApiKey.AlterPartition => true,               // flexibleVersions: "0+"

            // KRaft Cluster Membership APIs (always flexible)
            ApiKey.BrokerRegistration => true,           // flexibleVersions: "0+"
            ApiKey.BrokerHeartbeat => true,              // flexibleVersions: "0+"

            // KIP-848 next-gen Consumer Group Protocol (always flexible)
            ApiKey.ConsumerGroupHeartbeat => true,       // flexibleVersions: "0+"
            ApiKey.ConsumerGroupDescribe => true,        // flexibleVersions: "0+"

            // KIP-932 Share Groups (always flexible)
            ApiKey.ShareGroupHeartbeat => true,          // flexibleVersions: "0+"
            ApiKey.ShareGroupDescribe => true,           // flexibleVersions: "0+"
            ApiKey.ShareFetch => true,                   // flexibleVersions: "0+"
            ApiKey.ShareAcknowledge => true,             // flexibleVersions: "0+"
            ApiKey.InitializeShareGroupState => true,    // flexibleVersions: "0+"
            ApiKey.ReadShareGroupState => true,          // flexibleVersions: "0+"
            ApiKey.WriteShareGroupState => true,         // flexibleVersions: "0+"
            ApiKey.DeleteShareGroupState => true,        // flexibleVersions: "0+"
            ApiKey.ReadShareGroupStateSummary => true,   // flexibleVersions: "0+"
            ApiKey.DescribeShareGroupOffsets => true,    // flexibleVersions: "0+"
            ApiKey.AlterShareGroupOffsets => true,       // flexibleVersions: "0+"
            ApiKey.DeleteShareGroupOffsets => true,      // flexibleVersions: "0+"

            // KIP-1071 Streams Group Protocol (always flexible)
            ApiKey.StreamsGroupHeartbeat => true,        // flexibleVersions: "0+"
            ApiKey.StreamsGroupDescribe => true,         // flexibleVersions: "0+"

            // KIP-714 Client Telemetry (always flexible)
            ApiKey.GetTelemetrySubscriptions => true,    // flexibleVersions: "0+"
            ApiKey.PushTelemetry => true,                // flexibleVersions: "0+"

            // KIP-853 Dynamic Raft Voters (always flexible)
            ApiKey.AddRaftVoter => true,                 // flexibleVersions: "0+"
            ApiKey.RemoveRaftVoter => true,              // flexibleVersions: "0+"
            ApiKey.UpdateRaftVoter => true,              // flexibleVersions: "0+"

            _ => false
        };
    }

    /// <summary>
    /// Version-specific feature flags for each API.
    /// These define when specific features were introduced in the Kafka protocol.
    /// </summary>
    public static class Features
    {
        /// <summary>
        /// Produce API feature versions
        /// </summary>
        public static class Produce
        {
            /// <summary>TransactionalId support (v3+)</summary>
            public const short TransactionalIdVersion = 3;

            /// <summary>Flexible format (v9+)</summary>
            public const short FlexibleVersion = 9;
        }

        /// <summary>
        /// Fetch API feature versions
        /// </summary>
        public static class Fetch
        {
            /// <summary>MaxBytes field (v3+)</summary>
            public const short MaxBytesVersion = 3;

            /// <summary>IsolationLevel field (v4+)</summary>
            public const short IsolationLevelVersion = 4;

            /// <summary>LogStartOffset in response (v5+)</summary>
            public const short LogStartOffsetVersion = 5;

            /// <summary>Session support (v7+)</summary>
            public const short SessionVersion = 7;

            /// <summary>CurrentLeaderEpoch field (v9+)</summary>
            public const short CurrentLeaderEpochVersion = 9;

            /// <summary>PreferredReadReplica in response (v11+)</summary>
            public const short PreferredReadReplicaVersion = 11;

            /// <summary>Flexible format (v12+)</summary>
            public const short FlexibleVersion = 12;

            /// <summary>LastFetchedEpoch field (v12+)</summary>
            public const short LastFetchedEpochVersion = 12;
        }

        /// <summary>
        /// Metadata API feature versions
        /// </summary>
        public static class Metadata
        {
            /// <summary>Flexible format (v9+)</summary>
            public const short FlexibleVersion = 9;
        }

        /// <summary>
        /// OffsetCommit API feature versions
        /// </summary>
        public static class OffsetCommit
        {
            /// <summary>Flexible format (v8+)</summary>
            public const short FlexibleVersion = 8;
        }

        /// <summary>
        /// OffsetFetch API feature versions
        /// </summary>
        public static class OffsetFetch
        {
            /// <summary>Flexible format (v6+)</summary>
            public const short FlexibleVersion = 6;
        }

        /// <summary>
        /// ListOffsets API feature versions
        /// </summary>
        public static class ListOffsets
        {
            /// <summary>Flexible format (v6+)</summary>
            public const short FlexibleVersion = 6;
        }

        /// <summary>
        /// JoinGroup API feature versions
        /// </summary>
        public static class JoinGroup
        {
            /// <summary>Flexible format (v6+)</summary>
            public const short FlexibleVersion = 6;
        }

        /// <summary>
        /// SyncGroup API feature versions
        /// </summary>
        public static class SyncGroup
        {
            /// <summary>Flexible format (v4+)</summary>
            public const short FlexibleVersion = 4;
        }

        /// <summary>
        /// Heartbeat API feature versions
        /// </summary>
        public static class Heartbeat
        {
            /// <summary>Flexible format (v4+)</summary>
            public const short FlexibleVersion = 4;
        }

        /// <summary>
        /// FindCoordinator API feature versions
        /// </summary>
        public static class FindCoordinator
        {
            /// <summary>Flexible format (v3+)</summary>
            public const short FlexibleVersion = 3;

            /// <summary>Batch coordinator lookup with CoordinatorKeys array (v4+)</summary>
            public const short BatchLookupVersion = 4;
        }

        /// <summary>
        /// InitProducerId API feature versions
        /// </summary>
        public static class InitProducerId
        {
            /// <summary>Flexible format (v2+)</summary>
            public const short FlexibleVersion = 2;
        }
    }

}

/// <summary>
/// Extension methods for convenient version checking
/// </summary>
public static class ProtocolVersionExtensions
{
    /// <summary>
    /// Checks if this request uses the flexible format
    /// </summary>
    public static bool IsFlexible(this KafkaRequest request)
    {
        return ProtocolVersions.IsFlexible(request.ApiKey, request.ApiVersion);
    }
}
