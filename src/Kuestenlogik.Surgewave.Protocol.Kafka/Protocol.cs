using Kuestenlogik.Surgewave.Protocol;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Kafka API keys for different request types
/// </summary>
public enum ApiKey : short
{
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    DeleteRecords = 21,
    InitProducerId = 22,
    OffsetForLeaderEpoch = 23,
    AddPartitionsToTxn = 24,
    AddOffsetsToTxn = 25,
    EndTxn = 26,
    WriteTxnMarkers = 27,
    TxnOffsetCommit = 28,
    DescribeAcls = 29,
    CreateAcls = 30,
    DeleteAcls = 31,
    DescribeConfigs = 32,
    AlterConfigs = 33,
    AlterReplicaLogDirs = 34,
    DescribeLogDirs = 35,
    SaslAuthenticate = 36,
    CreatePartitions = 37,
    CreateDelegationToken = 38,
    RenewDelegationToken = 39,
    ExpireDelegationToken = 40,
    DescribeDelegationToken = 41,
    DeleteGroups = 42,
    ElectLeaders = 43,
    IncrementalAlterConfigs = 44,
    AlterPartitionReassignments = 45,
    ListPartitionReassignments = 46,
    OffsetDelete = 47,
    DescribeClientQuotas = 48,
    AlterClientQuotas = 49,
    DescribeUserScramCredentials = 50,
    AlterUserScramCredentials = 51,
    Vote = 52,
    BeginQuorumEpoch = 53,
    EndQuorumEpoch = 54,
    DescribeQuorum = 55,
    AlterPartition = 56,
    UpdateFeatures = 57,
    Envelope = 58,
    FetchSnapshot = 59,
    DescribeCluster = 60,
    DescribeProducers = 61,
    BrokerRegistration = 62,
    BrokerHeartbeat = 63,
    UnregisterBroker = 64,
    DescribeTransactions = 65,
    ListTransactions = 66,
    AllocateProducerIds = 67,
    ConsumerGroupHeartbeat = 68,
    ConsumerGroupDescribe = 69,
    ControllerRegistration = 70,
    GetTelemetrySubscriptions = 71,
    PushTelemetry = 72,
    AssignReplicasToDirs = 73,
    ListConfigResources = 74,
    DescribeTopicPartitions = 75,

    // Kafka 4.2: Share Groups (KIP-932)
    ShareGroupHeartbeat = 76,
    ShareGroupDescribe = 77,
    ShareFetch = 78,
    ShareAcknowledge = 79,

    // Kafka 4.1: Dynamic Raft Voters (KIP-853)
    AddRaftVoter = 80,
    RemoveRaftVoter = 81,
    UpdateRaftVoter = 82,

    // Kafka 4.2: Share Group State (KIP-932 internal)
    InitializeShareGroupState = 83,
    ReadShareGroupState = 84,
    WriteShareGroupState = 85,
    DeleteShareGroupState = 86,
    ReadShareGroupStateSummary = 87,

    // Kafka 4.2: Streams Group Protocol (KIP-1071)
    StreamsGroupHeartbeat = 88,
    StreamsGroupDescribe = 89,

    // Kafka 4.2: Share Group Offset Management (KIP-932)
    DescribeShareGroupOffsets = 90,
    AlterShareGroupOffsets = 91,
    DeleteShareGroupOffsets = 92
}

/// <summary>
/// Error codes compatible with Kafka
/// </summary>
public enum ErrorCode : short
{
    None = 0,
    Unknown = -1,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidFetchSize = 4,
    LeaderNotAvailable = 5,
    NotLeaderForPartition = 6,
    RequestTimedOut = 7,
    BrokerNotAvailable = 8,
    ReplicaNotAvailable = 9,
    MessageTooLarge = 10,
    StaleControllerEpoch = 11,
    OffsetMetadataTooLarge = 12,
    NetworkException = 13,
    CoordinatorLoadInProgress = 14,
    CoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    InvalidTopicException = 17,
    RecordListTooLarge = 18,
    NotEnoughReplicas = 19,
    NotEnoughReplicasAfterAppend = 20,
    InvalidRequiredAcks = 21,
    IllegalGeneration = 22,
    InconsistentGroupProtocol = 23,
    InvalidGroupId = 24,
    UnknownMemberId = 25,
    InvalidSessionTimeout = 26,
    RebalanceInProgress = 27,
    InvalidCommitOffsetSize = 28,
    TopicAuthorizationFailed = 29,
    GroupAuthorizationFailed = 30,
    ClusterAuthorizationFailed = 31,
    InvalidTimestamp = 32,
    UnsupportedSaslMechanism = 33,
    IllegalSaslState = 34,
    UnsupportedVersion = 35,
    TopicAlreadyExists = 36,
    InvalidPartitions = 37,
    InvalidReplicationFactor = 38,
    InvalidReplicaAssignment = 39,
    InvalidConfig = 40,
    NotController = 41,
    InvalidRequest = 42,
    UnsupportedForMessageFormat = 43,

    // Transaction and idempotent producer errors
    OutOfOrderSequenceNumber = 44,
    DuplicateSequenceNumber = 46,
    InvalidProducerEpoch = 47,
    InvalidTxnState = 48,
    InvalidProducerIdMapping = 49,
    InvalidTransactionTimeout = 50,
    ConcurrentTransactions = 51,
    TransactionCoordinatorFenced = 52,
    TransactionalIdAuthorizationFailed = 53,
    SecurityDisabled = 54,
    OperationNotAttempted = 55,
    KafkaStorageError = 56,
    LogDirNotFound = 57,
    SaslAuthenticationFailed = 58,
    UnknownProducerId = 59,
    ReassignmentInProgress = 60,
    DelegationTokenAuthDisabled = 61,
    DelegationTokenNotFound = 62,
    DelegationTokenOwnerMismatch = 63,
    DelegationTokenRequestNotAllowed = 64,
    DelegationTokenAuthorizationFailed = 65,
    DelegationTokenExpired = 66,
    InvalidPrincipalType = 67,

    // Consumer group errors (continued)
    NonEmptyGroup = 68,
    GroupIdNotFound = 69,

    UnsupportedCompressionType = 76,
    StaleBrokerEpoch = 77,

    // Reassignment / leader-election errors (KIP-455 / KIP-460)
    PreferredLeaderNotAvailable = 80,
    ElectionNotNeeded = 84,
    NoReassignmentInProgress = 85,
    GroupSubscribedToTopic = 86,

    // KRaft errors
    SnapshotNotFound = 87,

    // KIP-554 SCRAM credential management
    ResourceNotFound = 91,
    DuplicateResource = 92,
    UnacceptableCredential = 93,

    // Topic ID errors (KIP-516)
    UnknownTopicId = 100,

    // Cluster ID errors
    InconsistentClusterId = 104,

    // KIP-848 next-gen consumer protocol
    /// <summary>The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin.</summary>
    FencedMemberEpoch = 110,

    /// <summary>The instance ID is still used by another member in the consumer group. That member must leave first.</summary>
    UnreleasedInstanceId = 111,

    /// <summary>The assignor or its version range is not supported by the consumer group.</summary>
    UnsupportedAssignor = 112,

    /// <summary>The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API.</summary>
    StaleMemberEpoch = 113,

    /// <summary>
    /// Client metadata is stale; the client should rebootstrap to obtain new metadata (KIP-1242).
    /// Surfaced from <see cref="ApiKey.ApiVersions"/> v5+ when the client's supplied
    /// ClusterId / NodeId hint disagrees with the broker's own identity — typically after an
    /// IP-reuse where the client cached the wrong broker's bootstrap address.
    /// </summary>
    RebootstrapRequired = 129,
}

/// <summary>
/// Base class for all Kafka protocol requests
/// </summary>
public abstract class KafkaRequest : IProtocolRequest
{
    public required ApiKey ApiKey { get; init; }
    public required short ApiVersion { get; init; }
    public required int CorrelationId { get; init; }
    public required string ClientId { get; init; }

    public abstract void WriteTo(KafkaProtocolWriter writer);

    /// <summary>
    /// Serialize the request to binary format
    /// </summary>
    public byte[] Serialize()
    {
        using var writer = new KafkaProtocolWriter();
        WriteTo(writer);
        return writer.ToArray();
    }
}

/// <summary>
/// Base class for all Kafka protocol responses
/// </summary>
public abstract class KafkaResponse : IProtocolResponse
{
    public required int CorrelationId { get; init; }
    public required short ApiVersion { get; init; }

    public abstract void WriteTo(KafkaProtocolWriter writer);

    /// <summary>
    /// Serialize the response to binary format
    /// </summary>
    public byte[] Serialize()
    {
        using var writer = new KafkaProtocolWriter();
        WriteTo(writer);
        return writer.ToArray();
    }
}
