namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Protocol error codes. The numeric values are Kafka-wire compatible (KIP error codes),
/// but this enum is the neutral, shared error vocabulary for the whole system: the native
/// protocol, the client SDK exceptions and the Kafka wire plugin all surface it.
/// </summary>
/// <remarks>
/// #59: physically relocated from the Protocol.Kafka plugin into the neutral Protocol
/// assembly so the client SDK (ConsumeException / ProduceException / RecoverySuggestion)
/// no longer drags Protocol.Kafka into every consumer's binary. The namespace is kept as
/// <c>Kuestenlogik.Surgewave.Protocol.Kafka</c> deliberately, so no call site changes:
/// Protocol.Kafka references Protocol and re-sees the type transparently.
/// </remarks>
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

    /// <summary>Request parameters do not satisfy the configured policy.</summary>
    PolicyViolation = 44,

    // Transaction and idempotent producer errors
    // 45, not 44: this used to be numbered one too low, which put it on POLICY_VIOLATION's code and
    // left 45 unused. An idempotent producer that broke its sequence was told its request violated
    // a policy — both are fatal to the client, so nothing hung, but the diagnosis was wrong and any
    // client branching on 45 never saw it.
    OutOfOrderSequenceNumber = 45,
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

    /// <summary>
    /// The record failed broker-side validation and was rejected. Kafka's INVALID_RECORD.
    /// </summary>
    /// <remarks>
    /// Distinct from <see cref="CorruptMessage"/>, which means the bytes did not survive transport:
    /// this one means well-formed bytes the broker will not accept. Kafka uses it for, among other
    /// things, a produce request carrying more than one record batch per partition
    /// (ProduceRequest.validateRecords: "only allowed to contain exactly one record batch per
    /// partition").
    /// </remarks>
    InvalidRecord = 87,

    // KRaft errors
    SnapshotNotFound = 98,

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
    /// Surfaced from ApiVersions v5+ when the client's supplied ClusterId / NodeId hint disagrees
    /// with the broker's own identity — typically after an IP-reuse where the client cached the
    /// wrong broker's bootstrap address.
    /// </summary>
    RebootstrapRequired = 129,
}
