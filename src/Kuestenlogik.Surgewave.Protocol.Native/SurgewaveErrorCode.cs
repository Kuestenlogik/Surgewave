namespace Kuestenlogik.Surgewave.Protocol.Native;

/// <summary>
/// Error codes for Surgewave native protocol
/// </summary>
public enum SurgewaveErrorCode : ushort
{
    None = 0,
    UnknownError = 1,
    InvalidRequest = 2,
    TopicNotFound = 3,
    PartitionNotFound = 4,
    NotLeader = 5,
    AuthenticationFailed = 6,
    AuthorizationFailed = 7,
    InvalidOffset = 8,
    MessageTooLarge = 9,
    GroupNotFound = 10,
    RebalanceInProgress = 11,
    InvalidSession = 12,
    Timeout = 13,
    MemberIdRequired = 14,
    UnknownMemberId = 15,
    IllegalGeneration = 16,
    InconsistentGroupProtocol = 17,
    GroupNotEmpty = 18,
    GroupAuthorizationFailed = 19,
    NotCoordinator = 20,
    CoordinatorNotAvailable = 21,

    // Transaction errors
    InvalidProducerEpoch = 30,
    UnknownProducerId = 31,
    InvalidTxnState = 32,
    TransactionAborted = 33,
    ConcurrentTransactions = 34,
    TransactionTimeout = 35,
    DuplicateSequenceNumber = 36,
    OutOfOrderSequenceNumber = 37,

    // Security errors
    SecurityDisabled = 40,
    InvalidAclFilter = 41,
    AclNotFound = 42,

    // Config errors
    InvalidConfig = 50,
    ConfigNotFound = 51,

    // Leader election errors
    ElectionNotNeeded = 60,
    PreferredLeaderNotAvailable = 61,
    EligibleLeadersNotAvailable = 62,

    // Schema Registry errors
    SchemaNotFound = 70,
    SubjectNotFound = 71,
    VersionNotFound = 72,
    IncompatibleSchema = 73,
    InvalidSchema = 74,
    SchemaRegistryDisabled = 75,

    // Connect errors
    ConnectorNotFound = 80,
    ConnectorAlreadyExists = 81,
    TaskNotFound = 82,
    InvalidConnectorConfig = 83,
    ConnectDisabled = 84,
    ConnectorFailed = 85,

    // Plugin/Marketplace errors
    PluginManagerDisabled = 90,
    PluginNotFound = 91,
    PluginAlreadyInstalled = 92,
    PluginInstallFailed = 93,
    PluginUninstallFailed = 94,
    DependencyResolutionFailed = 95,

    // Streaming errors
    SubscriptionAlreadyExists = 110,
    SubscriptionNotFound = 111,
    MaxSubscriptionsExceeded = 112,

    // Cross-Topic Transaction errors
    CrossTopicTxnNotFound = 100,
    CrossTopicTxnInvalidState = 101,
    CrossTopicTxnTimedOut = 102,
    CrossTopicTxnMaxWritesExceeded = 103,
    CrossTopicTxnCommitFailed = 104,
    CrossTopicTxnDisabled = 105,

    // Key-Value Store errors
    KvBucketNotFound = 120,
    KvBucketAlreadyExists = 121,
    KvKeyNotFound = 122,
    KvValueTooLarge = 123,

    // Object Store errors
    ObjStoreNotFound = 130,
    ObjObjectNotFound = 131,
}
