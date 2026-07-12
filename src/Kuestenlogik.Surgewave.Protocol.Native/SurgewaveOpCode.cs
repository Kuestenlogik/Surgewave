namespace Kuestenlogik.Surgewave.Protocol.Native;

/// <summary>
/// Operation codes for Surgewave native protocol
/// </summary>
public enum SurgewaveOpCode : ushort
{
    /// <summary>No operation (default value)</summary>
    None = 0x0000,

    // Connection & Metadata (0x00xx)
    Handshake = 0x0001,
    Ping = 0x0002,
    Pong = 0x0003,
    GetMetadata = 0x0004,

    // Produce operations (0x01xx)
    Produce = 0x0100,
    ProduceBatch = 0x0101,
    ProduceAck = 0x0102,

    // Consume operations (0x02xx)
    Fetch = 0x0200,
    FetchResponse = 0x0201,
    Subscribe = 0x0202,
    Unsubscribe = 0x0203,

    // Nack / DLQ operations (0x02xx continued)
    Nack = 0x0204,
    NackAck = 0x0205,
    StreamAck = 0x0206,

    // Offset management (0x03xx)
    CommitOffset = 0x0300,
    FetchOffset = 0x0301,
    ListOffsets = 0x0302,

    // Consumer groups (0x04xx)
    JoinGroup = 0x0400,
    SyncGroup = 0x0401,
    LeaveGroup = 0x0402,
    Heartbeat = 0x0403,
    ListGroups = 0x0404,
    DescribeGroup = 0x0405,
    DeleteGroup = 0x0406,
    FindCoordinator = 0x0407,
    GetGroupLag = 0x0408,
    GetLagSummary = 0x0409,

    // Admin operations (0x05xx)
    CreateTopic = 0x0500,
    DeleteTopic = 0x0501,
    ListTopics = 0x0502,
    DescribeTopic = 0x0503,
    AlterConfig = 0x0504,
    DescribeConfig = 0x0505,
    GetClusterInfo = 0x0506,
    ListBrokers = 0x0507,
    AlterPartitionReassignments = 0x0508,
    ListPartitionReassignments = 0x0509,
    TriggerLogCompaction = 0x050A,
    GetCompactionStatus = 0x050B,
    VerifyLogIntegrity = 0x050C,
    CreatePartitions = 0x050D,
    DeleteRecords = 0x050E,

    // Transaction operations (0x06xx)
    InitProducerId = 0x0600,
    AddPartitionsToTxn = 0x0601,
    AddOffsetsToTxn = 0x0602,
    TxnOffsetCommit = 0x0603,
    EndTxn = 0x0604,
    ListTransactions = 0x0605,
    DescribeTransactions = 0x0606,

    // Quota operations (0x07xx)
    GetQuotaConfig = 0x0700,
    SetQuotaConfig = 0x0701,
    DescribeClientQuotas = 0x0702,
    ListClientQuotas = 0x0703,

    // Security operations (0x08xx)
    DescribeAcls = 0x0800,
    CreateAcls = 0x0801,
    DeleteAcls = 0x0802,

    // Leader operations (0x09xx)
    ElectLeader = 0x0900,
    DescribeBrokerConfig = 0x0901,
    AlterBrokerConfig = 0x0902,

    // Schema Registry operations (0x0Axx)
    ListSubjects = 0x0A00,
    GetSubjectVersions = 0x0A01,
    RegisterSchema = 0x0A02,
    GetSchemaById = 0x0A03,
    GetSchemaByVersion = 0x0A04,
    DeleteSubject = 0x0A05,
    DeleteSchemaVersion = 0x0A06,
    CheckCompatibility = 0x0A07,
    GetCompatibilityConfig = 0x0A08,
    SetCompatibilityConfig = 0x0A09,
    GetSchemaTypes = 0x0A0A,

    // Connect operations (0x0Bxx)
    ListConnectors = 0x0B00,
    GetConnector = 0x0B01,
    CreateConnector = 0x0B02,
    DeleteConnector = 0x0B03,
    GetConnectorConfig = 0x0B04,
    UpdateConnectorConfig = 0x0B05,
    GetConnectorStatus = 0x0B06,
    RestartConnector = 0x0B07,
    PauseConnector = 0x0B08,
    ResumeConnector = 0x0B09,
    GetConnectorTasks = 0x0B0A,
    RestartConnectorTask = 0x0B0B,
    ListConnectorPlugins = 0x0B0C,

    // Delegation Token operations (0x0Cxx)
    CreateDelegationToken = 0x0C00,
    RenewDelegationToken = 0x0C01,
    ExpireDelegationToken = 0x0C02,
    DescribeDelegationTokens = 0x0C03,

    // Plugin/Marketplace operations (0x0Dxx)
    SearchPlugins = 0x0D00,
    GetPlugin = 0x0D01,
    InstallPlugin = 0x0D02,
    UninstallPlugin = 0x0D03,
    ListInstalledPlugins = 0x0D04,
    GetPluginDependencies = 0x0D05,
    UploadPlugin = 0x0D06,
    PushPluginNotification = 0x0D07,
    PullPlugin = 0x0D08,

    // Share Groups (0x0Fxx) — per-message ack/nack, consumer scales beyond partition count
    ShareGroupHeartbeat = 0x0F00,
    ShareGroupDescribe = 0x0F01,
    ShareFetch = 0x0F02,
    ShareAcknowledge = 0x0F03,
    ShareGroupJoin = 0x0F04,
    ShareGroupLeave = 0x0F05,
    DescribeShareGroupOffsets = 0x0F06,
    AlterShareGroupOffsets = 0x0F07,
    DeleteShareGroupOffsets = 0x0F08,

    // Consumer Group v2 — KIP-848 (0x10xx) — server-side rebalance, no SyncGroup needed
    ConsumerGroupHeartbeat = 0x1000,
    ConsumerGroupDescribe = 0x1001,

    // Client Telemetry — KIP-714 (0x11xx) — client metrics collection
    GetTelemetrySubscriptions = 0x1100,
    PushTelemetry = 0x1101,

    // Streams Groups — KIP-1071 (0x12xx) — server-side streams rebalance
    StreamsGroupHeartbeat = 0x1200,
    StreamsGroupDescribe = 0x1201,

    // Cross-Topic Transaction operations (0x0Exx)
    CrossTopicTxnBegin = 0x0E00,
    CrossTopicTxnBeginAck = 0x0E01,
    CrossTopicTxnAddWrite = 0x0E02,
    CrossTopicTxnAddWriteAck = 0x0E03,
    CrossTopicTxnCommit = 0x0E04,
    CrossTopicTxnCommitAck = 0x0E05,
    CrossTopicTxnAbort = 0x0E06,
    CrossTopicTxnAbortAck = 0x0E07,

    // Key-Value Store (0x13xx)
    KvCreateBucket = 0x1300,
    KvDeleteBucket = 0x1301,
    KvListBuckets = 0x1302,
    KvGet = 0x1303,
    KvPut = 0x1304,
    KvDelete = 0x1305,
    KvListKeys = 0x1306,
    KvHistory = 0x1307,
    KvWatch = 0x1308,
    KvPurge = 0x1309,

    // Object Store (0x14xx)
    ObjCreateStore = 0x1400,
    ObjPutObject = 0x1401,
    ObjGetObject = 0x1402,
    ObjDeleteObject = 0x1403,
    ObjListObjects = 0x1404,
    ObjGetObjectInfo = 0x1405,

    // #60 — inter-broker control plane (0x15xx): native inter-broker RPC so a broker without the
    // Protocol.Kafka plugin can cluster. Definitions only until the native inter-broker server (Inc4)
    // and clients (Inc5-7) are wired; nothing reads these yet.
    InterBrokerLeaderAndIsr = 0x1500,
    InterBrokerUpdateMetadata = 0x1501,
    InterBrokerStopReplica = 0x1502,
    InterBrokerAlterPartition = 0x1503,
    InterBrokerRegistration = 0x1504,
    InterBrokerHeartbeat = 0x1505,
    InterBrokerControlledShutdown = 0x1506,
    InterBrokerWriteTxnMarkers = 0x1507,
    InterBrokerReplicaFetch = 0x1508,

    // #60 — inter-broker Raft/KRaft consensus (0x16xx). The real consensus already rides a
    // hand-rolled framing on the ReplicationPort (Family B); these opcodes are for unifying it
    // onto the SRWV frame later (Inc8, optional).
    RaftRequestVote = 0x1600,
    RaftPreVote = 0x1601,
    RaftAppendEntries = 0x1602,
    RaftMetadataUpdatePush = 0x1603,

    // Error response (0xFFxx)
    Error = 0xFF00,
}
