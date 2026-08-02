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
/// Base class for all Kafka protocol responses.
///
/// <para>A response may carry <b>borrowed</b> memory — the fetch path serves record sets straight
/// out of a pooled or memory-mapped storage lease instead of copying them (#78). Such a response
/// owns that lease until it has been serialized: <b>whoever received a response calls
/// <see cref="ReleaseBorrowedMemory"/> once it has been written</b> — for the broker that is the
/// connection loop in <c>KafkaConnectionHandler</c>. Responses that borrow nothing (the vast
/// majority) release to a no-op, so the rule is uniform and costs nothing.</para>
///
/// <para><b>Do not read a response's payload after releasing it.</b> A returned pool buffer can be
/// handed to an unrelated partition, and a released memory-mapped view is no longer mapped at all
/// — reading through it takes the process down. Consumers that need the bytes to outlive the
/// response must copy them.</para>
///
/// <para>Deliberately <b>not</b> <see cref="IDisposable"/>: a response does not own a resource in
/// the usual sense, and declaring it disposable would make every one of the dozens of response
/// constructions across the handlers an ownership question (CA2000) for a rule that concerns
/// exactly one API.</para>
/// </summary>
public abstract class KafkaResponse : IProtocolResponse
{
    // Most responses attach nothing and single-partition fetches attach exactly one lease, so the
    // first one lives in a field and only multi-partition fetches pay for a list.
    private IDisposable? _lifetime;
    private List<IDisposable>? _additionalLifetimes;

    public required int CorrelationId { get; init; }
    public required short ApiVersion { get; init; }

    /// <summary>
    /// Ties <paramref name="lifetime"/> to this response: it is released by
    /// <see cref="ReleaseBorrowedMemory"/>, i.e. once the response has been written to the wire
    /// (#78). Used by the fetch path to hand a storage lease across the stack instead of
    /// materializing its bytes into an array.
    /// </summary>
    public void AttachLifetime(IDisposable lifetime)
    {
        if (_lifetime is null)
        {
            _lifetime = lifetime;
            return;
        }

        (_additionalLifetimes ??= []).Add(lifetime);
    }

    /// <summary>
    /// Releases every attached lease, giving pooled buffers and mapped views back. Idempotent; the
    /// payload memory of a borrowed record set is invalid afterwards.
    /// </summary>
    public void ReleaseBorrowedMemory()
    {
        var first = _lifetime;
        var additional = _additionalLifetimes;
        _lifetime = null;
        _additionalLifetimes = null;

        first?.Dispose();

        if (additional is null)
            return;

        foreach (var lifetime in additional)
            lifetime.Dispose();
    }

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
