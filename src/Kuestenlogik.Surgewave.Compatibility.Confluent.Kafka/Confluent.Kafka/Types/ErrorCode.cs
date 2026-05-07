namespace Confluent.Kafka;

/// <summary>
/// Kafka error codes (matches Confluent.Kafka ErrorCode enum).
/// </summary>
public enum ErrorCode
{
    /// <summary>No error.</summary>
    NoError = 0,

    /// <summary>Unknown error.</summary>
    Unknown = -1,

    /// <summary>Local: No offset stored.</summary>
    Local_NoOffset = -168,

    /// <summary>Local: Timed out.</summary>
    Local_TimedOut = -185,

    /// <summary>Local: Queue full.</summary>
    Local_QueueFull = -184,

    /// <summary>Local: Invalid argument.</summary>
    Local_InvalidArg = -186,

    /// <summary>Local: State error.</summary>
    Local_State = -172,

    /// <summary>Local: Unknown topic.</summary>
    Local_UnknownTopic = -188,

    /// <summary>Local: Unknown partition.</summary>
    Local_UnknownPartition = -190,

    /// <summary>Local: All brokers down.</summary>
    Local_AllBrokersDown = -187,

    /// <summary>Local: Transport error.</summary>
    Local_Transport = -195,

    /// <summary>Local: Fatal error.</summary>
    Local_Fatal = -150,

    /// <summary>Broker: Offset out of range.</summary>
    OffsetOutOfRange = 1,

    /// <summary>Broker: Invalid message.</summary>
    InvalidMessage = 2,

    /// <summary>Broker: Unknown topic or partition.</summary>
    UnknownTopicOrPartition = 3,

    /// <summary>Broker: Invalid message size.</summary>
    InvalidMessageSize = 4,

    /// <summary>Broker: Leader not available.</summary>
    LeaderNotAvailable = 5,

    /// <summary>Broker: Not leader for partition.</summary>
    NotLeaderForPartition = 6,

    /// <summary>Broker: Request timed out.</summary>
    RequestTimedOut = 7,

    /// <summary>Broker: Broker not available.</summary>
    BrokerNotAvailable = 8,

    /// <summary>Broker: Replica not available.</summary>
    ReplicaNotAvailable = 9,

    /// <summary>Broker: Message too large.</summary>
    MessageSizeTooLarge = 10,

    /// <summary>Broker: Topic already exists.</summary>
    TopicAlreadyExists = 36,

    /// <summary>Broker: Invalid partition count.</summary>
    InvalidPartitions = 37,

    /// <summary>Broker: Invalid replication factor.</summary>
    InvalidReplicationFactor = 38,

    /// <summary>Broker: Group coordinator not available.</summary>
    GroupCoordinatorNotAvailable = 15,

    /// <summary>Broker: Not coordinator.</summary>
    NotCoordinator = 16,

    /// <summary>Broker: Illegal generation.</summary>
    IllegalGeneration = 22,

    /// <summary>Broker: Inconsistent group protocol.</summary>
    InconsistentGroupProtocol = 23,

    /// <summary>Broker: Unknown member ID.</summary>
    UnknownMemberId = 25,

    /// <summary>Broker: Rebalance in progress.</summary>
    RebalanceInProgress = 27,

    /// <summary>Broker: Invalid session timeout.</summary>
    InvalidSessionTimeout = 26,

    /// <summary>Broker: Member ID required.</summary>
    MemberIdRequired = 79
}
