namespace Kuestenlogik.Surgewave.Coordination.Consumer;

/// <summary>
/// Protocol-neutral outcome for a classic (JoinGroup/SyncGroup rebalance) consumer-group
/// operation. The adapter maps these onto Kafka wire error codes; the coordinator never
/// references a Kafka <c>ErrorCode</c> (#59).
/// </summary>
public enum ConsumerGroupErrorStatus
{
    /// <summary>Operation succeeded.</summary>
    None,

    /// <summary>The member (or its group) is unknown to the coordinator.</summary>
    UnknownMember,

    /// <summary>A supplied topic id could not be resolved to a topic name.</summary>
    UnknownTopicId,

    /// <summary>The member's epoch is older than the current per-partition assignment epoch (KIP-1251).</summary>
    StaleMemberEpoch,

    /// <summary>The member has been fenced by a newer epoch (KIP-848 v2 fall-through).</summary>
    FencedMemberEpoch,

    /// <summary>The requested group id is invalid / not found (Describe surface).</summary>
    InvalidGroupId,

    /// <summary>The group still has active members and therefore cannot be deleted.</summary>
    NonEmptyGroup,

    /// <summary>The group id is unknown (OffsetDelete surface, KIP-496).</summary>
    GroupIdNotFound,

    /// <summary>An active member of the group still subscribes to the topic (OffsetDelete, KIP-496).</summary>
    GroupSubscribedToTopic,
}
