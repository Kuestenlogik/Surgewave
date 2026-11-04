namespace Kuestenlogik.Surgewave.Coordination.ShareGroups;

/// <summary>
/// Protocol-neutral outcome for a share-group (KIP-932) coordinator operation. The adapter maps
/// these onto Kafka wire error codes; the coordinator never references a Kafka <c>ErrorCode</c> (#59).
/// </summary>
public enum ShareGroupErrorStatus
{
    /// <summary>Operation succeeded.</summary>
    None,

    /// <summary>The requested group id is invalid / not found.</summary>
    InvalidGroupId,

    /// <summary>The supplied topic id could not be resolved to a topic name.</summary>
    UnknownTopicId,

    /// <summary>The (topic, partition) has no log on this broker.</summary>
    UnknownTopicOrPartition,

    /// <summary>The group still has active members and therefore cannot be altered/deleted.</summary>
    NonEmptyGroup,

    /// <summary>The request is invalid (e.g. a Renew ack while the group disables renew, KIP-1240).</summary>
    InvalidRequest,

    /// <summary>An unexpected error occurred while serving the request.</summary>
    Unknown,

    /// <summary>Another broker coordinates the group (wire NotCoordinator) — #219.</summary>
    NotCoordinator,

    /// <summary>This broker is still reading the group's partition (wire CoordinatorLoadInProgress) — #219.</summary>
    CoordinatorLoadInProgress,

    /// <summary>The group's change could not be made durable now (wire CoordinatorNotAvailable) — #219.</summary>
    CoordinatorNotAvailable,

    /// <summary>Another broker leads the partition (wire NotLeaderOrFollower) — #228.</summary>
    NotLeaderOrFollower,

    /// <summary>A record acknowledged is not one the member holds (wire InvalidRecordState) — #228.</summary>
    InvalidRecordState,
}
