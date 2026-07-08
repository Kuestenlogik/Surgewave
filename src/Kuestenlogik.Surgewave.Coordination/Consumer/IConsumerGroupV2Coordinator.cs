namespace Kuestenlogik.Surgewave.Coordination.Consumer;

/// <summary>
/// Protocol-neutral contract for the KIP-848 (next-gen) consumer-group coordinator. Speaks
/// domain command/result records — no Kafka wire DTOs — so the Kafka adapter lives in the
/// protocol plugin while the implementation stays in the broker engine (#59). Besides the
/// heartbeat/describe surface it exposes the two offset-path fence checks the classic
/// consumer-group coordinator calls when routing KIP-848 offset commits.
/// </summary>
public interface IConsumerGroupV2Coordinator
{
    /// <summary>
    /// Processes a member heartbeat (join / steady-state / leave via
    /// <see cref="ConsumerHeartbeatCommand.MemberEpoch"/>) and returns the member's assignment
    /// or a fence <see cref="ConsumerHeartbeatResult.Status"/>.
    /// </summary>
    ConsumerHeartbeatResult Heartbeat(ConsumerHeartbeatCommand command);

    /// <summary>
    /// Describes the requested groups; unknown ids come back with
    /// <see cref="ConsumerGroupDescribeStatus.GroupNotFound"/>.
    /// </summary>
    IReadOnlyList<ConsumerGroupDescription> Describe(IReadOnlyList<string> groupIds);

    /// <summary>
    /// Fences an offset commit/fetch for a KIP-848 member. Returns
    /// <see cref="ConsumerGroupFenceStatus.NotAV2Group"/> (distinct!) when the group is not a
    /// KIP-848 group, so the caller falls through to the classic coordinator.
    /// </summary>
    ConsumerGroupFenceStatus ValidateMemberForOffsetOperation(string groupId, string? memberId, int memberEpoch);

    /// <summary>
    /// Whether the given member currently owns (or is being assigned) the given partition —
    /// the per-partition offset-commit check (KIP-1251).
    /// </summary>
    bool IsPartitionAssignmentValid(string groupId, string? memberId, int memberEpoch, Guid topicId, int partition);

    /// <summary>Evicts members whose heartbeat has expired. Background maintenance; no wire concern.</summary>
    void SweepStaleMembers();
}
