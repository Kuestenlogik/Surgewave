namespace Kuestenlogik.Surgewave.Coordination.Consumer;

/// <summary>
/// Protocol-neutral contract for the classic (rebalance-based) consumer-group coordinator:
/// membership (JoinGroup/SyncGroup/Heartbeat/LeaveGroup), offsets (commit/fetch/delete) and
/// admin (describe/list/delete). The Kafka DTO &lt;-&gt; neutral conversion, the wire envelope
/// (CorrelationId/ApiVersion/ThrottleTimeMs) and the version-shaped encoding (v8 group batch,
/// v10 topic ids) live entirely in the adapter (<c>ConsumerGroupApiHandler</c>), so the
/// coordinator references no Kafka type (#59).
/// </summary>
public interface IConsumerGroupCoordinator
{
    JoinGroupResult JoinGroup(JoinGroupCommand request);

    SyncGroupResult SyncGroup(SyncGroupCommand request);

    GroupHeartbeatResult Heartbeat(GroupHeartbeatCommand request);

    LeaveGroupResult LeaveGroup(LeaveGroupCommand request);

    OffsetCommitResult CommitOffsets(OffsetCommitCommand request);

    OffsetFetchResult FetchOffsets(OffsetFetchCommand request);

    IReadOnlyList<GroupDescription> DescribeGroups(IReadOnlyList<string> groupIds);

    /// <summary>Lists groups, optionally filtered to the supplied set of states (case-insensitive).</summary>
    IReadOnlyList<GroupListing> ListGroups(IReadOnlyList<string>? statesFilter);

    IReadOnlyList<DeleteGroupResult> DeleteGroups(IReadOnlyList<string> groupIds);

    OffsetDeleteResult DeleteOffsets(OffsetDeleteCommand request);
}
