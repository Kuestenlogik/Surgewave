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
    /// <summary>Completes when the join round does: every member rejoined, or the rebalance timeout passed (#216).</summary>
    Task<JoinGroupResult> JoinGroupAsync(JoinGroupCommand request, CancellationToken cancellationToken);

    /// <summary>A follower's completes when the leader's SyncGroup brings the assignment (#216).</summary>
    Task<SyncGroupResult> SyncGroupAsync(SyncGroupCommand request, CancellationToken cancellationToken);

    GroupHeartbeatResult Heartbeat(GroupHeartbeatCommand request);

    LeaveGroupResult LeaveGroup(LeaveGroupCommand request);

    /// <summary>Completes once the offsets are in <c>__consumer_offsets</c> and on its in-sync replicas (#205).</summary>
    Task<OffsetCommitResult> CommitOffsetsAsync(OffsetCommitCommand request, CancellationToken cancellationToken);

    OffsetFetchResult FetchOffsets(OffsetFetchCommand request);

    IReadOnlyList<GroupDescription> DescribeGroups(IReadOnlyList<string> groupIds);

    /// <summary>Lists groups, optionally filtered to the supplied set of states (case-insensitive).</summary>
    IReadOnlyList<GroupListing> ListGroups(IReadOnlyList<string>? statesFilter);

    Task<IReadOnlyList<DeleteGroupResult>> DeleteGroupsAsync(IReadOnlyList<string> groupIds, CancellationToken cancellationToken);

    Task<OffsetDeleteResult> DeleteOffsetsAsync(OffsetDeleteCommand request, CancellationToken cancellationToken);
}
