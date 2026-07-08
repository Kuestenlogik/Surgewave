namespace Kuestenlogik.Surgewave.Coordination.ShareGroups;

/// <summary>
/// Protocol-neutral contract for the share-group coordinator's wire-API surface (KIP-932):
/// membership (Heartbeat/Describe), the data plane (ShareFetch/ShareAcknowledge) and the offset
/// admin (DescribeShareGroupOffsets/AlterShareGroupOffsets/DeleteShareGroupOffsets). The Kafka DTO
/// conversion + wire envelope live in the <c>ShareGroupApiHandler</c> adapter, so the coordinator
/// references no Kafka type (#59). Non-wire helpers (SweepStaleMembers, SetShareGroupConfig) stay
/// concrete and are intentionally not part of this contract.
/// </summary>
public interface IShareGroupCoordinator
{
    ShareGroupHeartbeatResult Heartbeat(ShareGroupHeartbeatCommand request);

    IReadOnlyList<ShareGroupDescription> Describe(IReadOnlyList<string> groupIds);

    Task<ShareFetchResult> ShareFetchAsync(ShareFetchCommand request, CancellationToken cancellationToken);

    ShareAcknowledgeResult ShareAcknowledge(ShareAcknowledgeCommand request);

    DescribeShareOffsetsResult DescribeOffsets(DescribeShareOffsetsCommand request);

    AlterShareOffsetsResult AlterOffsets(AlterShareOffsetsCommand request);

    DeleteShareOffsetsResult DeleteOffsets(DeleteShareOffsetsCommand request);
}
