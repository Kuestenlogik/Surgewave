namespace Kuestenlogik.Surgewave.Coordination.Streams;

/// <summary>
/// Protocol-neutral contract for the KIP-1071 streams-group coordinator. Speaks
/// domain command/result records — no Kafka wire DTOs — so the Kafka adapter can live
/// in the protocol plugin while the implementation stays in the broker engine (#59).
/// </summary>
public interface IStreamsGroupCoordinator
{
    /// <summary>
    /// Processes a streams-group member heartbeat (join / steady-state / leave, driven by
    /// <see cref="StreamsHeartbeatCommand.MemberEpoch"/>) and returns the member's assignment.
    /// </summary>
    StreamsHeartbeatResult Heartbeat(StreamsHeartbeatCommand command);

    /// <summary>
    /// Describes the requested streams groups; unknown group ids come back with
    /// <see cref="StreamsGroupStatus.GroupNotFound"/>.
    /// </summary>
    IReadOnlyList<StreamsGroupDescription> Describe(IReadOnlyList<string> groupIds);

    /// <summary>
    /// Evicts members whose heartbeat has expired. Background maintenance; no wire concern.
    /// </summary>
    void SweepStaleMembers();
}
