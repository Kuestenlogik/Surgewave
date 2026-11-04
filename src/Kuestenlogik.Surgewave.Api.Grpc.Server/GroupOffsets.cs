using Kuestenlogik.Surgewave.Coordination.Consumer;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using GrpcTopicPartition = Kuestenlogik.Surgewave.Api.Grpc.TopicPartition;

namespace Kuestenlogik.Surgewave.Api.Grpc.Server;

/// <summary>
/// The gRPC offset commit and fetch, through the group's coordinator (#221) — the same calls, checks and errors as the
/// Kafka path's OffsetCommit and OffsetFetch: a broker that does not coordinate the group answers NOT_COORDINATOR and
/// names the one that does, and the coordinator checks the member and generation. They used to write and read
/// <c>OffsetStore</c> directly, past all of it.
/// </summary>
public sealed class GroupOffsets(IConsumerGroupCoordinator coordinator, ICoordinatorLocator locator)
{
    public async Task<IReadOnlyList<OffsetCommitOutcome>> CommitAsync(
        string groupId, string? memberId, int generationId, IReadOnlyList<OffsetCommit> offsets, CancellationToken cancellationToken)
    {
        if (!locator.IsLocal(CoordinatorKind.Group, groupId))
            return [.. offsets.Select(o => new OffsetCommitOutcome(o.Topic, o.Partition, NotHere(groupId)))];

        var result = await coordinator.CommitOffsetsAsync(new OffsetCommitCommand
        {
            GroupId = groupId,
            MemberId = memberId,
            GenerationIdOrMemberEpoch = generationId,
            UseTopicId = false,
            Topics = [.. offsets.GroupBy(o => o.Topic, StringComparer.Ordinal).Select(t => new OffsetCommitTopic
            {
                Topic = t.Key,
                TopicId = Guid.Empty,
                Partitions = [.. t.Select(o => new OffsetCommitPartition(o.Partition, o.Offset))],
            })],
        }, cancellationToken).ConfigureAwait(false);

        return [.. result.Topics.SelectMany(t => t.Partitions.Select(p => new OffsetCommitOutcome(t.Topic ?? string.Empty, p.PartitionIndex, ErrorOf(p.Status))))];
    }

    public IReadOnlyList<OffsetFetchOutcome> Fetch(string groupId, IReadOnlyList<GrpcTopicPartition> partitions)
    {
        if (!locator.IsLocal(CoordinatorKind.Group, groupId))
            return [.. partitions.Select(p => new OffsetFetchOutcome(p.Topic, p.Partition, -1, NotHere(groupId)))];

        var result = coordinator.FetchOffsets(new OffsetFetchCommand
        {
            UseTopicId = false,
            Groups =
            [
                new OffsetFetchGroupRequest
                {
                    GroupId = groupId,
                    Topics = [.. partitions.GroupBy(p => p.Topic, StringComparer.Ordinal).Select(t => new OffsetFetchTopicRequest
                    {
                        Topic = t.Key,
                        TopicId = Guid.Empty,
                        PartitionIndexes = [.. t.Select(p => p.Partition)],
                    })],
                },
            ],
        });

        var group = result.Groups.Single();
        if (group.Status != ConsumerGroupErrorStatus.None)
            return [.. partitions.Select(p => new OffsetFetchOutcome(p.Topic, p.Partition, -1, ErrorOf(group.Status)))];

        var answered = group.Topics
            .SelectMany(t => t.Partitions.Select(p => (Key: (t.Topic, p.PartitionIndex), p.CommittedOffset, p.Status)))
            .ToDictionary(a => a.Key, a => (a.CommittedOffset, a.Status));
        return [.. partitions.Select(p => answered.TryGetValue((p.Topic, p.Partition), out var a)
            ? new OffsetFetchOutcome(p.Topic, p.Partition, a.CommittedOffset, ErrorOf(a.Status))
            : new OffsetFetchOutcome(p.Topic, p.Partition, -1, ErrorCode.None))];
    }

    /// <summary>The broker that coordinates the group, as FindCoordinator names it.</summary>
    public BrokerInfo? CoordinatorOf(string groupId)
    {
        var location = locator.Locate(CoordinatorKind.Group, groupId);
        return location.Status == CoordinatorLookupStatus.Ok
            ? new BrokerInfo { BrokerId = location.BrokerId, Host = location.Host, Port = location.Port }
            : null;
    }

    // Another broker coordinates the group — or none does right now, which the client retries.
    private ErrorCode NotHere(string groupId) =>
        locator.Locate(CoordinatorKind.Group, groupId).Status == CoordinatorLookupStatus.Ok
            ? ErrorCode.NotCoordinator
            : ErrorCode.CoordinatorNotAvailable;

    private static ErrorCode ErrorOf(ConsumerGroupErrorStatus status) => status switch
    {
        ConsumerGroupErrorStatus.None => ErrorCode.None,
        ConsumerGroupErrorStatus.NotCoordinator => ErrorCode.NotCoordinator,
        ConsumerGroupErrorStatus.CoordinatorLoadInProgress => ErrorCode.CoordinatorLoadInProgress,
        ConsumerGroupErrorStatus.CoordinatorNotAvailable => ErrorCode.CoordinatorNotAvailable,
        ConsumerGroupErrorStatus.UnknownMember or ConsumerGroupErrorStatus.FencedMemberEpoch => ErrorCode.UnknownMemberId,
        ConsumerGroupErrorStatus.IllegalGeneration or ConsumerGroupErrorStatus.StaleMemberEpoch => ErrorCode.IllegalGeneration,
        ConsumerGroupErrorStatus.RebalanceInProgress => ErrorCode.RebalanceInProgress,
        ConsumerGroupErrorStatus.GroupIdNotFound or ConsumerGroupErrorStatus.InvalidGroupId => ErrorCode.GroupNotFound,
        ConsumerGroupErrorStatus.NonEmptyGroup => ErrorCode.GroupNotEmpty,
        ConsumerGroupErrorStatus.UnknownTopicId => ErrorCode.UnknownTopicOrPartition,
        _ => ErrorCode.Unknown,
    };
}
