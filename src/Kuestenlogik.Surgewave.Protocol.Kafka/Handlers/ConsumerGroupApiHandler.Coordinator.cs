using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

public sealed partial class ConsumerGroupApiHandler
{
    // Everything is local until a cluster sets the locator: a broker on its own coordinates every group.
    private ICoordinatorLocator? _coordinators;

    /// <summary>
    /// Sets who coordinates which group (#204). A group this broker does not coordinate is answered NOT_COORDINATOR,
    /// and the client finds the coordinator again; without it every broker served every group, and the members of
    /// one group could join different brokers' copies of it.
    /// </summary>
    public void SetCoordinatorLocator(ICoordinatorLocator coordinators) => _coordinators = coordinators;

    private bool IsLocal(string groupId) => _coordinators?.IsLocal(CoordinatorKind.Group, groupId) ?? true;

    /// <summary>
    /// The NOT_COORDINATOR answer for a single-group request whose group another broker coordinates, or null when
    /// this broker coordinates it — the error where Kafka puts it for each API.
    /// </summary>
    private static KafkaResponse? NotCoordinatorFor(KafkaRequest request, Func<string, bool> isLocal) => request switch
    {
        JoinGroupRequest r when !isLocal(r.GroupId) => new JoinGroupResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ErrorCode.NotCoordinator,
            GenerationId = -1,
            ProtocolType = r.ProtocolType,
            ProtocolName = string.Empty,
            Leader = string.Empty,
            MemberId = r.MemberId,
            Members = [],
        },
        SyncGroupRequest r when !isLocal(r.GroupId) => new SyncGroupResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ErrorCode.NotCoordinator,
            ProtocolType = r.ProtocolType,
            ProtocolName = r.ProtocolName,
            Assignment = [],
        },
        HeartbeatRequest r when !isLocal(r.GroupId) => new HeartbeatResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ErrorCode.NotCoordinator,
        },
        LeaveGroupRequest r when !isLocal(r.GroupId) => new LeaveGroupResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ErrorCode.NotCoordinator,
        },
        OffsetCommitRequest r when !isLocal(r.GroupId) => new OffsetCommitResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            Topics = [.. r.Topics.Select(t => new TopicPartitionCommitResult
            {
                Topic = t.Topic,
                TopicId = t.TopicId,
                Partitions = [.. t.Partitions.Select(p => new PartitionCommitResult
                {
                    PartitionIndex = p.PartitionIndex,
                    ErrorCode = ErrorCode.NotCoordinator,
                })],
            })],
        },
        OffsetFetchRequest { Groups: null } r when !isLocal(r.GroupId ?? string.Empty) => new OffsetFetchResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            Topics = [],
            ErrorCode = ErrorCode.NotCoordinator,
        },
        OffsetDeleteRequest r when !isLocal(r.GroupId) => new OffsetDeleteResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ErrorCode.NotCoordinator,
            ThrottleTimeMs = 0,
            Topics = [],
        },
        _ => null,
    };

    /// <summary>
    /// DescribeGroups over groups some of which other brokers coordinate: this broker answers its own, NOT_COORDINATOR
    /// for the rest, in the order asked.
    /// </summary>
    private DescribeGroupsResponse DescribeGroupsHere(DescribeGroupsRequest r)
    {
        var local = r.GroupIds.Where(IsLocal).ToList();
        var described = ToDescribeGroupsResponse(_coordinator.DescribeGroups(local), r).Groups
            .ToDictionary(g => g.GroupId, StringComparer.Ordinal);

        return new DescribeGroupsResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            Groups = [.. r.GroupIds.Select(id => described.TryGetValue(id, out var group)
                ? group
                : new DescribeGroupsResponse.DescribedGroup
                {
                    ErrorCode = ErrorCode.NotCoordinator,
                    ErrorMessage = $"This broker does not coordinate group {id}.",
                    GroupId = id,
                    GroupState = string.Empty,
                    ProtocolType = string.Empty,
                    ProtocolData = string.Empty,
                    Members = [],
                })],
        };
    }

    /// <summary>DeleteGroups: this broker deletes its own groups, NOT_COORDINATOR for the rest.</summary>
    private async Task<DeleteGroupsResponse> DeleteGroupsHereAsync(DeleteGroupsRequest r, CancellationToken cancellationToken)
    {
        var local = r.GroupIds.Where(IsLocal).ToList();
        var deleted = ToDeleteGroupsResponse(await _coordinator.DeleteGroupsAsync(local, cancellationToken).ConfigureAwait(false), r).Results
            .ToDictionary(g => g.GroupId, StringComparer.Ordinal);

        return new DeleteGroupsResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ThrottleTimeMs = 0,
            Results = [.. r.GroupIds.Select(id => deleted.TryGetValue(id, out var result)
                ? result
                : new DeleteGroupsResponse.DeletableGroupResult
                {
                    GroupId = id,
                    ErrorCode = ErrorCode.NotCoordinator,
                    ErrorMessage = $"This broker does not coordinate group {id}.",
                })],
        };
    }

    /// <summary>OffsetFetch v8+ over several groups: this broker fetches its own, NOT_COORDINATOR for the rest.</summary>
    private OffsetFetchResponse OffsetFetchGroupsHere(OffsetFetchRequest r)
    {
        var localRequest = new OffsetFetchRequest
        {
            ApiKey = r.ApiKey,
            ApiVersion = r.ApiVersion,
            CorrelationId = r.CorrelationId,
            ClientId = r.ClientId,
            RequireStable = r.RequireStable,
            Groups = [.. r.Groups!.Where(g => IsLocal(g.GroupId))],
        };
        var fetched = (localRequest.Groups.Count > 0
                ? ToOffsetFetchResponse(_coordinator.FetchOffsets(ToOffsetFetchCommand(localRequest)), localRequest).Groups!
                : [])
            .ToDictionary(g => g.GroupId, StringComparer.Ordinal);

        return new OffsetFetchResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ErrorCode.None,
            Groups = [.. r.Groups!.Select(g => fetched.TryGetValue(g.GroupId, out var group)
                ? group
                : new OffsetFetchResponseGroup { GroupId = g.GroupId, Topics = [], ErrorCode = ErrorCode.NotCoordinator })],
        };
    }
}
