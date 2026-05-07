using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Handlers;

/// <summary>
/// Handler for consumer group coordination APIs: JoinGroup, SyncGroup, Heartbeat, LeaveGroup,
/// OffsetCommit, OffsetFetch, DescribeGroups, ListGroups
/// </summary>
public sealed class ConsumerGroupApiHandler : IKafkaRequestHandler
{
    private readonly ConsumerGroupCoordinator _coordinator;
    private readonly ILogger<ConsumerGroupApiHandler> _logger;

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.JoinGroup,
        ApiKey.SyncGroup,
        ApiKey.Heartbeat,
        ApiKey.LeaveGroup,
        ApiKey.OffsetCommit,
        ApiKey.OffsetFetch,
        ApiKey.DescribeGroups,
        ApiKey.ListGroups,
        ApiKey.DeleteGroups,
        ApiKey.OffsetDelete,
    ];

    public ConsumerGroupApiHandler(
        ConsumerGroupCoordinator coordinator,
        ILogger<ConsumerGroupApiHandler> logger)
    {
        _coordinator = coordinator;
        _logger = logger;
    }

    public Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        KafkaResponse response = request switch
        {
            JoinGroupRequest r => _coordinator.HandleJoinGroup(r),
            SyncGroupRequest r => _coordinator.HandleSyncGroup(r),
            HeartbeatRequest r => _coordinator.HandleHeartbeat(r),
            LeaveGroupRequest r => _coordinator.HandleLeaveGroup(r),
            OffsetCommitRequest r => _coordinator.HandleOffsetCommit(r),
            OffsetFetchRequest r => _coordinator.HandleOffsetFetch(r),
            DescribeGroupsRequest r => _coordinator.HandleDescribeGroups(r),
            ListGroupsRequest r => _coordinator.HandleListGroups(r),
            DeleteGroupsRequest r => _coordinator.HandleDeleteGroups(r),
            OffsetDeleteRequest r => _coordinator.HandleOffsetDelete(r),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by ConsumerGroupApiHandler")
        };

        return Task.FromResult(response);
    }
}
