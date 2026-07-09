using Kuestenlogik.Surgewave.Coordination.Consumer;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Handler for consumer group v2 APIs (KIP-848): ConsumerGroupHeartbeat, ConsumerGroupDescribe.
/// This is the Kafka-DTO &lt;-&gt; neutral ADAPTER for <see cref="IConsumerGroupV2Coordinator"/> (#59):
/// it decodes the wire request into a neutral command, calls the protocol-agnostic coordinator,
/// and re-encodes the neutral result (CorrelationId/ApiVersion echo, fence-status -> ErrorCode,
/// phase -> group-state string, the null-means-unchanged empty-assignment convention, the ""/"*"
/// wire defaults). The coordinator no longer references any Kafka type; this is the piece that
/// moves into the Kafka protocol plugin later.
/// </summary>
public sealed class ConsumerGroupV2ApiHandler : IKafkaRequestHandler
{
    private readonly IConsumerGroupV2Coordinator _coordinator;
    private readonly ILogger<ConsumerGroupV2ApiHandler> _logger;

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.ConsumerGroupHeartbeat,
        ApiKey.ConsumerGroupDescribe
    ];

    public ConsumerGroupV2ApiHandler(
        IConsumerGroupV2Coordinator coordinator,
        ILogger<ConsumerGroupV2ApiHandler> logger)
    {
        _coordinator = coordinator;
        _logger = logger;
    }

    public Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        KafkaResponse response = request switch
        {
            ConsumerGroupHeartbeatRequest r => ToHeartbeatResponse(_coordinator.Heartbeat(ToHeartbeatCommand(r)), r),
            ConsumerGroupDescribeRequest r => ToDescribeResponse(_coordinator.Describe(r.GroupIds), r),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by ConsumerGroupV2ApiHandler")
        };

        return Task.FromResult(response);
    }

    // ── Heartbeat: wire -> neutral -> wire ──────────────────────────────────

    private static ConsumerHeartbeatCommand ToHeartbeatCommand(ConsumerGroupHeartbeatRequest r)
    {
        IReadOnlyList<ConsumerTopicPartitions>? owned = null;
        if (r.TopicPartitions != null)
        {
            var list = new List<ConsumerTopicPartitions>(r.TopicPartitions.Count);
            foreach (var a in r.TopicPartitions)
            {
                list.Add(new ConsumerTopicPartitions(a.TopicId, a.Partitions));
            }
            owned = list;
        }

        return new ConsumerHeartbeatCommand
        {
            GroupId = r.GroupId,
            MemberId = r.MemberId,
            MemberEpoch = r.MemberEpoch,
            ClientId = r.ClientId,
            InstanceId = r.InstanceId,
            RackId = r.RackId,
            RebalanceTimeoutMs = r.RebalanceTimeoutMs,
            ServerAssignor = r.ServerAssignor,
            SubscribedTopicNames = r.SubscribedTopicNames,
            OwnedTopicPartitions = owned,
        };
    }

    private static ConsumerGroupHeartbeatResponse ToHeartbeatResponse(ConsumerHeartbeatResult result, ConsumerGroupHeartbeatRequest r)
        => new()
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ToErrorCode(result.Status),
            MemberId = result.MemberId,
            MemberEpoch = result.MemberEpoch,
            HeartbeatIntervalMs = result.HeartbeatIntervalMs,
            MemberAssignment = ToHeartbeatAssignment(result.Assignment),
        };

    // Wire convention: null == "no assignment"; the neutral result uses an empty list.
    private static ConsumerGroupHeartbeatResponse.Assignment? ToHeartbeatAssignment(IReadOnlyList<ConsumerTopicPartitions> assignment)
    {
        if (assignment.Count == 0) return null;
        var topicPartitions = new List<ConsumerGroupHeartbeatResponse.TopicPartitions>(assignment.Count);
        foreach (var t in assignment)
        {
            topicPartitions.Add(new ConsumerGroupHeartbeatResponse.TopicPartitions { TopicId = t.TopicId, Partitions = [.. t.Partitions] });
        }
        return new ConsumerGroupHeartbeatResponse.Assignment { TopicPartitions = topicPartitions };
    }

    private static ErrorCode ToErrorCode(ConsumerGroupFenceStatus status) => status switch
    {
        ConsumerGroupFenceStatus.Ok => ErrorCode.None,
        ConsumerGroupFenceStatus.UnknownMember => ErrorCode.UnknownMemberId,
        ConsumerGroupFenceStatus.StaleEpoch => ErrorCode.StaleMemberEpoch,
        ConsumerGroupFenceStatus.FencedEpoch => ErrorCode.FencedMemberEpoch,
        // NotAV2Group is offset-path only; never reaches the heartbeat response.
        _ => ErrorCode.None,
    };

    // ── Describe: neutral -> wire ───────────────────────────────────────────

    private static ConsumerGroupDescribeResponse ToDescribeResponse(IReadOnlyList<ConsumerGroupDescription> descriptions, ConsumerGroupDescribeRequest r)
    {
        var groups = new List<ConsumerGroupDescribeResponse.DescribedGroup>(descriptions.Count);
        foreach (var d in descriptions)
        {
            if (d.Status == ConsumerGroupDescribeStatus.GroupNotFound)
            {
                groups.Add(new ConsumerGroupDescribeResponse.DescribedGroup
                {
                    ErrorCode = ErrorCode.InvalidGroupId,
                    GroupId = d.GroupId,
                    GroupState = "",
                    AssignorName = "range",
                    Members = [],
                });
                continue;
            }

            var members = new List<ConsumerGroupDescribeResponse.Member>(d.Members.Count);
            foreach (var m in d.Members)
            {
                members.Add(new ConsumerGroupDescribeResponse.Member
                {
                    MemberId = m.MemberId,
                    InstanceId = m.InstanceId,
                    RackId = m.RackId,
                    MemberEpoch = m.MemberEpoch,
                    ClientId = m.ClientId ?? "",
                    ClientHost = m.ClientHost ?? "*",
                    SubscribedTopicNames = [.. m.SubscribedTopicNames],
                    SubscribedTopicRegex = null,
                    MemberAssignment = ToDescribeAssignment(m.MemberAssignment),
                    TargetAssignment = ToDescribeAssignment(m.TargetAssignment),
                });
            }

            groups.Add(new ConsumerGroupDescribeResponse.DescribedGroup
            {
                ErrorCode = ErrorCode.None,
                GroupId = d.GroupId,
                GroupState = ToGroupState(d.Phase),
                GroupEpoch = d.GroupEpoch,
                AssignmentEpoch = d.AssignmentEpoch,
                AssignorName = d.AssignorName,
                Members = members,
            });
        }

        return new ConsumerGroupDescribeResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            Groups = groups,
        };
    }

    private static string ToGroupState(ConsumerGroupPhase phase) => phase switch
    {
        ConsumerGroupPhase.Empty => "Empty",
        ConsumerGroupPhase.Stable => "Stable",
        _ => "Reconciling",
    };

    private static ConsumerGroupDescribeResponse.Assignment ToDescribeAssignment(IReadOnlyList<ConsumerTopicPartitions> assignment)
    {
        var topicPartitions = new List<ConsumerGroupDescribeResponse.TopicPartitions>(assignment.Count);
        foreach (var t in assignment)
        {
            topicPartitions.Add(new ConsumerGroupDescribeResponse.TopicPartitions { TopicId = t.TopicId, Partitions = [.. t.Partitions] });
        }
        return new ConsumerGroupDescribeResponse.Assignment { TopicPartitions = topicPartitions };
    }
}
