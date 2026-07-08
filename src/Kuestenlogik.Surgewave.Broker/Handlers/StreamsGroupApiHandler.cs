using Kuestenlogik.Surgewave.Coordination.Streams;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Handlers;

/// <summary>
/// Handler for streams group APIs (KIP-1071): StreamsGroupHeartbeat, StreamsGroupDescribe.
/// This is the Kafka-DTO &lt;-&gt; neutral ADAPTER for <see cref="IStreamsGroupCoordinator"/> (#59):
/// it decodes the wire request into a neutral command, calls the protocol-agnostic coordinator,
/// and re-encodes the neutral result into the wire response (echoing CorrelationId/ApiVersion,
/// mapping status to ErrorCode, and applying the wire's null/default conventions). The coordinator
/// itself no longer references any Kafka type, so this class is the only piece that moves into the
/// Kafka protocol plugin later.
/// </summary>
public sealed class StreamsGroupApiHandler : IKafkaRequestHandler
{
    private readonly IStreamsGroupCoordinator _coordinator;
    private readonly ILogger<StreamsGroupApiHandler> _logger;

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.StreamsGroupHeartbeat,
        ApiKey.StreamsGroupDescribe
    ];

    public StreamsGroupApiHandler(
        IStreamsGroupCoordinator coordinator,
        ILogger<StreamsGroupApiHandler> logger)
    {
        _coordinator = coordinator;
        _logger = logger;
    }

    public Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        KafkaResponse response = request switch
        {
            StreamsGroupHeartbeatRequest r => ToHeartbeatResponse(_coordinator.Heartbeat(ToHeartbeatCommand(r)), r),
            StreamsGroupDescribeRequest r => ToDescribeResponse(_coordinator.Describe(r.GroupIds), r),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by StreamsGroupApiHandler")
        };

        return Task.FromResult(response);
    }

    // ── Heartbeat: wire -> neutral -> wire ──────────────────────────────────

    private static StreamsHeartbeatCommand ToHeartbeatCommand(StreamsGroupHeartbeatRequest r)
    {
        StreamsTopology? topology = null;
        if (r.Topology != null)
        {
            var subs = new List<StreamsSubtopology>(r.Topology.Subtopologies.Count);
            foreach (var sub in r.Topology.Subtopologies)
            {
                subs.Add(new StreamsSubtopology(sub.SubtopologyId, sub.SourceTopics));
            }
            topology = new StreamsTopology(r.Topology.Epoch, subs);
        }

        return new StreamsHeartbeatCommand
        {
            GroupId = r.GroupId,
            MemberId = r.MemberId,
            MemberEpoch = r.MemberEpoch,
            ClientId = r.ClientId,
            InstanceId = r.InstanceId,
            RackId = r.RackId,
            ProcessId = r.ProcessId,
            Topology = topology,
        };
    }

    private static StreamsGroupHeartbeatResponse ToHeartbeatResponse(StreamsHeartbeatResult result, StreamsGroupHeartbeatRequest r)
        => new()
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ErrorCode.None,
            MemberId = result.MemberId,
            MemberEpoch = result.MemberEpoch,
            HeartbeatIntervalMs = result.HeartbeatIntervalMs,
            AcceptableRecoveryLag = result.AcceptableRecoveryLag,
            TaskOffsetIntervalMs = result.TaskOffsetIntervalMs,
            ActiveTasks = ToHeartbeatTaskIds(result.ActiveTasks),
            StandbyTasks = ToHeartbeatTaskIds(result.StandbyTasks),
            WarmupTasks = ToHeartbeatTaskIds(result.WarmupTasks),
        };

    // The wire convention is null == "unchanged"; the neutral result uses an empty list, so map back.
    private static List<StreamsGroupHeartbeatResponse.TaskIds>? ToHeartbeatTaskIds(IReadOnlyList<StreamsTaskAssignment> tasks)
    {
        if (tasks.Count == 0) return null;
        var result = new List<StreamsGroupHeartbeatResponse.TaskIds>(tasks.Count);
        foreach (var t in tasks)
        {
            result.Add(new StreamsGroupHeartbeatResponse.TaskIds { SubtopologyId = t.SubtopologyId, Partitions = new List<int>(t.Partitions) });
        }
        return result;
    }

    // ── Describe: neutral -> wire ───────────────────────────────────────────

    private static StreamsGroupDescribeResponse ToDescribeResponse(IReadOnlyList<StreamsGroupDescription> descriptions, StreamsGroupDescribeRequest r)
    {
        var groups = new List<StreamsGroupDescribeResponse.DescribedGroup>(descriptions.Count);
        foreach (var d in descriptions)
        {
            if (d.Status == StreamsGroupStatus.GroupNotFound)
            {
                groups.Add(new StreamsGroupDescribeResponse.DescribedGroup
                {
                    ErrorCode = ErrorCode.InvalidGroupId,
                    GroupId = d.GroupId,
                    GroupState = "",
                    Members = [],
                });
                continue;
            }

            var members = new List<StreamsGroupDescribeResponse.Member>(d.Members.Count);
            foreach (var m in d.Members)
            {
                members.Add(new StreamsGroupDescribeResponse.Member
                {
                    MemberId = m.MemberId,
                    MemberEpoch = m.MemberEpoch,
                    InstanceId = m.InstanceId,
                    RackId = m.RackId,
                    ClientId = m.ClientId ?? "",
                    ClientHost = m.ClientHost ?? "*",
                    TopologyEpoch = m.TopologyEpoch,
                    ProcessId = m.ProcessId ?? "",
                    ClientTags = [],
                    TaskOffsets = [],
                    TaskEndOffsets = [],
                    Assignment = ToDescribeAssignment(m),
                    TargetAssignment = ToDescribeAssignment(m),
                });
            }

            StreamsGroupDescribeResponse.TopologyInfo? topology = null;
            if (d.Topology != null)
            {
                var subs = new List<StreamsGroupDescribeResponse.SubtopologyInfo>(d.Topology.Subtopologies.Count);
                foreach (var sub in d.Topology.Subtopologies)
                {
                    subs.Add(new StreamsGroupDescribeResponse.SubtopologyInfo
                    {
                        SubtopologyId = sub.SubtopologyId,
                        SourceTopics = new List<string>(sub.SourceTopics),
                        RepartitionSinkTopics = [],
                        StateChangelogTopics = [],
                        RepartitionSourceTopics = [],
                    });
                }
                topology = new StreamsGroupDescribeResponse.TopologyInfo { Epoch = d.Topology.Epoch, Subtopologies = subs };
            }

            groups.Add(new StreamsGroupDescribeResponse.DescribedGroup
            {
                ErrorCode = ErrorCode.None,
                GroupId = d.GroupId,
                GroupState = d.Phase == StreamsGroupPhase.Empty ? "Empty" : "Stable",
                GroupEpoch = d.GroupEpoch,
                AssignmentEpoch = d.AssignmentEpoch,
                Topology = topology,
                Members = members,
            });
        }

        return new StreamsGroupDescribeResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            Groups = groups,
        };
    }

    private static StreamsGroupDescribeResponse.AssignmentInfo ToDescribeAssignment(StreamsGroupMemberDescription m)
        => new()
        {
            ActiveTasks = ToDescribeTaskIds(m.ActiveTasks),
            StandbyTasks = ToDescribeTaskIds(m.StandbyTasks),
            WarmupTasks = ToDescribeTaskIds(m.WarmupTasks),
        };

    private static List<StreamsGroupDescribeResponse.TaskIds> ToDescribeTaskIds(IReadOnlyList<StreamsTaskAssignment> tasks)
    {
        var result = new List<StreamsGroupDescribeResponse.TaskIds>(tasks.Count);
        foreach (var t in tasks)
        {
            result.Add(new StreamsGroupDescribeResponse.TaskIds { SubtopologyId = t.SubtopologyId, Partitions = new List<int>(t.Partitions) });
        }
        return result;
    }
}
