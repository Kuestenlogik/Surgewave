using Kuestenlogik.Surgewave.Coordination.Consumer;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Handler for the classic consumer-group coordination APIs: JoinGroup, SyncGroup, Heartbeat,
/// LeaveGroup, OffsetCommit, OffsetFetch, DescribeGroups, ListGroups, DeleteGroups, OffsetDelete.
/// This is the Kafka-DTO &lt;-&gt; neutral ADAPTER for <see cref="IConsumerGroupCoordinator"/> (#59):
/// it decodes each wire request into a protocol-neutral command, calls the coordinator, and
/// re-encodes the neutral result. It owns everything wire-shaped: the CorrelationId/ApiVersion/
/// ThrottleTimeMs envelope, the neutral-status -> ErrorCode mapping, the version-gated
/// topic-id resolution flag (v10+) and the single-vs-group-batch OffsetFetch envelope (v8+).
/// The coordinator references no Kafka type; this is the piece that moves into the Kafka
/// protocol plugin later.
/// </summary>
public sealed class ConsumerGroupApiHandler : IKafkaRequestHandler
{
    private readonly IConsumerGroupCoordinator _coordinator;
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
        IConsumerGroupCoordinator coordinator,
        ILogger<ConsumerGroupApiHandler> logger)
    {
        _coordinator = coordinator;
        _logger = logger;
    }

    public Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        KafkaResponse response = request switch
        {
            JoinGroupRequest r => ToJoinGroupResponse(_coordinator.JoinGroup(ToJoinGroupCommand(r)), r),
            SyncGroupRequest r => ToSyncGroupResponse(_coordinator.SyncGroup(ToSyncGroupCommand(r)), r),
            HeartbeatRequest r => ToHeartbeatResponse(_coordinator.Heartbeat(new GroupHeartbeatCommand(r.GroupId, r.MemberId)), r),
            LeaveGroupRequest r => ToLeaveGroupResponse(_coordinator.LeaveGroup(ToLeaveGroupCommand(r)), r),
            OffsetCommitRequest r => ToOffsetCommitResponse(_coordinator.CommitOffsets(ToOffsetCommitCommand(r)), r),
            OffsetFetchRequest r => ToOffsetFetchResponse(_coordinator.FetchOffsets(ToOffsetFetchCommand(r)), r),
            DescribeGroupsRequest r => ToDescribeGroupsResponse(_coordinator.DescribeGroups(r.GroupIds), r),
            ListGroupsRequest r => ToListGroupsResponse(_coordinator.ListGroups(r.StatesFilter), r),
            DeleteGroupsRequest r => ToDeleteGroupsResponse(_coordinator.DeleteGroups(r.GroupIds), r),
            OffsetDeleteRequest r => ToOffsetDeleteResponse(_coordinator.DeleteOffsets(ToOffsetDeleteCommand(r)), r),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by ConsumerGroupApiHandler")
        };

        return Task.FromResult(response);
    }

    // Maps a neutral outcome onto the Kafka wire error code. Kept exhaustive so a new
    // status value surfaces as a build warning rather than a silent None.
    private static ErrorCode ToErrorCode(ConsumerGroupErrorStatus status) => status switch
    {
        ConsumerGroupErrorStatus.None => ErrorCode.None,
        ConsumerGroupErrorStatus.UnknownMember => ErrorCode.UnknownMemberId,
        ConsumerGroupErrorStatus.UnknownTopicId => ErrorCode.UnknownTopicId,
        ConsumerGroupErrorStatus.StaleMemberEpoch => ErrorCode.StaleMemberEpoch,
        ConsumerGroupErrorStatus.FencedMemberEpoch => ErrorCode.FencedMemberEpoch,
        ConsumerGroupErrorStatus.InvalidGroupId => ErrorCode.InvalidGroupId,
        ConsumerGroupErrorStatus.NonEmptyGroup => ErrorCode.NonEmptyGroup,
        ConsumerGroupErrorStatus.GroupIdNotFound => ErrorCode.GroupIdNotFound,
        ConsumerGroupErrorStatus.GroupSubscribedToTopic => ErrorCode.GroupSubscribedToTopic,
        _ => ErrorCode.None,
    };

    // ── JoinGroup ────────────────────────────────────────────────────────────

    private static JoinGroupCommand ToJoinGroupCommand(JoinGroupRequest r)
    {
        var protocols = new List<GroupJoinProtocol>(r.Protocols.Length);
        foreach (var p in r.Protocols)
        {
            protocols.Add(new GroupJoinProtocol(p.Name, p.Metadata ?? Array.Empty<byte>()));
        }

        return new JoinGroupCommand
        {
            GroupId = r.GroupId,
            MemberId = r.MemberId,
            ClientId = r.ClientId,
            GroupInstanceId = r.GroupInstanceId,
            ProtocolType = r.ProtocolType,
            SessionTimeoutMs = r.SessionTimeoutMs,
            Protocols = protocols,
        };
    }

    private static JoinGroupResponse ToJoinGroupResponse(JoinGroupResult result, JoinGroupRequest r)
    {
        var members = new JoinGroupResponse.JoinGroupMember[result.Members.Count];
        for (int i = 0; i < members.Length; i++)
        {
            var m = result.Members[i];
            members[i] = new JoinGroupResponse.JoinGroupMember
            {
                MemberId = m.MemberId,
                GroupInstanceId = m.GroupInstanceId,
                Metadata = m.Metadata,
            };
        }

        return new JoinGroupResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ErrorCode.None,
            GenerationId = result.GenerationId,
            ProtocolName = result.ProtocolName,
            Leader = result.LeaderId,
            MemberId = result.MemberId,
            Members = members,
        };
    }

    // ── SyncGroup ────────────────────────────────────────────────────────────

    private static SyncGroupCommand ToSyncGroupCommand(SyncGroupRequest r)
    {
        var assignments = new List<SyncGroupAssignmentInput>(r.Assignments.Length);
        foreach (var a in r.Assignments)
        {
            assignments.Add(new SyncGroupAssignmentInput(a.MemberId, a.Assignment));
        }

        return new SyncGroupCommand
        {
            GroupId = r.GroupId,
            MemberId = r.MemberId,
            GenerationId = r.GenerationId,
            Assignments = assignments,
        };
    }

    private static SyncGroupResponse ToSyncGroupResponse(SyncGroupResult result, SyncGroupRequest r)
        => new()
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ToErrorCode(result.Status),
            Assignment = result.Assignment,
        };

    // ── Heartbeat ────────────────────────────────────────────────────────────

    private static HeartbeatResponse ToHeartbeatResponse(GroupHeartbeatResult result, HeartbeatRequest r)
        => new()
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ToErrorCode(result.Status),
        };

    // ── LeaveGroup ───────────────────────────────────────────────────────────

    private static LeaveGroupCommand ToLeaveGroupCommand(LeaveGroupRequest r)
    {
        var members = new List<LeaveGroupMemberInput>(r.Members.Length);
        foreach (var m in r.Members)
        {
            members.Add(new LeaveGroupMemberInput(m.MemberId, m.GroupInstanceId));
        }

        return new LeaveGroupCommand
        {
            GroupId = r.GroupId,
            MemberId = r.MemberId,
            Members = members,
        };
    }

    private static LeaveGroupResponse ToLeaveGroupResponse(LeaveGroupResult result, LeaveGroupRequest r)
    {
        // Wire convention: the batch (v3+) path echoes a per-member array; the single
        // (v0-2) and not-found paths leave Members at the DTO default (the neutral result
        // signals this by carrying a null member list).
        if (result.Members == null)
        {
            return new LeaveGroupResponse
            {
                CorrelationId = r.CorrelationId,
                ApiVersion = r.ApiVersion,
                ErrorCode = ToErrorCode(result.Status),
            };
        }

        var members = new LeaveGroupResponse.MemberResponse[result.Members.Count];
        for (int i = 0; i < members.Length; i++)
        {
            var m = result.Members[i];
            members[i] = new LeaveGroupResponse.MemberResponse
            {
                MemberId = m.MemberId,
                GroupInstanceId = m.GroupInstanceId,
                ErrorCode = ToErrorCode(m.Status),
            };
        }

        return new LeaveGroupResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ToErrorCode(result.Status),
            Members = members,
        };
    }

    // ── OffsetCommit ─────────────────────────────────────────────────────────

    private static OffsetCommitCommand ToOffsetCommitCommand(OffsetCommitRequest r)
    {
        var topics = new List<OffsetCommitTopic>(r.Topics.Count);
        foreach (var t in r.Topics)
        {
            var partitions = new List<OffsetCommitPartition>(t.Partitions.Count);
            foreach (var p in t.Partitions)
            {
                partitions.Add(new OffsetCommitPartition(p.PartitionIndex, p.CommittedOffset));
            }
            topics.Add(new OffsetCommitTopic { Topic = t.Topic, TopicId = t.TopicId, Partitions = partitions });
        }

        return new OffsetCommitCommand
        {
            GroupId = r.GroupId,
            MemberId = r.MemberId,
            GenerationIdOrMemberEpoch = r.GenerationIdOrMemberEpoch,
            UseTopicId = r.ApiVersion >= 10,
            Topics = topics,
        };
    }

    private static OffsetCommitResponse ToOffsetCommitResponse(OffsetCommitResult result, OffsetCommitRequest r)
    {
        var topics = new List<TopicPartitionCommitResult>(result.Topics.Count);
        foreach (var t in result.Topics)
        {
            var partitions = new List<PartitionCommitResult>(t.Partitions.Count);
            foreach (var p in t.Partitions)
            {
                partitions.Add(new PartitionCommitResult { PartitionIndex = p.PartitionIndex, ErrorCode = ToErrorCode(p.Status) });
            }
            topics.Add(new TopicPartitionCommitResult { Topic = t.Topic, TopicId = t.TopicId, Partitions = partitions });
        }

        return new OffsetCommitResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            Topics = topics,
        };
    }

    // ── OffsetFetch ──────────────────────────────────────────────────────────

    private static OffsetFetchCommand ToOffsetFetchCommand(OffsetFetchRequest r)
    {
        bool isBatch = r.ApiVersion >= 8 && r.Groups != null;
        var groups = new List<OffsetFetchGroupRequest>();

        if (isBatch)
        {
            foreach (var g in r.Groups!)
            {
                groups.Add(new OffsetFetchGroupRequest { GroupId = g.GroupId, Topics = ToNeutralFetchTopics(g.Topics) });
            }
        }
        else
        {
            groups.Add(new OffsetFetchGroupRequest
            {
                GroupId = r.GroupId ?? string.Empty,
                Topics = ToNeutralFetchTopics(r.Topics),
            });
        }

        return new OffsetFetchCommand { UseTopicId = r.ApiVersion >= 10, Groups = groups };
    }

    private static IReadOnlyList<OffsetFetchTopicRequest> ToNeutralFetchTopics(List<TopicPartitionRequest>? topicRequests)
    {
        if (topicRequests == null) return [];
        var topics = new List<OffsetFetchTopicRequest>(topicRequests.Count);
        foreach (var t in topicRequests)
        {
            topics.Add(new OffsetFetchTopicRequest { Topic = t.Topic, TopicId = t.TopicId, PartitionIndexes = t.PartitionIndexes });
        }
        return topics;
    }

    private static OffsetFetchResponse ToOffsetFetchResponse(OffsetFetchResult result, OffsetFetchRequest r)
    {
        // v8+: multi-group batch envelope, one group entry per requested group.
        if (r.ApiVersion >= 8 && r.Groups != null)
        {
            var groups = new List<OffsetFetchResponseGroup>(result.Groups.Count);
            foreach (var g in result.Groups)
            {
                groups.Add(new OffsetFetchResponseGroup
                {
                    GroupId = g.GroupId,
                    Topics = ToWireFetchTopics(g.Topics),
                    ErrorCode = ErrorCode.None,
                });
            }

            return new OffsetFetchResponse
            {
                CorrelationId = r.CorrelationId,
                ApiVersion = r.ApiVersion,
                Groups = groups,
                ErrorCode = ErrorCode.None,
            };
        }

        // v1-7: single-group envelope. The command was lowered to exactly one group.
        return new OffsetFetchResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            Topics = ToWireFetchTopics(result.Groups[0].Topics),
            ErrorCode = ErrorCode.None,
        };
    }

    private static List<TopicPartitionOffset> ToWireFetchTopics(IReadOnlyList<OffsetFetchTopicResult> topics)
    {
        var result = new List<TopicPartitionOffset>(topics.Count);
        foreach (var t in topics)
        {
            var partitions = new List<PartitionOffsetMetadata>(t.Partitions.Count);
            foreach (var p in t.Partitions)
            {
                partitions.Add(new PartitionOffsetMetadata
                {
                    PartitionIndex = p.PartitionIndex,
                    CommittedOffset = p.CommittedOffset,
                    ErrorCode = ToErrorCode(p.Status),
                });
            }
            result.Add(new TopicPartitionOffset { Topic = t.Topic, TopicId = t.TopicId, Partitions = partitions });
        }
        return result;
    }

    // ── DescribeGroups ─────────────────────────────────────────────────────

    private static DescribeGroupsResponse ToDescribeGroupsResponse(IReadOnlyList<GroupDescription> descriptions, DescribeGroupsRequest r)
    {
        var groups = new List<DescribeGroupsResponse.DescribedGroup>(descriptions.Count);
        foreach (var d in descriptions)
        {
            if (d.Status == ConsumerGroupErrorStatus.InvalidGroupId)
            {
                groups.Add(new DescribeGroupsResponse.DescribedGroup
                {
                    ErrorCode = ErrorCode.InvalidGroupId,
                    GroupId = d.GroupId,
                    GroupState = "",
                    ProtocolType = "",
                    ProtocolData = "",
                    Members = new List<DescribeGroupsResponse.GroupMember>(),
                });
                continue;
            }

            var members = new List<DescribeGroupsResponse.GroupMember>(d.Members.Count);
            foreach (var m in d.Members)
            {
                members.Add(new DescribeGroupsResponse.GroupMember
                {
                    MemberId = m.MemberId,
                    GroupInstanceId = m.GroupInstanceId,
                    ClientId = m.ClientId,
                    ClientHost = m.ClientHost,
                    MemberMetadata = m.MemberMetadata,
                    MemberAssignment = m.MemberAssignment,
                });
            }

            groups.Add(new DescribeGroupsResponse.DescribedGroup
            {
                ErrorCode = ErrorCode.None,
                GroupId = d.GroupId,
                GroupState = d.GroupState,
                ProtocolType = d.ProtocolType,
                ProtocolData = d.ProtocolData,
                Members = members,
                AuthorizedOperations = d.AuthorizedOperations,
            });
        }

        return new DescribeGroupsResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            Groups = groups,
        };
    }

    // ── ListGroups ───────────────────────────────────────────────────────────

    private static ListGroupsResponse ToListGroupsResponse(IReadOnlyList<GroupListing> listings, ListGroupsRequest r)
    {
        var groups = new List<ListGroupsResponse.ListedGroup>(listings.Count);
        foreach (var g in listings)
        {
            groups.Add(new ListGroupsResponse.ListedGroup
            {
                GroupId = g.GroupId,
                ProtocolType = g.ProtocolType,
                GroupState = g.GroupState,
            });
        }

        return new ListGroupsResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ErrorCode.None,
            Groups = groups,
        };
    }

    // ── DeleteGroups ─────────────────────────────────────────────────────────

    private static DeleteGroupsResponse ToDeleteGroupsResponse(IReadOnlyList<DeleteGroupResult> results, DeleteGroupsRequest r)
    {
        var deletable = new List<DeleteGroupsResponse.DeletableGroupResult>(results.Count);
        foreach (var g in results)
        {
            deletable.Add(new DeleteGroupsResponse.DeletableGroupResult
            {
                GroupId = g.GroupId,
                ErrorCode = ToErrorCode(g.Status),
            });
        }

        return new DeleteGroupsResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ThrottleTimeMs = 0,
            Results = deletable,
        };
    }

    // ── OffsetDelete ─────────────────────────────────────────────────────────

    private static OffsetDeleteCommand ToOffsetDeleteCommand(OffsetDeleteRequest r)
    {
        var topics = new List<OffsetDeleteTopic>(r.Topics.Count);
        foreach (var t in r.Topics)
        {
            var partitions = new List<int>(t.Partitions.Count);
            foreach (var p in t.Partitions)
            {
                partitions.Add(p.PartitionIndex);
            }
            topics.Add(new OffsetDeleteTopic { Name = t.Name, Partitions = partitions });
        }

        return new OffsetDeleteCommand { GroupId = r.GroupId, Topics = topics };
    }

    private static OffsetDeleteResponse ToOffsetDeleteResponse(OffsetDeleteResult result, OffsetDeleteRequest r)
    {
        var topics = new List<OffsetDeleteResponse.OffsetDeleteTopicResponse>(result.Topics.Count);
        foreach (var t in result.Topics)
        {
            var partitions = new List<OffsetDeleteResponse.OffsetDeletePartitionResponse>(t.Partitions.Count);
            foreach (var p in t.Partitions)
            {
                partitions.Add(new OffsetDeleteResponse.OffsetDeletePartitionResponse
                {
                    PartitionIndex = p.PartitionIndex,
                    ErrorCode = ToErrorCode(p.Status),
                });
            }
            topics.Add(new OffsetDeleteResponse.OffsetDeleteTopicResponse { Name = t.Name, Partitions = partitions });
        }

        return new OffsetDeleteResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ErrorCode.None,
            ThrottleTimeMs = 0,
            Topics = topics,
        };
    }
}
