using Kuestenlogik.Surgewave.Coordination.ShareGroups;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Handler for share group APIs (KIP-932): ShareGroupHeartbeat, ShareGroupDescribe, ShareFetch,
/// ShareAcknowledge, DescribeShareGroupOffsets, AlterShareGroupOffsets, DeleteShareGroupOffsets.
/// This is the Kafka-DTO &lt;-&gt; neutral ADAPTER for <see cref="IShareGroupCoordinator"/> (#59):
/// it decodes each wire request into a protocol-neutral command, calls the coordinator, and
/// re-encodes the neutral result (wire envelope CorrelationId/ApiVersion/NodeEndpoints, status ->
/// ErrorCode). The fetched record bytes flow through by reference (zero-copy). The coordinator
/// references no Kafka type; this is the piece that moves into the Kafka protocol plugin later.
/// </summary>
public sealed class ShareGroupApiHandler : IKafkaRequestHandler
{
    private readonly IShareGroupCoordinator _coordinator;
    private readonly ILogger<ShareGroupApiHandler> _logger;

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.ShareGroupHeartbeat,
        ApiKey.ShareGroupDescribe,
        ApiKey.ShareFetch,
        ApiKey.ShareAcknowledge,
        ApiKey.DescribeShareGroupOffsets,
        ApiKey.AlterShareGroupOffsets,
        ApiKey.DeleteShareGroupOffsets
    ];

    public ShareGroupApiHandler(
        IShareGroupCoordinator coordinator,
        ILogger<ShareGroupApiHandler> logger)
    {
        _coordinator = coordinator;
        _logger = logger;
    }

    public async Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        return request switch
        {
            ShareGroupHeartbeatRequest r => ToHeartbeatResponse(_coordinator.Heartbeat(ToHeartbeatCommand(r)), r),
            ShareGroupDescribeRequest r => ToDescribeResponse(_coordinator.Describe(r.GroupIds), r),
            ShareFetchRequest r => ToShareFetchResponse(await _coordinator.ShareFetchAsync(ToShareFetchCommand(r), cancellationToken), r),
            ShareAcknowledgeRequest r => ToShareAcknowledgeResponse(_coordinator.ShareAcknowledge(ToShareAcknowledgeCommand(r)), r),
            DescribeShareGroupOffsetsRequest r => ToDescribeOffsetsResponse(_coordinator.DescribeOffsets(ToDescribeOffsetsCommand(r)), r),
            AlterShareGroupOffsetsRequest r => ToAlterOffsetsResponse(_coordinator.AlterOffsets(ToAlterOffsetsCommand(r)), r),
            DeleteShareGroupOffsetsRequest r => ToDeleteOffsetsResponse(_coordinator.DeleteOffsets(ToDeleteOffsetsCommand(r)), r),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by ShareGroupApiHandler")
        };
    }

    private static ErrorCode ToErrorCode(ShareGroupErrorStatus status) => status switch
    {
        ShareGroupErrorStatus.None => ErrorCode.None,
        ShareGroupErrorStatus.InvalidGroupId => ErrorCode.InvalidGroupId,
        ShareGroupErrorStatus.UnknownTopicId => ErrorCode.UnknownTopicId,
        ShareGroupErrorStatus.UnknownTopicOrPartition => ErrorCode.UnknownTopicOrPartition,
        ShareGroupErrorStatus.NonEmptyGroup => ErrorCode.NonEmptyGroup,
        ShareGroupErrorStatus.InvalidRequest => ErrorCode.InvalidRequest,
        ShareGroupErrorStatus.Unknown => ErrorCode.Unknown,
        _ => ErrorCode.Unknown,
    };

    // ── ShareGroupHeartbeat ─────────────────────────────────────────────────

    private static ShareGroupHeartbeatCommand ToHeartbeatCommand(ShareGroupHeartbeatRequest r)
        => new()
        {
            GroupId = r.GroupId,
            MemberId = r.MemberId,
            MemberEpoch = r.MemberEpoch,
            ClientId = r.ClientId,
            RackId = r.RackId,
            SubscribedTopicNames = r.SubscribedTopicNames,
        };

    private static ShareGroupHeartbeatResponse ToHeartbeatResponse(ShareGroupHeartbeatResult result, ShareGroupHeartbeatRequest r)
        => new()
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ErrorCode.None,
            MemberId = result.MemberId,
            MemberEpoch = result.MemberEpoch,
            HeartbeatIntervalMs = result.HeartbeatIntervalMs,
            Assignment = result.Assignment == null ? null : ToHeartbeatAssignment(result.Assignment),
        };

    private static ShareGroupHeartbeatResponse.AssignmentInfo ToHeartbeatAssignment(ShareAssignment assignment)
    {
        var topicPartitions = new List<ShareGroupHeartbeatResponse.TopicPartitions>(assignment.TopicPartitions.Count);
        foreach (var tp in assignment.TopicPartitions)
        {
            topicPartitions.Add(new ShareGroupHeartbeatResponse.TopicPartitions { TopicId = tp.TopicId, Partitions = [.. tp.Partitions] });
        }
        return new ShareGroupHeartbeatResponse.AssignmentInfo { TopicPartitions = topicPartitions };
    }

    // ── ShareGroupDescribe ──────────────────────────────────────────────────

    private static ShareGroupDescribeResponse ToDescribeResponse(IReadOnlyList<ShareGroupDescription> descriptions, ShareGroupDescribeRequest r)
    {
        var groups = new List<ShareGroupDescribeResponse.DescribedGroup>(descriptions.Count);
        foreach (var d in descriptions)
        {
            if (d.Status == ShareGroupErrorStatus.InvalidGroupId)
            {
                groups.Add(new ShareGroupDescribeResponse.DescribedGroup
                {
                    ErrorCode = ErrorCode.InvalidGroupId,
                    GroupId = d.GroupId,
                    GroupState = "",
                    AssignorName = "uniform",
                    Members = []
                });
                continue;
            }

            var members = new List<ShareGroupDescribeResponse.Member>(d.Members.Count);
            foreach (var m in d.Members)
            {
                members.Add(new ShareGroupDescribeResponse.Member
                {
                    MemberId = m.MemberId,
                    RackId = m.RackId,
                    MemberEpoch = m.MemberEpoch,
                    ClientId = m.ClientId,
                    ClientHost = m.ClientHost,
                    SubscribedTopicNames = [.. m.SubscribedTopicNames],
                    Assignment = ToDescribeAssignment(m.Assignment),
                });
            }

            groups.Add(new ShareGroupDescribeResponse.DescribedGroup
            {
                ErrorCode = ErrorCode.None,
                GroupId = d.GroupId,
                GroupState = d.GroupState,
                GroupEpoch = d.GroupEpoch,
                AssignmentEpoch = d.AssignmentEpoch,
                AssignorName = d.AssignorName,
                Members = members,
            });
        }

        return new ShareGroupDescribeResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            Groups = groups,
        };
    }

    private static ShareGroupDescribeResponse.AssignmentInfo ToDescribeAssignment(ShareDescribeAssignment assignment)
    {
        var topicPartitions = new List<ShareGroupDescribeResponse.ShareTopicPartitions>(assignment.TopicPartitions.Count);
        foreach (var tp in assignment.TopicPartitions)
        {
            topicPartitions.Add(new ShareGroupDescribeResponse.ShareTopicPartitions
            {
                TopicId = tp.TopicId,
                TopicName = tp.TopicName,
                Partitions = [.. tp.Partitions],
            });
        }
        return new ShareGroupDescribeResponse.AssignmentInfo { TopicPartitions = topicPartitions };
    }

    // ── ShareFetch ──────────────────────────────────────────────────────────

    private static ShareFetchCommand ToShareFetchCommand(ShareFetchRequest r)
    {
        var topics = new List<ShareFetchTopic>(r.Topics.Count);
        foreach (var t in r.Topics)
        {
            var partitions = new List<ShareFetchPartition>(t.Partitions.Count);
            foreach (var p in t.Partitions)
            {
                partitions.Add(new ShareFetchPartition
                {
                    PartitionIndex = p.PartitionIndex,
                    AcknowledgementBatches = ToAckBatches(p.AcknowledgementBatches),
                });
            }
            topics.Add(new ShareFetchTopic { TopicId = t.TopicId, Partitions = partitions });
        }
        return new ShareFetchCommand
        {
            GroupId = r.GroupId,
            MemberId = r.MemberId,
            MaxRecords = r.MaxRecords,
            Topics = topics,
        };
    }

    private static IReadOnlyList<ShareAcknowledgementBatch> ToAckBatches(List<ShareFetchRequest.AcknowledgementBatch> batches)
    {
        var result = new List<ShareAcknowledgementBatch>(batches.Count);
        foreach (var b in batches)
        {
            result.Add(new ShareAcknowledgementBatch(b.FirstOffset, b.LastOffset, b.AcknowledgeTypes));
        }
        return result;
    }

    private static ShareFetchResponse ToShareFetchResponse(ShareFetchResult result, ShareFetchRequest r)
    {
        var responses = new List<ShareFetchResponse.ShareFetchableTopicResponse>(result.Responses.Count);
        foreach (var t in result.Responses)
        {
            var partitions = new List<ShareFetchResponse.PartitionData>(t.Partitions.Count);
            foreach (var p in t.Partitions)
            {
                var acquired = new List<ShareFetchResponse.AcquiredRecords>(p.AcquiredRecords.Count);
                foreach (var ar in p.AcquiredRecords)
                {
                    acquired.Add(new ShareFetchResponse.AcquiredRecords
                    {
                        FirstOffset = ar.FirstOffset,
                        LastOffset = ar.LastOffset,
                        DeliveryCount = ar.DeliveryCount,
                    });
                }
                partitions.Add(new ShareFetchResponse.PartitionData
                {
                    PartitionIndex = p.PartitionIndex,
                    ErrorCode = ToErrorCode(p.Status),
                    AcknowledgeErrorCode = ToErrorCode(p.AcknowledgeStatus),
                    CurrentLeader = new ShareFetchResponse.LeaderIdAndEpoch { LeaderId = p.CurrentLeader.LeaderId, LeaderEpoch = p.CurrentLeader.LeaderEpoch },
                    Records = p.Records,
                    AcquiredRecords = acquired,
                });
            }
            responses.Add(new ShareFetchResponse.ShareFetchableTopicResponse { TopicId = t.TopicId, Partitions = partitions });
        }

        return new ShareFetchResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ErrorCode.None,
            Responses = responses,
            NodeEndpoints = [],
        };
    }

    // ── ShareAcknowledge ────────────────────────────────────────────────────

    private static ShareAcknowledgeCommand ToShareAcknowledgeCommand(ShareAcknowledgeRequest r)
    {
        var topics = new List<ShareAcknowledgeTopic>(r.Topics.Count);
        foreach (var t in r.Topics)
        {
            var partitions = new List<ShareAcknowledgePartition>(t.Partitions.Count);
            foreach (var p in t.Partitions)
            {
                partitions.Add(new ShareAcknowledgePartition
                {
                    PartitionIndex = p.PartitionIndex,
                    AcknowledgementBatches = ToAckBatches(p.AcknowledgementBatches),
                });
            }
            topics.Add(new ShareAcknowledgeTopic { TopicId = t.TopicId, Partitions = partitions });
        }
        return new ShareAcknowledgeCommand { GroupId = r.GroupId, Topics = topics };
    }

    private static IReadOnlyList<ShareAcknowledgementBatch> ToAckBatches(List<ShareAcknowledgeRequest.AcknowledgementBatch> batches)
    {
        var result = new List<ShareAcknowledgementBatch>(batches.Count);
        foreach (var b in batches)
        {
            result.Add(new ShareAcknowledgementBatch(b.FirstOffset, b.LastOffset, b.AcknowledgeTypes));
        }
        return result;
    }

    private static ShareAcknowledgeResponse ToShareAcknowledgeResponse(ShareAcknowledgeResult result, ShareAcknowledgeRequest r)
    {
        var responses = new List<ShareAcknowledgeResponse.ShareAcknowledgeTopicResponse>(result.Responses.Count);
        foreach (var t in result.Responses)
        {
            var partitions = new List<ShareAcknowledgeResponse.PartitionData>(t.Partitions.Count);
            foreach (var p in t.Partitions)
            {
                partitions.Add(new ShareAcknowledgeResponse.PartitionData
                {
                    PartitionIndex = p.PartitionIndex,
                    ErrorCode = ToErrorCode(p.Status),
                    CurrentLeader = new ShareAcknowledgeResponse.LeaderIdAndEpoch { LeaderId = p.CurrentLeader.LeaderId, LeaderEpoch = p.CurrentLeader.LeaderEpoch },
                });
            }
            responses.Add(new ShareAcknowledgeResponse.ShareAcknowledgeTopicResponse { TopicId = t.TopicId, Partitions = partitions });
        }

        return new ShareAcknowledgeResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ErrorCode.None,
            Responses = responses,
            NodeEndpoints = [],
        };
    }

    // ── DescribeShareGroupOffsets ───────────────────────────────────────────

    private static DescribeShareOffsetsCommand ToDescribeOffsetsCommand(DescribeShareGroupOffsetsRequest r)
    {
        var groups = new List<DescribeShareOffsetsGroup>(r.Groups.Count);
        foreach (var g in r.Groups)
        {
            IReadOnlyList<DescribeShareOffsetsTopic>? topics = null;
            if (g.Topics != null)
            {
                var list = new List<DescribeShareOffsetsTopic>(g.Topics.Count);
                foreach (var t in g.Topics)
                {
                    list.Add(new DescribeShareOffsetsTopic { TopicName = t.TopicName, Partitions = t.Partitions });
                }
                topics = list;
            }
            groups.Add(new DescribeShareOffsetsGroup { GroupId = g.GroupId, Topics = topics });
        }
        return new DescribeShareOffsetsCommand(groups);
    }

    private static DescribeShareGroupOffsetsResponse ToDescribeOffsetsResponse(DescribeShareOffsetsResult result, DescribeShareGroupOffsetsRequest r)
    {
        var groups = new List<DescribeShareGroupOffsetsResponse.DescribeGroupResult>(result.Groups.Count);
        foreach (var g in result.Groups)
        {
            var topics = new List<DescribeShareGroupOffsetsResponse.DescribeTopicResult>(g.Topics.Count);
            foreach (var t in g.Topics)
            {
                var partitions = new List<DescribeShareGroupOffsetsResponse.DescribePartitionResult>(t.Partitions.Count);
                foreach (var p in t.Partitions)
                {
                    partitions.Add(new DescribeShareGroupOffsetsResponse.DescribePartitionResult
                    {
                        PartitionIndex = p.PartitionIndex,
                        StartOffset = p.StartOffset,
                        LeaderEpoch = p.LeaderEpoch,
                        Lag = p.Lag,
                        ErrorCode = ToErrorCode(p.Status),
                    });
                }
                topics.Add(new DescribeShareGroupOffsetsResponse.DescribeTopicResult { TopicName = t.TopicName, TopicId = t.TopicId, Partitions = partitions });
            }
            groups.Add(new DescribeShareGroupOffsetsResponse.DescribeGroupResult
            {
                GroupId = g.GroupId,
                Topics = topics,
                ErrorCode = ToErrorCode(g.Status),
                ErrorMessage = g.ErrorMessage,
            });
        }

        return new DescribeShareGroupOffsetsResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            Groups = groups,
        };
    }

    // ── AlterShareGroupOffsets ──────────────────────────────────────────────

    private static AlterShareOffsetsCommand ToAlterOffsetsCommand(AlterShareGroupOffsetsRequest r)
    {
        var topics = new List<AlterShareOffsetsTopic>(r.Topics.Count);
        foreach (var t in r.Topics)
        {
            var partitions = new List<AlterShareOffsetsPartition>(t.Partitions.Count);
            foreach (var p in t.Partitions)
            {
                partitions.Add(new AlterShareOffsetsPartition(p.PartitionIndex, p.StartOffset));
            }
            topics.Add(new AlterShareOffsetsTopic { TopicName = t.TopicName, Partitions = partitions });
        }
        return new AlterShareOffsetsCommand { GroupId = r.GroupId, Topics = topics };
    }

    private static AlterShareGroupOffsetsResponse ToAlterOffsetsResponse(AlterShareOffsetsResult result, AlterShareGroupOffsetsRequest r)
    {
        var responses = new List<AlterShareGroupOffsetsResponse.AlterTopicResult>(result.Responses.Count);
        foreach (var t in result.Responses)
        {
            var partitions = new List<AlterShareGroupOffsetsResponse.AlterPartitionResult>(t.Partitions.Count);
            foreach (var p in t.Partitions)
            {
                partitions.Add(new AlterShareGroupOffsetsResponse.AlterPartitionResult { PartitionIndex = p.PartitionIndex, ErrorCode = ToErrorCode(p.Status) });
            }
            responses.Add(new AlterShareGroupOffsetsResponse.AlterTopicResult { TopicName = t.TopicName, TopicId = t.TopicId, Partitions = partitions });
        }

        return new AlterShareGroupOffsetsResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ToErrorCode(result.Status),
            ErrorMessage = result.ErrorMessage,
            Responses = responses,
        };
    }

    // ── DeleteShareGroupOffsets ─────────────────────────────────────────────

    private static DeleteShareOffsetsCommand ToDeleteOffsetsCommand(DeleteShareGroupOffsetsRequest r)
    {
        var topics = new List<DeleteShareOffsetsTopic>(r.Topics.Count);
        foreach (var t in r.Topics)
        {
            topics.Add(new DeleteShareOffsetsTopic(t.TopicName));
        }
        return new DeleteShareOffsetsCommand { GroupId = r.GroupId, Topics = topics };
    }

    private static DeleteShareGroupOffsetsResponse ToDeleteOffsetsResponse(DeleteShareOffsetsResult result, DeleteShareGroupOffsetsRequest r)
    {
        var responses = new List<DeleteShareGroupOffsetsResponse.DeleteTopicResult>(result.Responses.Count);
        foreach (var t in result.Responses)
        {
            responses.Add(new DeleteShareGroupOffsetsResponse.DeleteTopicResult { TopicName = t.TopicName, TopicId = t.TopicId, ErrorCode = ToErrorCode(t.Status) });
        }

        return new DeleteShareGroupOffsetsResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ErrorCode = ToErrorCode(result.Status),
            ErrorMessage = result.ErrorMessage,
            Responses = responses,
        };
    }
}
