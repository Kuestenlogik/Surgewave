using Kuestenlogik.Surgewave.Broker.Native.Coordination;
using Kuestenlogik.Surgewave.Broker.Native.Handlers;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

namespace Kuestenlogik.Surgewave.Broker.Native.Operations.ConsumerGroups;

/// <summary>
/// Result for join group operation.
/// </summary>
public readonly record struct JoinGroupResult : IOperationResult
{
    public required JoinGroupResponsePayload Response { get; init; }
    public required SurgewaveErrorCode ErrorCode { get; init; }
}

/// <summary>
/// Operation to join a consumer group.
/// </summary>
public sealed class JoinGroupOperation : IOperationHandler<JoinGroupRequestPayload, JoinGroupResult>
{
    private readonly NativeGroupCoordinator _groupCoordinator;

    public JoinGroupOperation(NativeGroupCoordinator groupCoordinator) => _groupCoordinator = groupCoordinator;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.JoinGroup;

    public JoinGroupRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => JoinGroupRequestPayload.Read(ref reader);

    public void ValidateRequest(in JoinGroupRequestPayload request) { }

    public Task<JoinGroupResult> ExecuteAsync(JoinGroupRequestPayload request, CancellationToken cancellationToken)
    {
        var protocols = new List<Coordination.GroupProtocol>(request.Protocols.Length);
        foreach (var p in request.Protocols)
            protocols.Add(new Coordination.GroupProtocol(p.Name, p.Metadata));

        var result = _groupCoordinator.JoinGroup(
            request.GroupId, request.MemberId, request.GroupInstanceId, request.ClientId,
            request.ProtocolType, request.SessionTimeoutMs, request.RebalanceTimeoutMs, protocols);

        var members = new JoinGroupMemberPayload[result.Members.Count];
        for (int i = 0; i < result.Members.Count; i++)
        {
            members[i] = new JoinGroupMemberPayload
            {
                MemberId = result.Members[i].MemberId,
                GroupInstanceId = result.Members[i].GroupInstanceId,
                Metadata = result.Members[i].Metadata
            };
        }

        var response = new JoinGroupResponsePayload
        {
            ErrorCode = (ushort)result.ErrorCode,
            GenerationId = result.GenerationId,
            ProtocolName = result.ProtocolName,
            LeaderId = result.LeaderId,
            MemberId = result.MemberId,
            Members = members
        };

        return Task.FromResult(new JoinGroupResult
        {
            Response = response,
            ErrorCode = (SurgewaveErrorCode)result.ErrorCode
        });
    }

    public void WriteResponse(IPayloadWriter writer, in JoinGroupResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Result for sync group operation.
/// </summary>
public readonly record struct SyncGroupResult : IOperationResult
{
    public required SyncGroupResponsePayload Response { get; init; }
    public required SurgewaveErrorCode ErrorCode { get; init; }
}

/// <summary>
/// Operation to sync a consumer group.
/// </summary>
public sealed class SyncGroupOperation : IOperationHandler<SyncGroupRequestPayload, SyncGroupResult>
{
    private readonly NativeGroupCoordinator _groupCoordinator;

    public SyncGroupOperation(NativeGroupCoordinator groupCoordinator) => _groupCoordinator = groupCoordinator;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.SyncGroup;

    public SyncGroupRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => SyncGroupRequestPayload.Read(ref reader);

    public void ValidateRequest(in SyncGroupRequestPayload request) { }

    public Task<SyncGroupResult> ExecuteAsync(SyncGroupRequestPayload request, CancellationToken cancellationToken)
    {
        var assignments = new List<MemberAssignment>(request.Assignments.Length);
        foreach (var a in request.Assignments)
            assignments.Add(new MemberAssignment(a.MemberId, a.Assignment));

        var result = _groupCoordinator.SyncGroup(request.GroupId, request.MemberId, request.GenerationId, assignments);

        var response = new SyncGroupResponsePayload
        {
            ErrorCode = (ushort)result.ErrorCode,
            Assignment = result.Assignment
        };

        return Task.FromResult(new SyncGroupResult
        {
            Response = response,
            ErrorCode = (SurgewaveErrorCode)result.ErrorCode
        });
    }

    public void WriteResponse(IPayloadWriter writer, in SyncGroupResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Result for heartbeat operation.
/// </summary>
public readonly record struct HeartbeatResult : IOperationResult
{
    public required HeartbeatResponsePayload Response { get; init; }
    public required SurgewaveErrorCode ErrorCode { get; init; }
}

/// <summary>
/// Operation to send heartbeat.
/// </summary>
public sealed class HeartbeatOperation : IOperationHandler<HeartbeatRequestPayload, HeartbeatResult>
{
    private readonly NativeGroupCoordinator _groupCoordinator;

    public HeartbeatOperation(NativeGroupCoordinator groupCoordinator) => _groupCoordinator = groupCoordinator;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.Heartbeat;

    public HeartbeatRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => HeartbeatRequestPayload.Read(ref reader);

    public void ValidateRequest(in HeartbeatRequestPayload request) { }

    public Task<HeartbeatResult> ExecuteAsync(HeartbeatRequestPayload request, CancellationToken cancellationToken)
    {
        var result = _groupCoordinator.Heartbeat(request.GroupId, request.MemberId, request.GenerationId);

        var response = new HeartbeatResponsePayload { ErrorCode = (ushort)result.ErrorCode };

        return Task.FromResult(new HeartbeatResult
        {
            Response = response,
            ErrorCode = (SurgewaveErrorCode)result.ErrorCode
        });
    }

    public void WriteResponse(IPayloadWriter writer, in HeartbeatResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Result for leave group operation.
/// </summary>
public readonly record struct LeaveGroupResult : IOperationResult
{
    public required LeaveGroupResponsePayload Response { get; init; }
    public required SurgewaveErrorCode ErrorCode { get; init; }
}

/// <summary>
/// Operation to leave a consumer group.
/// </summary>
public sealed class LeaveGroupOperation : IOperationHandler<LeaveGroupRequestPayload, LeaveGroupResult>
{
    private readonly NativeGroupCoordinator _groupCoordinator;

    public LeaveGroupOperation(NativeGroupCoordinator groupCoordinator) => _groupCoordinator = groupCoordinator;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.LeaveGroup;

    public LeaveGroupRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => LeaveGroupRequestPayload.Read(ref reader);

    public void ValidateRequest(in LeaveGroupRequestPayload request) { }

    public Task<LeaveGroupResult> ExecuteAsync(LeaveGroupRequestPayload request, CancellationToken cancellationToken)
    {
        var result = _groupCoordinator.LeaveGroup(request.GroupId, request.MemberId);

        var response = new LeaveGroupResponsePayload { ErrorCode = (ushort)result.ErrorCode };

        return Task.FromResult(new LeaveGroupResult
        {
            Response = response,
            ErrorCode = (SurgewaveErrorCode)result.ErrorCode
        });
    }

    public void WriteResponse(IPayloadWriter writer, in LeaveGroupResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Result for list groups operation.
/// </summary>
public readonly record struct ListGroupsResult
{
    public required ListGroupsResponsePayload Response { get; init; }
}

/// <summary>
/// Operation to list consumer groups.
/// </summary>
public sealed class ListGroupsOperation : INoRequestOperationHandler<ListGroupsResult>
{
    private readonly NativeGroupCoordinator _groupCoordinator;

    public ListGroupsOperation(NativeGroupCoordinator groupCoordinator) => _groupCoordinator = groupCoordinator;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.ListGroups;

    public Task<ListGroupsResult> ExecuteAsync(CancellationToken cancellationToken)
    {
        var groups = _groupCoordinator.ListGroups();

        var groupInfos = new GroupInfoPayload[groups.Count];
        for (int i = 0; i < groups.Count; i++)
        {
            groupInfos[i] = new GroupInfoPayload
            {
                GroupId = groups[i].GroupId,
                ProtocolType = groups[i].ProtocolType,
                State = groups[i].State
            };
        }

        var response = new ListGroupsResponsePayload
        {
            ErrorCode = 0,
            Groups = groupInfos
        };

        return Task.FromResult(new ListGroupsResult { Response = response });
    }

    public void WriteResponse(IPayloadWriter writer, in ListGroupsResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Result for describe group operation.
/// </summary>
public readonly record struct DescribeGroupResult : IOperationResult
{
    public required DescribeGroupResponsePayload Response { get; init; }
    public required SurgewaveErrorCode ErrorCode { get; init; }
}

/// <summary>
/// Operation to describe a consumer group.
/// </summary>
public sealed class DescribeGroupOperation : IOperationHandler<DescribeGroupRequestPayload, DescribeGroupResult>
{
    private readonly NativeGroupCoordinator _groupCoordinator;

    public DescribeGroupOperation(NativeGroupCoordinator groupCoordinator) => _groupCoordinator = groupCoordinator;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.DescribeGroup;

    public DescribeGroupRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => DescribeGroupRequestPayload.Read(ref reader);

    public void ValidateRequest(in DescribeGroupRequestPayload request) { }

    public Task<DescribeGroupResult> ExecuteAsync(DescribeGroupRequestPayload request, CancellationToken cancellationToken)
    {
        var result = _groupCoordinator.DescribeGroup(request.GroupId);

        var members = new GroupMemberPayload[result.Members.Count];
        for (int i = 0; i < result.Members.Count; i++)
        {
            members[i] = new GroupMemberPayload
            {
                MemberId = result.Members[i].MemberId,
                GroupInstanceId = result.Members[i].GroupInstanceId,
                ClientId = result.Members[i].ClientId,
                Metadata = result.Members[i].Metadata,
                Assignment = result.Members[i].Assignment
            };
        }

        var response = new DescribeGroupResponsePayload
        {
            ErrorCode = (ushort)result.ErrorCode,
            GroupId = result.GroupId,
            State = result.State,
            ProtocolType = result.ProtocolType,
            ProtocolName = result.ProtocolName,
            GenerationId = result.GenerationId,
            Members = members
        };

        return Task.FromResult(new DescribeGroupResult
        {
            Response = response,
            ErrorCode = (SurgewaveErrorCode)result.ErrorCode
        });
    }

    public void WriteResponse(IPayloadWriter writer, in DescribeGroupResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Result for delete group operation.
/// </summary>
public readonly record struct DeleteGroupResult : IOperationResult
{
    public required DeleteGroupResponsePayload Response { get; init; }
    public required SurgewaveErrorCode ErrorCode { get; init; }
}

/// <summary>
/// Operation to delete a consumer group.
/// </summary>
public sealed class DeleteGroupOperation : IOperationHandler<DeleteGroupRequestPayload, DeleteGroupResult>
{
    private readonly NativeGroupCoordinator _groupCoordinator;

    public DeleteGroupOperation(NativeGroupCoordinator groupCoordinator) => _groupCoordinator = groupCoordinator;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.DeleteGroup;

    public DeleteGroupRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => DeleteGroupRequestPayload.Read(ref reader);

    public void ValidateRequest(in DeleteGroupRequestPayload request) { }

    public Task<DeleteGroupResult> ExecuteAsync(DeleteGroupRequestPayload request, CancellationToken cancellationToken)
    {
        var result = _groupCoordinator.DeleteGroup(request.GroupId);

        var response = new DeleteGroupResponsePayload { ErrorCode = (ushort)result.ErrorCode };

        return Task.FromResult(new DeleteGroupResult
        {
            Response = response,
            ErrorCode = (SurgewaveErrorCode)result.ErrorCode
        });
    }

    public void WriteResponse(IPayloadWriter writer, in DeleteGroupResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Result for find coordinator operation.
/// </summary>
public readonly record struct FindCoordinatorResult : IOperationResult
{
    public required FindCoordinatorResponsePayload Response { get; init; }
    public required SurgewaveErrorCode ErrorCode { get; init; }
}

/// <summary>
/// Operation to find coordinator.
/// </summary>
public sealed class FindCoordinatorOperation : IOperationHandler<FindCoordinatorRequestPayload, FindCoordinatorResult>
{
    private readonly NativeGroupCoordinator _groupCoordinator;
    private readonly NativeRequestContext _context;

    public FindCoordinatorOperation(NativeGroupCoordinator groupCoordinator, NativeRequestContext context)
    {
        _groupCoordinator = groupCoordinator;
        _context = context;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.FindCoordinator;

    public FindCoordinatorRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => FindCoordinatorRequestPayload.Read(ref reader);

    public void ValidateRequest(in FindCoordinatorRequestPayload request) { }

    public Task<FindCoordinatorResult> ExecuteAsync(FindCoordinatorRequestPayload request, CancellationToken cancellationToken)
    {
        var result = _groupCoordinator.FindCoordinator(request.Key, request.KeyType);

        var response = new FindCoordinatorResponsePayload
        {
            ErrorCode = (ushort)result.ErrorCode,
            CoordinatorId = result.CoordinatorId,
            Host = _context.Config.Host,
            Port = _context.Config.Port
        };

        return Task.FromResult(new FindCoordinatorResult
        {
            Response = response,
            ErrorCode = (SurgewaveErrorCode)result.ErrorCode
        });
    }

    public void WriteResponse(IPayloadWriter writer, in FindCoordinatorResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Result for get group lag operation.
/// </summary>
public readonly record struct GetGroupLagResult : IOperationResult
{
    public required GetGroupLagResponsePayload Response { get; init; }
    public required SurgewaveErrorCode ErrorCode { get; init; }
}

/// <summary>
/// Operation to get the lag of a single consumer group.
/// </summary>
public sealed class GetGroupLagOperation : IOperationHandler<GetGroupLagRequestPayload, GetGroupLagResult>
{
    private readonly Core.Monitoring.ILagCalculator _lagCalculator;

    public GetGroupLagOperation(Core.Monitoring.ILagCalculator lagCalculator) => _lagCalculator = lagCalculator;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetGroupLag;

    public GetGroupLagRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => GetGroupLagRequestPayload.Read(ref reader);

    public void ValidateRequest(in GetGroupLagRequestPayload request) { }

    public Task<GetGroupLagResult> ExecuteAsync(GetGroupLagRequestPayload request, CancellationToken cancellationToken)
    {
        var info = _lagCalculator.GetGroupLag(request.GroupId);
        if (info is null)
        {
            return Task.FromResult(new GetGroupLagResult
            {
                Response = new GetGroupLagResponsePayload
                {
                    ErrorCode = (ushort)SurgewaveErrorCode.GroupNotFound,
                    GroupId = request.GroupId,
                    State = "",
                    Topics = []
                },
                ErrorCode = SurgewaveErrorCode.GroupNotFound
            });
        }

        var topics = new TopicLagPayload[info.Topics.Count];
        for (int i = 0; i < info.Topics.Count; i++)
        {
            var topic = info.Topics[i];
            var partitions = new PartitionLagPayload[topic.Partitions.Count];
            for (int j = 0; j < topic.Partitions.Count; j++)
            {
                var p = topic.Partitions[j];
                partitions[j] = new PartitionLagPayload
                {
                    Partition = p.Partition,
                    CommittedOffset = p.CommittedOffset,
                    HighWatermark = p.HighWatermark,
                    Lag = p.Lag,
                    LogStartOffset = p.LogStartOffset
                };
            }

            topics[i] = new TopicLagPayload
            {
                Topic = topic.Topic,
                TotalLag = topic.TotalLag,
                Partitions = partitions
            };
        }

        var response = new GetGroupLagResponsePayload
        {
            ErrorCode = 0,
            GroupId = info.GroupId,
            State = info.State,
            TotalLag = info.TotalLag,
            PartitionCount = info.PartitionCount,
            MemberCount = info.MemberCount,
            Topics = topics
        };

        return Task.FromResult(new GetGroupLagResult
        {
            Response = response,
            ErrorCode = SurgewaveErrorCode.None
        });
    }

    public void WriteResponse(IPayloadWriter writer, in GetGroupLagResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Result for get lag summary operation.
/// </summary>
public readonly record struct GetLagSummaryResult
{
    public required GetLagSummaryResponsePayload Response { get; init; }
}

/// <summary>
/// Operation to get the lag summary across all consumer groups.
/// </summary>
public sealed class GetLagSummaryOperation : INoRequestOperationHandler<GetLagSummaryResult>
{
    private readonly Core.Monitoring.ILagCalculator _lagCalculator;

    public GetLagSummaryOperation(Core.Monitoring.ILagCalculator lagCalculator) => _lagCalculator = lagCalculator;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetLagSummary;

    public Task<GetLagSummaryResult> ExecuteAsync(CancellationToken cancellationToken)
    {
        var summary = _lagCalculator.GetLagSummary();

        var groups = new LagSummaryGroupPayload[summary.Groups.Count];
        for (int i = 0; i < summary.Groups.Count; i++)
        {
            var g = summary.Groups[i];
            groups[i] = new LagSummaryGroupPayload
            {
                GroupId = g.GroupId,
                State = g.State,
                TotalLag = g.TotalLag,
                PartitionCount = g.PartitionCount,
                MemberCount = g.MemberCount
            };
        }

        var response = new GetLagSummaryResponsePayload
        {
            ErrorCode = 0,
            GroupCount = summary.GroupCount,
            GroupsWithHighLag = summary.GroupsWithHighLag,
            TotalLag = summary.TotalLag,
            MaxLag = summary.MaxLag,
            MaxLagGroup = summary.MaxLagGroup,
            Groups = groups
        };

        return Task.FromResult(new GetLagSummaryResult { Response = response });
    }

    public void WriteResponse(IPayloadWriter writer, in GetLagSummaryResult response)
        => response.Response.WriteTo(writer);
}
