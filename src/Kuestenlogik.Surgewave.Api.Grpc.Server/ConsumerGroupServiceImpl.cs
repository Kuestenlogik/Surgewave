using Google.Protobuf;
using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;

namespace Kuestenlogik.Surgewave.Api.Grpc.Server;

/// <summary>
/// Result from JoinGroup operation.
/// </summary>
public record JoinGroupResultDto(
    int ErrorCode,
    int GenerationId,
    string ProtocolName,
    string LeaderId,
    string MemberId,
    List<JoinGroupMemberDto> Members);

/// <summary>
/// Member info for JoinGroup result.
/// </summary>
public record JoinGroupMemberDto(string MemberId, string? GroupInstanceId, byte[] Metadata);

/// <summary>
/// Result from SyncGroup operation.
/// </summary>
public record SyncGroupResultDto(int ErrorCode, byte[] Assignment);

/// <summary>
/// Result from Heartbeat operation.
/// </summary>
public record HeartbeatResultDto(int ErrorCode);

/// <summary>
/// Result from LeaveGroup operation.
/// </summary>
public record LeaveGroupResultDto(int ErrorCode);

/// <summary>
/// Group info for ListGroups.
/// </summary>
public record GroupInfoDto(string GroupId, string ProtocolType, string State);

/// <summary>
/// Result from DescribeGroup operation.
/// </summary>
public record DescribeGroupResultDto(
    int ErrorCode,
    string GroupId,
    string State,
    string ProtocolType,
    string ProtocolName,
    int GenerationId,
    List<GroupMemberInfoDto> Members);

/// <summary>
/// Member info for DescribeGroup result.
/// </summary>
public record GroupMemberInfoDto(
    string MemberId,
    string? GroupInstanceId,
    string ClientId,
    byte[] Metadata,
    byte[] Assignment);

/// <summary>
/// Result from DeleteGroup operation.
/// </summary>
public record DeleteGroupResultDto(int ErrorCode);

/// <summary>
/// Result from FindCoordinator operation.
/// </summary>
public record FindCoordinatorResultDto(int ErrorCode, int CoordinatorId, string Host, int Port);

/// <summary>
/// Delegate for JoinGroup operation.
/// </summary>
public delegate JoinGroupResultDto JoinGroupDelegate(
    string groupId,
    string? memberId,
    string? groupInstanceId,
    string clientId,
    string protocolType,
    int sessionTimeoutMs,
    int rebalanceTimeoutMs,
    List<(string Name, byte[] Metadata)> protocols);

/// <summary>
/// Delegate for SyncGroup operation.
/// </summary>
public delegate SyncGroupResultDto SyncGroupDelegate(
    string groupId,
    string memberId,
    int generationId,
    List<(string MemberId, byte[] Assignment)> assignments);

/// <summary>
/// Delegate for Heartbeat operation.
/// </summary>
public delegate HeartbeatResultDto HeartbeatDelegate(string groupId, string memberId, int generationId);

/// <summary>
/// Delegate for LeaveGroup operation.
/// </summary>
public delegate LeaveGroupResultDto LeaveGroupDelegate(string groupId, string memberId);

/// <summary>
/// Delegate for ListGroups operation.
/// </summary>
public delegate List<GroupInfoDto> ListGroupsDelegate();

/// <summary>
/// Delegate for DescribeGroup operation.
/// </summary>
public delegate DescribeGroupResultDto DescribeGroupDelegate(string groupId);

/// <summary>
/// Delegate for DeleteGroup operation.
/// </summary>
public delegate DeleteGroupResultDto DeleteGroupDelegate(string groupId);

/// <summary>
/// Delegate for FindCoordinator operation.
/// </summary>
public delegate FindCoordinatorResultDto FindCoordinatorDelegate(string key, int keyType);

/// <summary>
/// gRPC ConsumerGroupService implementation.
/// </summary>
public class ConsumerGroupServiceImpl : ConsumerGroupService.ConsumerGroupServiceBase
{
    private readonly JoinGroupDelegate _joinGroup;
    private readonly SyncGroupDelegate _syncGroup;
    private readonly HeartbeatDelegate _heartbeat;
    private readonly LeaveGroupDelegate _leaveGroup;
    private readonly ListGroupsDelegate _listGroups;
    private readonly DescribeGroupDelegate _describeGroup;
    private readonly DeleteGroupDelegate _deleteGroup;
    private readonly FindCoordinatorDelegate _findCoordinator;

    public ConsumerGroupServiceImpl(
        JoinGroupDelegate joinGroup,
        SyncGroupDelegate syncGroup,
        HeartbeatDelegate heartbeat,
        LeaveGroupDelegate leaveGroup,
        ListGroupsDelegate listGroups,
        DescribeGroupDelegate describeGroup,
        DeleteGroupDelegate deleteGroup,
        FindCoordinatorDelegate findCoordinator)
    {
        _joinGroup = joinGroup;
        _syncGroup = syncGroup;
        _heartbeat = heartbeat;
        _leaveGroup = leaveGroup;
        _listGroups = listGroups;
        _describeGroup = describeGroup;
        _deleteGroup = deleteGroup;
        _findCoordinator = findCoordinator;
    }

    public override Task<JoinGroupResponse> JoinGroup(JoinGroupRequest request, ServerCallContext context)
    {
        var protocols = request.Protocols
            .Select(p => (p.Name, p.Metadata.ToByteArray()))
            .ToList();

        var result = _joinGroup(
            request.GroupId,
            string.IsNullOrEmpty(request.MemberId) ? null : request.MemberId,
            string.IsNullOrEmpty(request.GroupInstanceId) ? null : request.GroupInstanceId,
            context.Peer ?? "unknown",
            request.ProtocolType,
            request.SessionTimeoutMs,
            request.RebalanceTimeoutMs,
            protocols);

        var response = new JoinGroupResponse
        {
            GenerationId = result.GenerationId,
            ProtocolName = result.ProtocolName,
            Leader = result.LeaderId,
            MemberId = result.MemberId,
            Status = new ResponseStatus { ErrorCode = MapErrorCode(result.ErrorCode) }
        };

        foreach (var member in result.Members)
        {
            response.Members.Add(new GroupMember
            {
                MemberId = member.MemberId,
                GroupInstanceId = member.GroupInstanceId ?? "",
                Metadata = ByteString.CopyFrom(member.Metadata)
            });
        }

        return Task.FromResult(response);
    }

    public override Task<SyncGroupResponse> SyncGroup(SyncGroupRequest request, ServerCallContext context)
    {
        var assignments = request.Assignments
            .Select(a => (a.MemberId, a.Assignment.ToByteArray()))
            .ToList();

        var result = _syncGroup(
            request.GroupId,
            request.MemberId,
            request.GenerationId,
            assignments);

        return Task.FromResult(new SyncGroupResponse
        {
            Assignment = ByteString.CopyFrom(result.Assignment),
            Status = new ResponseStatus { ErrorCode = MapErrorCode(result.ErrorCode) }
        });
    }

    public override Task<HeartbeatResponse> Heartbeat(HeartbeatRequest request, ServerCallContext context)
    {
        var result = _heartbeat(request.GroupId, request.MemberId, request.GenerationId);

        return Task.FromResult(new HeartbeatResponse
        {
            Status = new ResponseStatus { ErrorCode = MapErrorCode(result.ErrorCode) }
        });
    }

    public override Task<LeaveGroupResponse> LeaveGroup(LeaveGroupRequest request, ServerCallContext context)
    {
        var result = _leaveGroup(request.GroupId, request.MemberId);

        return Task.FromResult(new LeaveGroupResponse
        {
            Status = new ResponseStatus { ErrorCode = MapErrorCode(result.ErrorCode) }
        });
    }

    public override Task<ListGroupsResponse> ListGroups(ListGroupsRequest request, ServerCallContext context)
    {
        var groups = _listGroups();

        var response = new ListGroupsResponse
        {
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };

        foreach (var group in groups)
        {
            // Apply state filter if provided
            if (request.StatesFilter.Count > 0 &&
                !request.StatesFilter.Contains(group.State, StringComparer.OrdinalIgnoreCase))
            {
                continue;
            }

            response.Groups.Add(new GroupListing
            {
                GroupId = group.GroupId,
                ProtocolType = group.ProtocolType,
                State = group.State
            });
        }

        return Task.FromResult(response);
    }

    public override Task<DescribeGroupResponse> DescribeGroup(DescribeGroupRequest request, ServerCallContext context)
    {
        var response = new DescribeGroupResponse();

        // Support both single group_id (HTTP path) and multiple group_ids (gRPC)
        IEnumerable<string> groupIds = request.GroupIds.Count > 0
            ? request.GroupIds
            : !string.IsNullOrEmpty(request.GroupId)
                ? [request.GroupId]
                : [];

        foreach (var groupId in groupIds)
        {
            var result = _describeGroup(groupId);

            var description = new GroupDescription
            {
                GroupId = result.GroupId ?? groupId,
                State = result.State ?? "",
                ProtocolType = result.ProtocolType ?? "",
                ProtocolName = result.ProtocolName ?? "",
                GenerationId = result.GenerationId,
                Status = new ResponseStatus { ErrorCode = MapErrorCode(result.ErrorCode) }
            };

            if (result.Members != null)
            {
                foreach (var member in result.Members)
                {
                    description.Members.Add(new MemberDescription
                    {
                        MemberId = member.MemberId,
                        GroupInstanceId = member.GroupInstanceId ?? "",
                        ClientId = member.ClientId,
                        MemberMetadata = ByteString.CopyFrom(member.Metadata),
                        MemberAssignment = ByteString.CopyFrom(member.Assignment)
                    });
                }
            }

            response.Groups.Add(description);
        }

        return Task.FromResult(response);
    }

    public override Task<DeleteGroupResponse> DeleteGroup(DeleteGroupRequest request, ServerCallContext context)
    {
        var response = new DeleteGroupResponse();

        // Support both single group_id (HTTP path) and multiple group_ids (gRPC)
        IEnumerable<string> groupIds = request.GroupIds.Count > 0
            ? request.GroupIds
            : !string.IsNullOrEmpty(request.GroupId)
                ? [request.GroupId]
                : [];

        foreach (var groupId in groupIds)
        {
            var result = _deleteGroup(groupId);

            response.Results.Add(new DeleteGroupResult
            {
                GroupId = groupId,
                Status = new ResponseStatus { ErrorCode = MapErrorCode(result.ErrorCode) }
            });
        }

        return Task.FromResult(response);
    }

    public override Task<FindCoordinatorResponse> FindCoordinator(FindCoordinatorRequest request, ServerCallContext context)
    {
        var result = _findCoordinator(request.Key, (int)request.KeyType);

        return Task.FromResult(new FindCoordinatorResponse
        {
            NodeId = result.CoordinatorId,
            Host = result.Host,
            Port = result.Port,
            Status = new ResponseStatus { ErrorCode = MapErrorCode(result.ErrorCode) }
        });
    }

    private static ErrorCode MapErrorCode(int errorCode) => errorCode switch
    {
        0 => ErrorCode.None,
        10 => ErrorCode.GroupNotFound,
        15 => ErrorCode.UnknownMemberId,
        16 => ErrorCode.IllegalGeneration,
        18 => ErrorCode.GroupNotEmpty,
        25 => ErrorCode.RebalanceInProgress,
        _ => ErrorCode.Unknown
    };
}
