using Google.Protobuf;
using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;

namespace Kuestenlogik.Surgewave.Gateway.Services;

/// <summary>
/// gRPC service implementation for consumer group operations.
/// Uses Surgewave native client to communicate with the broker.
/// </summary>
public sealed class ConsumerGroupGatewayService : ConsumerGroupService.ConsumerGroupServiceBase
{
    private readonly ClusterRegistry _registry;
    private readonly ILogger<ConsumerGroupGatewayService> _logger;

    public ConsumerGroupGatewayService(
        ClusterRegistry registry,
        ILogger<ConsumerGroupGatewayService> logger)
    {
        _registry = registry;
        _logger = logger;
    }

    public override async Task<JoinGroupResponse> JoinGroup(JoinGroupRequest request, ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);
            var protocols = request.Protocols
                .Select(p => (p.Name, p.Metadata.ToByteArray()))
                .ToList();

            var result = await client.Groups.JoinAsync(
                request.GroupId,
                string.IsNullOrEmpty(request.MemberId) ? null : request.MemberId,
                "gateway-client",
                request.ProtocolType,
                request.SessionTimeoutMs,
                request.RebalanceTimeoutMs,
                protocols,
                context.CancellationToken);

            var response = new JoinGroupResponse
            {
                GenerationId = result.GenerationId,
                ProtocolName = result.ProtocolName,
                Leader = result.LeaderId,
                MemberId = result.MemberId,
                Status = new ResponseStatus
                {
                    ErrorCode = (ErrorCode)result.ErrorCode
                }
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

            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to join group {GroupId}", request.GroupId);
            return new JoinGroupResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<SyncGroupResponse> SyncGroup(SyncGroupRequest request, ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);
            var assignments = request.Assignments
                .Select(a => (a.MemberId, a.Assignment.ToByteArray()))
                .ToList();

            var result = await client.Groups.SyncAsync(
                request.GroupId,
                request.MemberId,
                request.GenerationId,
                assignments,
                context.CancellationToken);

            return new SyncGroupResponse
            {
                Assignment = ByteString.CopyFrom(result.Assignment),
                Status = new ResponseStatus
                {
                    ErrorCode = (ErrorCode)result.ErrorCode
                }
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to sync group {GroupId}", request.GroupId);
            return new SyncGroupResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<LeaveGroupResponse> LeaveGroup(LeaveGroupRequest request, ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);
            await client.Groups.LeaveAsync(request.GroupId, request.MemberId, context.CancellationToken);

            return new LeaveGroupResponse
            {
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to leave group {GroupId}", request.GroupId);
            return new LeaveGroupResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<HeartbeatResponse> Heartbeat(HeartbeatRequest request, ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);
            var errorCode = await client.Groups.HeartbeatAsync(
                request.GroupId,
                request.MemberId,
                request.GenerationId,
                context.CancellationToken);

            return new HeartbeatResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = (ErrorCode)errorCode
                }
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to heartbeat group {GroupId}", request.GroupId);
            return new HeartbeatResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<ListGroupsResponse> ListGroups(ListGroupsRequest request, ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);
            var groups = await client.Groups.ListAsync(context.CancellationToken);

            var response = new ListGroupsResponse
            {
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };

            foreach (var group in groups)
            {
                // Apply state filter if specified
                if (request.StatesFilter.Count > 0 &&
                    !request.StatesFilter.Contains(group.State, StringComparer.OrdinalIgnoreCase))
                    continue;

                response.Groups.Add(new GroupListing
                {
                    GroupId = group.GroupId,
                    ProtocolType = group.ProtocolType,
                    State = group.State
                });
            }

            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to list groups");
            return new ListGroupsResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<DescribeGroupResponse> DescribeGroup(DescribeGroupRequest request, ServerCallContext context)
    {
        var client = _registry.GetClient(request.ClusterId);
        var response = new DescribeGroupResponse();

        // Collect group IDs from both single group_id and group_ids list
        var groupIds = new List<string>();
        if (!string.IsNullOrEmpty(request.GroupId))
            groupIds.Add(request.GroupId);
        groupIds.AddRange(request.GroupIds);

        foreach (var groupId in groupIds)
        {
            try
            {
                var description = await client.Groups.DescribeAsync(groupId, context.CancellationToken);

                var groupDesc = new GroupDescription
                {
                    GroupId = description.GroupId,
                    State = description.State,
                    ProtocolType = description.ProtocolType,
                    ProtocolName = description.ProtocolName,
                    GenerationId = description.GenerationId,
                    Status = new ResponseStatus
                    {
                        ErrorCode = (ErrorCode)description.ErrorCode
                    }
                };

                foreach (var member in description.Members)
                {
                    groupDesc.Members.Add(new MemberDescription
                    {
                        MemberId = member.MemberId,
                        GroupInstanceId = member.GroupInstanceId ?? "",
                        ClientId = member.ClientId,
                        MemberMetadata = ByteString.CopyFrom(member.Metadata),
                        MemberAssignment = ByteString.CopyFrom(member.Assignment)
                    });
                }

                response.Groups.Add(groupDesc);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to describe group {GroupId}", groupId);
                response.Groups.Add(new GroupDescription
                {
                    GroupId = groupId,
                    Status = new ResponseStatus
                    {
                        ErrorCode = ErrorCode.GroupNotFound,
                        ErrorMessage = ex.Message
                    }
                });
            }
        }

        return response;
    }

    public override async Task<DeleteGroupResponse> DeleteGroup(DeleteGroupRequest request, ServerCallContext context)
    {
        var client = _registry.GetClient(request.ClusterId);
        var response = new DeleteGroupResponse();

        // Collect group IDs from both single group_id and group_ids list
        var groupIds = new List<string>();
        if (!string.IsNullOrEmpty(request.GroupId))
            groupIds.Add(request.GroupId);
        groupIds.AddRange(request.GroupIds);

        foreach (var groupId in groupIds)
        {
            try
            {
                await client.Groups.DeleteAsync(groupId, context.CancellationToken);

                response.Results.Add(new DeleteGroupResult
                {
                    GroupId = groupId,
                    Status = new ResponseStatus { ErrorCode = ErrorCode.None }
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to delete group {GroupId}", groupId);
                response.Results.Add(new DeleteGroupResult
                {
                    GroupId = groupId,
                    Status = new ResponseStatus
                    {
                        ErrorCode = ErrorCode.Unknown,
                        ErrorMessage = ex.Message
                    }
                });
            }
        }

        return response;
    }

    public override Task<FindCoordinatorResponse> FindCoordinator(FindCoordinatorRequest request, ServerCallContext context)
    {
        // For a single-broker setup, the coordinator is always the connected broker
        var clusterConfig = _registry.GetConfig(request.ClusterId);
        if (clusterConfig == null)
        {
            return Task.FromResult(new FindCoordinatorResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.CoordinatorNotAvailable,
                    ErrorMessage = $"Unknown cluster: {request.ClusterId}"
                }
            });
        }

        return Task.FromResult(new FindCoordinatorResponse
        {
            NodeId = 0,
            Host = clusterConfig.BrokerHost,
            Port = clusterConfig.BrokerPort,
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        });
    }
}
