using Kuestenlogik.Surgewave.Gateway.WebSocket.Protocol;

namespace Kuestenlogik.Surgewave.Gateway.WebSocket.Handlers;

/// <summary>
/// Handles admin WebSocket messages.
/// </summary>
public sealed class AdminHandler
{
    private readonly ClusterRegistry _clusterRegistry;
    private readonly ILogger<AdminHandler> _logger;

    public AdminHandler(
        ClusterRegistry clusterRegistry,
        ILogger<AdminHandler> logger)
    {
        _clusterRegistry = clusterRegistry;
        _logger = logger;
    }

    /// <summary>
    /// Handles an admin request.
    /// </summary>
    public async Task HandleAdminAsync(
        WebSocketSession session,
        string? requestId,
        byte[] rawData)
    {
        WebSocketMessage<AdminPayload>? message;
        try
        {
            message = WebSocketMessageSerializer.Deserialize<AdminPayload>(rawData);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to deserialize admin message");
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Invalid admin message format");
            return;
        }

        if (message?.Payload == null)
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Missing payload");
            return;
        }

        var payload = message.Payload;

        if (string.IsNullOrEmpty(payload.Action))
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Action is required");
            return;
        }

        // Get the client for the session's cluster
        if (!_clusterRegistry.TryGetClient(session.ClusterId, out var client) || client == null)
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.UnknownCluster, $"Unknown cluster: {session.ClusterId}");
            return;
        }

        try
        {
            _logger.LogDebug(
                "Admin request {Action} on session {SessionId}",
                payload.Action, session.SessionId);

            object? result = payload.Action.ToLowerInvariant() switch
            {
                AdminActionType.ListTopics => await HandleListTopicsAsync(client, payload),
                AdminActionType.DescribeTopic => await HandleDescribeTopicAsync(client, payload),
                AdminActionType.ListConsumerGroups => await HandleListConsumerGroupsAsync(client, payload),
                AdminActionType.DescribeConsumerGroup => await HandleDescribeConsumerGroupAsync(client, payload),
                AdminActionType.GetClusterInfo => await HandleGetClusterInfoAsync(session.ClusterId),
                _ => throw new InvalidOperationException($"Unknown admin action: {payload.Action}")
            };

            var response = new WebSocketMessage<AdminResponsePayload>
            {
                Type = WebSocketMessageType.AdminResponse,
                Id = requestId,
                Payload = new AdminResponsePayload
                {
                    Success = true,
                    Action = payload.Action,
                    Data = result
                }
            };

            await session.SendAsync(response);

            _logger.LogDebug("Admin request {Action} completed", payload.Action);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to execute admin action {Action}", payload.Action);
            await SendAdminErrorAsync(session, requestId, payload.Action, ex.Message);
        }
    }

    private async Task<object> HandleListTopicsAsync(
        Kuestenlogik.Surgewave.Client.Native.SurgewaveNativeClient client,
        AdminPayload payload)
    {
        var topics = await client.Topics.ListAsync();

        return new
        {
            topics = topics.Select(t => new
            {
                name = t.Name,
                partition_count = t.PartitionCount
            }).ToList()
        };
    }

    private async Task<object> HandleDescribeTopicAsync(
        Kuestenlogik.Surgewave.Client.Native.SurgewaveNativeClient client,
        AdminPayload payload)
    {
        if (string.IsNullOrEmpty(payload.Topic))
        {
            throw new ArgumentException("Topic name is required for describe_topic action");
        }

        var description = await client.Topics.DescribeAsync(payload.Topic);

        return new
        {
            name = description.Name,
            partition_count = description.PartitionCount,
            replication_factor = description.ReplicationFactor,
            is_internal = description.IsInternal,
            partitions = description.Partitions.Select(p => new
            {
                partition_id = p.PartitionId,
                leader = p.Leader,
                replicas = p.Replicas,
                isr = p.Isr,
                high_watermark = p.HighWatermark,
                log_start_offset = p.LogStartOffset
            }).ToList()
        };
    }

    private async Task<object> HandleListConsumerGroupsAsync(
        Kuestenlogik.Surgewave.Client.Native.SurgewaveNativeClient client,
        AdminPayload payload)
    {
        var groups = await client.Groups.ListAsync();

        return new
        {
            groups = groups.Select(g => new
            {
                group_id = g.GroupId,
                state = g.State,
                protocol_type = g.ProtocolType
            }).ToList()
        };
    }

    private async Task<object> HandleDescribeConsumerGroupAsync(
        Kuestenlogik.Surgewave.Client.Native.SurgewaveNativeClient client,
        AdminPayload payload)
    {
        if (string.IsNullOrEmpty(payload.GroupId))
        {
            throw new ArgumentException("group_id is required for describe_consumer_group action");
        }

        var description = await client.Groups.DescribeAsync(payload.GroupId);

        return new
        {
            group_id = description.GroupId,
            state = description.State,
            protocol_type = description.ProtocolType,
            protocol_name = description.ProtocolName,
            generation_id = description.GenerationId,
            members = description.Members.Select(m => new
            {
                member_id = m.MemberId,
                group_instance_id = m.GroupInstanceId,
                client_id = m.ClientId
            }).ToList()
        };
    }

    private Task<object> HandleGetClusterInfoAsync(string clusterId)
    {
        var config = _clusterRegistry.GetConfig(clusterId);

        return Task.FromResult<object>(new
        {
            cluster_id = clusterId,
            broker_host = config?.BrokerHost,
            broker_port = config?.BrokerPort,
            pipelining_enabled = config?.EnablePipelining ?? false
        });
    }

    private static async Task SendErrorAsync(WebSocketSession session, string? requestId, string code, string message)
    {
        var error = WebSocketMessageSerializer.CreateError(requestId, code, message);
        await session.SendAsync(error);
    }

    private static async Task SendAdminErrorAsync(WebSocketSession session, string? requestId, string action, string errorMessage)
    {
        var response = new WebSocketMessage<AdminResponsePayload>
        {
            Type = WebSocketMessageType.AdminResponse,
            Id = requestId,
            Payload = new AdminResponsePayload
            {
                Success = false,
                Action = action,
                Error = errorMessage
            }
        };
        await session.SendAsync(response);
    }
}
