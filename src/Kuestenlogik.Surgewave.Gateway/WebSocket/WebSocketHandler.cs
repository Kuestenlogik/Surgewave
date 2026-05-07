using System.Net.WebSockets;
using Kuestenlogik.Surgewave.Gateway.WebSocket.Handlers;
using Kuestenlogik.Surgewave.Gateway.WebSocket.Protocol;
using Kuestenlogik.Surgewave.Gateway.WebSocket.Subscriptions;
using SysWebSocketMessageType = System.Net.WebSockets.WebSocketMessageType;

namespace Kuestenlogik.Surgewave.Gateway.WebSocket;

/// <summary>
/// Handles WebSocket message processing for a single session.
/// Manages the receive loop and dispatches messages to appropriate handlers.
/// </summary>
public sealed class WebSocketHandler
{
    private readonly GatewayConfig _config;
    private readonly ClusterRegistry _clusterRegistry;
    private readonly SubscribeHandler _subscribeHandler;
    private readonly ProduceHandler _produceHandler;
    private readonly AdminHandler _adminHandler;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<WebSocketHandler> _logger;

    public WebSocketHandler(
        GatewayConfig config,
        ClusterRegistry clusterRegistry,
        SubscribeHandler subscribeHandler,
        ProduceHandler produceHandler,
        AdminHandler adminHandler,
        IServiceProvider serviceProvider,
        ILogger<WebSocketHandler> logger)
    {
        _config = config;
        _clusterRegistry = clusterRegistry;
        _subscribeHandler = subscribeHandler;
        _produceHandler = produceHandler;
        _adminHandler = adminHandler;
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    /// <summary>
    /// Handles the WebSocket session lifecycle.
    /// </summary>
    public async Task HandleAsync(WebSocketSession session)
    {
        _logger.LogDebug("Starting WebSocket handler for session {SessionId}", session.SessionId);

        // Create per-session subscription manager
        var subscriptionManagerLogger = _serviceProvider.GetRequiredService<ILogger<SubscriptionManager>>();
        await using var subscriptionManager = new SubscriptionManager(
            session,
            _clusterRegistry,
            _config,
            subscriptionManagerLogger);

        // Start the send loop in the background
        var sendTask = session.RunSendLoopAsync();

        // Run the receive loop
        var receiveTask = RunReceiveLoopAsync(session, subscriptionManager);

        // Wait for either to complete
        await Task.WhenAny(sendTask, receiveTask);

        _logger.LogDebug("WebSocket handler completed for session {SessionId}", session.SessionId);
    }

    private async Task RunReceiveLoopAsync(WebSocketSession session, SubscriptionManager subscriptionManager)
    {
        var buffer = new byte[_config.WebSocket.MaxMessageSizeBytes];

        try
        {
            while (session.IsConnected && !session.CancellationToken.IsCancellationRequested)
            {
                var (messageType, data) = await session.ReceiveAsync(buffer);

                if (messageType == SysWebSocketMessageType.Close)
                {
                    _logger.LogDebug("Received close message for session {SessionId}", session.SessionId);
                    break;
                }

                if (data == null || data.Length == 0)
                {
                    continue;
                }

                await ProcessMessageAsync(session, subscriptionManager, data);
            }
        }
        catch (WebSocketException ex) when (ex.WebSocketErrorCode == WebSocketError.ConnectionClosedPrematurely)
        {
            _logger.LogDebug("Connection closed prematurely for session {SessionId}", session.SessionId);
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("Receive loop cancelled for session {SessionId}", session.SessionId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in receive loop for session {SessionId}", session.SessionId);
        }
    }

    private async Task ProcessMessageAsync(WebSocketSession session, SubscriptionManager subscriptionManager, byte[] data)
    {
        WebSocketMessage? baseMessage;
        try
        {
            baseMessage = WebSocketMessageSerializer.DeserializeBase(data);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to deserialize message for session {SessionId}", session.SessionId);
            await SendErrorAsync(session, null, WebSocketErrorCode.InvalidMessage, "Invalid JSON message");
            return;
        }

        if (baseMessage == null)
        {
            await SendErrorAsync(session, null, WebSocketErrorCode.InvalidMessage, "Empty message");
            return;
        }

        _logger.LogTrace("Received message type {Type} for session {SessionId}", baseMessage.Type, session.SessionId);

        try
        {
            await DispatchMessageAsync(session, subscriptionManager, baseMessage.Type, baseMessage.Id, data);
        }
        catch (KeyNotFoundException ex)
        {
            _logger.LogWarning("Unknown cluster requested: {Message}", ex.Message);
            await SendErrorAsync(session, baseMessage.Id, WebSocketErrorCode.UnknownCluster, ex.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing message {Type} for session {SessionId}", baseMessage.Type, session.SessionId);
            await SendErrorAsync(session, baseMessage.Id, WebSocketErrorCode.InternalError, "Internal server error");
        }
    }

    private async Task DispatchMessageAsync(
        WebSocketSession session,
        SubscriptionManager subscriptionManager,
        string messageType,
        string? requestId,
        byte[] rawData)
    {
        switch (messageType)
        {
            case Protocol.WebSocketMessageType.Ping:
                await HandlePingAsync(session, requestId);
                break;

            case Protocol.WebSocketMessageType.Subscribe:
                await _subscribeHandler.HandleSubscribeAsync(session, subscriptionManager, requestId, rawData);
                break;

            case Protocol.WebSocketMessageType.Unsubscribe:
                await _subscribeHandler.HandleUnsubscribeAsync(session, subscriptionManager, requestId, rawData);
                break;

            case Protocol.WebSocketMessageType.Produce:
                await _produceHandler.HandleProduceAsync(session, requestId, rawData);
                break;

            case Protocol.WebSocketMessageType.ProduceBatch:
                await _produceHandler.HandleProduceBatchAsync(session, requestId, rawData);
                break;

            case Protocol.WebSocketMessageType.Commit:
                await HandleCommitAsync(session, subscriptionManager, requestId, rawData);
                break;

            case Protocol.WebSocketMessageType.Admin:
                await _adminHandler.HandleAdminAsync(session, requestId, rawData);
                break;

            default:
                _logger.LogWarning("Unknown message type: {Type}", messageType);
                await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, $"Unknown message type: {messageType}");
                break;
        }
    }

    private async Task HandlePingAsync(WebSocketSession session, string? requestId)
    {
        session.Touch();
        var pong = WebSocketMessageSerializer.CreatePong(requestId);
        await session.SendAsync(pong);
    }

    private async Task HandleCommitAsync(WebSocketSession session, SubscriptionManager subscriptionManager, string? requestId, byte[] rawData)
    {
        // Deserialize the commit payload
        var message = WebSocketMessageSerializer.Deserialize<CommitPayload>(rawData);
        if (message?.Payload == null)
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Invalid commit payload");
            return;
        }

        var payload = message.Payload;

        // Validate required fields
        if (string.IsNullOrWhiteSpace(payload.ConsumerGroup))
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Consumer group is required");
            return;
        }

        if (payload.Offsets == null || payload.Offsets.Length == 0)
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "At least one offset is required");
            return;
        }

        _logger.LogDebug(
            "Commit request received for session {SessionId}: group={ConsumerGroup}, offsets={OffsetCount}",
            session.SessionId, payload.ConsumerGroup, payload.Offsets.Length);

        // Get the Surgewave client for this cluster
        if (!_clusterRegistry.TryGetClient(session.ClusterId, out var client) || client == null)
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.UnknownCluster, $"Unknown cluster: {session.ClusterId}");
            return;
        }

        // Commit each offset
        var errors = new List<string>();
        foreach (var offset in payload.Offsets)
        {
            try
            {
                await client.Groups.CommitOffsetAsync(
                    payload.ConsumerGroup,
                    "", // memberId - not required for simple WebSocket commit
                    0,  // generationId - not required for simple WebSocket commit
                    offset.Topic,
                    offset.Partition,
                    offset.Offset,
                    session.CancellationToken);

                _logger.LogDebug(
                    "Committed offset for {Topic}:{Partition}={Offset}",
                    offset.Topic, offset.Partition, offset.Offset);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Failed to commit offset for {Topic}:{Partition}",
                    offset.Topic, offset.Partition);
                errors.Add($"{offset.Topic}:{offset.Partition}: {ex.Message}");
            }
        }

        // Send response
        if (errors.Count == 0)
        {
            var response = WebSocketMessageSerializer.CreateCommitResponse(requestId, success: true);
            await session.SendAsync(response);
        }
        else if (errors.Count < payload.Offsets.Length)
        {
            // Partial success
            var response = WebSocketMessageSerializer.CreateCommitResponse(
                requestId,
                success: true,
                error: $"Partial commit: {string.Join("; ", errors)}");
            await session.SendAsync(response);
        }
        else
        {
            // All failed
            await SendErrorAsync(session, requestId, WebSocketErrorCode.CommitFailed, string.Join("; ", errors));
        }
    }

    private async Task SendErrorAsync(WebSocketSession session, string? requestId, string code, string message)
    {
        var error = WebSocketMessageSerializer.CreateError(requestId, code, message);
        await session.SendAsync(error);
    }
}
