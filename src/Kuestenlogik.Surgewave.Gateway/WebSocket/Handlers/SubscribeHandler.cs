using Kuestenlogik.Surgewave.Gateway.WebSocket.Protocol;
using Kuestenlogik.Surgewave.Gateway.WebSocket.Subscriptions;

namespace Kuestenlogik.Surgewave.Gateway.WebSocket.Handlers;

/// <summary>
/// Handles subscribe and unsubscribe WebSocket messages.
/// </summary>
public sealed class SubscribeHandler
{
    private readonly ILogger<SubscribeHandler> _logger;

    public SubscribeHandler(ILogger<SubscribeHandler> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Handles a subscribe request.
    /// </summary>
    public async Task HandleSubscribeAsync(
        WebSocketSession session,
        SubscriptionManager subscriptionManager,
        string? requestId,
        byte[] rawData)
    {
        WebSocketMessage<SubscribePayload>? message;
        try
        {
            message = WebSocketMessageSerializer.Deserialize<SubscribePayload>(rawData);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to deserialize subscribe message");
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Invalid subscribe message format");
            return;
        }

        if (message?.Payload == null)
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Missing payload");
            return;
        }

        var payload = message.Payload;

        if (string.IsNullOrEmpty(payload.Topic))
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Topic is required");
            return;
        }

        _logger.LogDebug(
            "Subscribe request for topic {Topic} on session {SessionId}",
            payload.Topic, session.SessionId);

        var (subscription, error) = await subscriptionManager.SubscribeAsync(
            payload.Topic,
            payload.Partitions,
            payload.FromOffset,
            payload.ConsumerGroup,
            session.CancellationToken);

        if (subscription == null)
        {
            _logger.LogWarning(
                "Subscribe failed for topic {Topic}: {Error}",
                payload.Topic, error);

            await SendErrorAsync(session, requestId, WebSocketErrorCode.SubscribeError, error ?? "Subscribe failed");
            return;
        }

        var response = WebSocketMessageSerializer.CreateSubscribeResponse(
            requestId,
            success: true,
            subscriptionId: subscription.SubscriptionId,
            topic: subscription.Topic,
            partitions: subscription.Partitions);

        await session.SendAsync(response);

        _logger.LogInformation(
            "Subscription {SubscriptionId} created for topic {Topic} on session {SessionId}",
            subscription.SubscriptionId, payload.Topic, session.SessionId);
    }

    /// <summary>
    /// Handles an unsubscribe request.
    /// </summary>
    public async Task HandleUnsubscribeAsync(
        WebSocketSession session,
        SubscriptionManager subscriptionManager,
        string? requestId,
        byte[] rawData)
    {
        WebSocketMessage<UnsubscribePayload>? message;
        try
        {
            message = WebSocketMessageSerializer.Deserialize<UnsubscribePayload>(rawData);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to deserialize unsubscribe message");
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Invalid unsubscribe message format");
            return;
        }

        if (message?.Payload == null || string.IsNullOrEmpty(message.Payload.SubscriptionId))
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Subscription ID is required");
            return;
        }

        var subscriptionId = message.Payload.SubscriptionId;

        _logger.LogDebug(
            "Unsubscribe request for subscription {SubscriptionId} on session {SessionId}",
            subscriptionId, session.SessionId);

        var success = await subscriptionManager.UnsubscribeAsync(subscriptionId);

        if (!success)
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.SubscriptionNotFound, $"Subscription not found: {subscriptionId}");
            return;
        }

        var response = new WebSocketMessage<UnsubscribeResponsePayload>
        {
            Type = WebSocketMessageType.UnsubscribeResponse,
            Id = requestId,
            Payload = new UnsubscribeResponsePayload
            {
                Success = true,
                SubscriptionId = subscriptionId
            }
        };

        await session.SendAsync(response);

        _logger.LogInformation(
            "Subscription {SubscriptionId} removed on session {SessionId}",
            subscriptionId, session.SessionId);
    }

    private static async Task SendErrorAsync(WebSocketSession session, string? requestId, string code, string message)
    {
        var error = WebSocketMessageSerializer.CreateError(requestId, code, message);
        await session.SendAsync(error);
    }
}
