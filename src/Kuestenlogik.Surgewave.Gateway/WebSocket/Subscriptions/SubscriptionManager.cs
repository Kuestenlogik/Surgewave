using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Gateway.WebSocket.Protocol;

namespace Kuestenlogik.Surgewave.Gateway.WebSocket.Subscriptions;

/// <summary>
/// Manages subscriptions for a single WebSocket session.
/// </summary>
public sealed class SubscriptionManager : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, Subscription> _subscriptions = new();
    private readonly WebSocketSession _session;
    private readonly ClusterRegistry _clusterRegistry;
    private readonly GatewayConfig _config;
    private readonly ILogger<SubscriptionManager> _logger;
    private int _disposed;

    public SubscriptionManager(
        WebSocketSession session,
        ClusterRegistry clusterRegistry,
        GatewayConfig config,
        ILogger<SubscriptionManager> logger)
    {
        _session = session;
        _clusterRegistry = clusterRegistry;
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Gets the number of active subscriptions.
    /// </summary>
    public int SubscriptionCount => _subscriptions.Count;

    /// <summary>
    /// Creates a new subscription.
    /// </summary>
    public async Task<(Subscription? subscription, string? error)> SubscribeAsync(
        string topic,
        int[]? partitions,
        string? fromOffset,
        string? consumerGroup,
        CancellationToken cancellationToken)
    {
        if (_disposed != 0)
        {
            return (null, "Session is closing");
        }

        if (_subscriptions.Count >= _config.WebSocket.MaxSubscriptionsPerSession)
        {
            return (null, $"Maximum subscriptions ({_config.WebSocket.MaxSubscriptionsPerSession}) exceeded");
        }

        // Validate cluster and topic
        if (!_clusterRegistry.TryGetClient(_session.ClusterId, out var client) || client == null)
        {
            return (null, $"Unknown cluster: {_session.ClusterId}");
        }

        // Check if topic exists
        try
        {
            var topics = await client.Topics.ListAsync(cancellationToken);
            if (!topics.Any(t => t.Name == topic))
            {
                return (null, $"Topic '{topic}' does not exist");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to validate topic {Topic}", topic);
            return (null, "Failed to validate topic");
        }

        var subscription = new Subscription(
            _session.SessionId,
            _session.ClusterId,
            topic,
            partitions,
            consumerGroup);

        if (!_subscriptions.TryAdd(subscription.SubscriptionId, subscription))
        {
            return (null, "Failed to register subscription");
        }

        // Start the streaming task
        var streamingTask = RunStreamingAsync(subscription, client, fromOffset);
        subscription.SetStreamingTask(streamingTask);

        _logger.LogInformation(
            "Created subscription {SubscriptionId} for topic {Topic} on session {SessionId}",
            subscription.SubscriptionId, topic, _session.SessionId);

        return (subscription, null);
    }

    /// <summary>
    /// Cancels a subscription.
    /// </summary>
    public async Task<bool> UnsubscribeAsync(string subscriptionId)
    {
        if (!_subscriptions.TryRemove(subscriptionId, out var subscription))
        {
            return false;
        }

        _logger.LogInformation(
            "Removing subscription {SubscriptionId} for topic {Topic}",
            subscriptionId, subscription.Topic);

        await subscription.DisposeAsync();
        return true;
    }

    /// <summary>
    /// Gets a subscription by ID.
    /// </summary>
    public Subscription? GetSubscription(string subscriptionId)
    {
        return _subscriptions.GetValueOrDefault(subscriptionId);
    }

    private async Task RunStreamingAsync(Subscription subscription, SurgewaveNativeClient client, string? fromOffset)
    {
        try
        {
            var builder = client.Messaging.Receive(subscription.Topic);

            // Configure partitions
            if (subscription.Partitions != null && subscription.Partitions.Length > 0)
            {
                if (subscription.Partitions.Length == 1)
                {
                    builder.FromPartition(subscription.Partitions[0]);
                }
                else
                {
                    builder.FromPartitions(subscription.Partitions);
                }
            }
            else
            {
                builder.FromAllPartitions();
            }

            // Configure offset
            switch (fromOffset?.ToLowerInvariant())
            {
                case "earliest":
                case "beginning":
                    builder.FromBeginning();
                    break;
                case "latest":
                case "end":
                case null:
                    builder.FromEnd();
                    break;
                default:
                    if (long.TryParse(fromOffset, out var offset))
                    {
                        builder.FromOffset(offset);
                    }
                    else
                    {
                        builder.FromEnd();
                    }
                    break;
            }

            // Stream messages
            if (_config.WebSocket.BatchDeliveryEnabled)
            {
                await StreamBatchedAsync(subscription, builder);
            }
            else
            {
                await StreamSingleAsync(subscription, builder);
            }
        }
        catch (OperationCanceledException) when (subscription.CancellationToken.IsCancellationRequested)
        {
            // Expected on unsubscribe
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error streaming subscription {SubscriptionId}", subscription.SubscriptionId);

            // Send error to client
            await SendErrorAsync(subscription.SubscriptionId, "STREAM_ERROR", ex.Message);
        }
        finally
        {
            _subscriptions.TryRemove(subscription.SubscriptionId, out _);
        }
    }

    private async Task StreamSingleAsync(Subscription subscription, ReceiveBuilder builder)
    {
        await foreach (var msg in builder.Stream(subscription.CancellationToken))
        {
            if (!_session.IsConnected) break;

            var message = WebSocketMessageSerializer.CreateMessage(
                subscription.SubscriptionId,
                subscription.Topic,
                0, // Partition info not available in ReceivedMessage
                msg.Offset,
                msg.Timestamp,
                msg.KeyString,
                msg.ValueString);

            await _session.SendAsync(message);
        }
    }

    private async Task StreamBatchedAsync(Subscription subscription, ReceiveBuilder builder)
    {
        var batch = new List<MessageBatchRecord>(_config.WebSocket.BatchDeliveryMaxSize);
        var lastSend = DateTimeOffset.UtcNow;
        var maxWait = TimeSpan.FromMilliseconds(_config.WebSocket.BatchDeliveryMaxWaitMs);
        long highWatermark = 0;

        await foreach (var msg in builder.Stream(subscription.CancellationToken))
        {
            if (!_session.IsConnected) break;

            batch.Add(new MessageBatchRecord
            {
                Offset = msg.Offset,
                Timestamp = msg.Timestamp,
                Key = msg.KeyString,
                Value = msg.ValueString
            });

            highWatermark = msg.Offset;

            // Send batch if full or timeout exceeded
            if (batch.Count >= _config.WebSocket.BatchDeliveryMaxSize ||
                DateTimeOffset.UtcNow - lastSend >= maxWait)
            {
                await SendBatchAsync(subscription, batch, highWatermark);
                batch.Clear();
                lastSend = DateTimeOffset.UtcNow;
            }
        }

        // Send remaining messages
        if (batch.Count > 0)
        {
            await SendBatchAsync(subscription, batch, highWatermark);
        }
    }

    private async Task SendBatchAsync(Subscription subscription, List<MessageBatchRecord> records, long highWatermark)
    {
        var message = WebSocketMessageSerializer.CreateMessageBatch(
            subscription.SubscriptionId,
            subscription.Topic,
            0, // Partition info aggregated
            highWatermark,
            [.. records]);

        await _session.SendAsync(message);
    }

    private async Task SendErrorAsync(string subscriptionId, string code, string message)
    {
        try
        {
            var error = WebSocketMessageSerializer.CreateError(null, code, message, new { subscription_id = subscriptionId });
            await _session.SendAsync(error);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to send error to session");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        var subscriptions = _subscriptions.Values.ToList();
        _subscriptions.Clear();

        foreach (var subscription in subscriptions)
        {
            try
            {
                await subscription.DisposeAsync();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error disposing subscription {SubscriptionId}", subscription.SubscriptionId);
            }
        }
    }
}
