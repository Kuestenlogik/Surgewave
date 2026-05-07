using System.Net.WebSockets;
using System.Text;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket;

/// <summary>
/// WebSocket protocol adapter that bridges browser-based WebSocket connections to Surgewave topics.
/// Provides produce (send), consume (receive), and multi-topic subscribe endpoints.
/// </summary>
public sealed class WebSocketProtocolAdapter
{
    private readonly WebSocketConfig _config;
    private readonly LogManager _logManager;
    private readonly ILogger<WebSocketProtocolAdapter> _logger;
    private int _activeConnections;

    /// <summary>
    /// Gets the number of currently active WebSocket connections.
    /// </summary>
    public int ActiveConnections => _activeConnections;

    public WebSocketProtocolAdapter(
        IOptions<WebSocketConfig> config,
        LogManager logManager,
        ILogger<WebSocketProtocolAdapter> logger)
    {
        _config = config.Value;
        _logManager = logManager;
        _logger = logger;
    }

    /// <summary>
    /// Map WebSocket endpoints onto the ASP.NET Core routing pipeline.
    /// </summary>
    public void MapEndpoints(IEndpointRouteBuilder endpoints)
    {
        var basePath = _config.Path.TrimEnd('/');

        endpoints.Map($"{basePath}/produce/{{topic}}", HandleProduceAsync);
        endpoints.Map($"{basePath}/consume/{{topic}}", HandleConsumeAsync);
        endpoints.Map($"{basePath}/subscribe", HandleSubscribeAsync);

        _logger.LogInformation("WebSocket protocol adapter mapped at {BasePath}/produce/{{topic}}, {BasePath}/consume/{{topic}}, {BasePath}/subscribe",
            basePath, basePath, basePath);
    }

    private async Task HandleProduceAsync(HttpContext context)
    {
        if (!context.WebSockets.IsWebSocketRequest)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsync("WebSocket connection required").ConfigureAwait(false);
            return;
        }

        if (_activeConnections >= _config.MaxConnections)
        {
            context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            await context.Response.WriteAsync("Maximum WebSocket connections reached").ConfigureAwait(false);
            return;
        }

        var topic = context.GetRouteValue("topic")?.ToString();
        if (string.IsNullOrEmpty(topic))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsync("Topic name is required").ConfigureAwait(false);
            return;
        }

        using var ws = await context.WebSockets.AcceptWebSocketAsync().ConfigureAwait(false);
        Interlocked.Increment(ref _activeConnections);

        try
        {
            await ProduceLoopAsync(ws, topic, context.RequestAborted).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Decrement(ref _activeConnections);
        }
    }

    private async Task ProduceLoopAsync(System.Net.WebSockets.WebSocket ws, string topic, CancellationToken cancellationToken)
    {
        var buffer = new byte[_config.MaxMessageSizeBytes];

        while (ws.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
        {
            var result = await ws.ReceiveAsync(buffer, cancellationToken).ConfigureAwait(false);

            if (result.MessageType == WebSocketMessageType.Close)
                break;

            if (result.MessageType != WebSocketMessageType.Text)
                continue;

            try
            {
                var json = buffer.AsSpan(0, result.Count);
                var message = WebSocketMessageSerializer.DeserializeProduceMessage(json);

                if (message?.Value is null)
                {
                    await SendErrorAsync(ws, "invalid_request", "Message value is required", cancellationToken).ConfigureAwait(false);
                    continue;
                }

                // Serialize value to bytes for storage
                var valueBytes = Encoding.UTF8.GetBytes(message.Value.ToString() ?? "");
                var tp = new TopicPartition { Topic = topic, Partition = 0 };
                await _logManager.AppendBatchAsync(tp, valueBytes, cancellationToken).ConfigureAwait(false);

                _logger.LogDebug("WebSocket PRODUCE to {Topic}: {Bytes} bytes", topic, valueBytes.Length);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error processing WebSocket produce message for topic {Topic}", topic);
                await SendErrorAsync(ws, "internal_error", ex.Message, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task HandleConsumeAsync(HttpContext context)
    {
        if (!context.WebSockets.IsWebSocketRequest)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsync("WebSocket connection required").ConfigureAwait(false);
            return;
        }

        if (_activeConnections >= _config.MaxConnections)
        {
            context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            await context.Response.WriteAsync("Maximum WebSocket connections reached").ConfigureAwait(false);
            return;
        }

        var topic = context.GetRouteValue("topic")?.ToString();
        if (string.IsNullOrEmpty(topic))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsync("Topic name is required").ConfigureAwait(false);
            return;
        }

        using var ws = await context.WebSockets.AcceptWebSocketAsync().ConfigureAwait(false);
        Interlocked.Increment(ref _activeConnections);

        try
        {
            await ConsumeLoopAsync(ws, topic, context.RequestAborted).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Decrement(ref _activeConnections);
        }
    }

    private async Task ConsumeLoopAsync(System.Net.WebSockets.WebSocket ws, string topic, CancellationToken cancellationToken)
    {
        var tp = new TopicPartition { Topic = topic, Partition = 0 };
        var log = _logManager.GetLog(tp);
        long offset = log?.HighWatermark ?? 0;

        while (ws.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
        {
            try
            {
                var batches = await _logManager.ReadBatchesAsync(tp, offset, cancellationToken: cancellationToken).ConfigureAwait(false);

                if (batches.Count > 0)
                {
                    foreach (var batch in batches)
                    {
                        var consumeMsg = new WebSocketConsumeMessage
                        {
                            Topic = topic,
                            Partition = 0,
                            Offset = offset,
                            Value = Encoding.UTF8.GetString(batch),
                            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        };

                        var json = WebSocketMessageSerializer.SerializeConsumeMessage(consumeMsg);
                        await ws.SendAsync(json, WebSocketMessageType.Text, endOfMessage: true, cancellationToken).ConfigureAwait(false);
                        offset++;
                    }
                }
                else
                {
                    // Poll interval when no new data
                    await Task.Delay(100, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error in WebSocket consume loop for topic {Topic}", topic);
                await SendErrorAsync(ws, "internal_error", ex.Message, cancellationToken).ConfigureAwait(false);
                break;
            }
        }
    }

    private async Task HandleSubscribeAsync(HttpContext context)
    {
        if (!context.WebSockets.IsWebSocketRequest)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsync("WebSocket connection required").ConfigureAwait(false);
            return;
        }

        if (_activeConnections >= _config.MaxConnections)
        {
            context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            await context.Response.WriteAsync("Maximum WebSocket connections reached").ConfigureAwait(false);
            return;
        }

        using var ws = await context.WebSockets.AcceptWebSocketAsync().ConfigureAwait(false);
        Interlocked.Increment(ref _activeConnections);

        try
        {
            await SubscribeLoopAsync(ws, context.RequestAborted).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Decrement(ref _activeConnections);
        }
    }

    private async Task SubscribeLoopAsync(System.Net.WebSockets.WebSocket ws, CancellationToken cancellationToken)
    {
        var subscribedTopics = new Dictionary<string, long>(); // topic -> current offset
        var buffer = new byte[_config.MaxMessageSizeBytes];

        // Start a background task to poll for messages and send to client
        using var pollCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var pollTask = Task.Run(() => PollSubscribedTopicsAsync(ws, subscribedTopics, pollCts.Token), pollCts.Token);

        try
        {
            while (ws.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
            {
                var result = await ws.ReceiveAsync(buffer, cancellationToken).ConfigureAwait(false);

                if (result.MessageType == WebSocketMessageType.Close)
                    break;

                if (result.MessageType != WebSocketMessageType.Text)
                    continue;

                var json = buffer.AsSpan(0, result.Count);
                var subMsg = WebSocketMessageSerializer.DeserializeSubscribeMessage(json);

                if (subMsg is null)
                {
                    await SendErrorAsync(ws, "invalid_request", "Invalid subscribe message format", cancellationToken).ConfigureAwait(false);
                    continue;
                }

                if (string.Equals(subMsg.Action, "subscribe", StringComparison.OrdinalIgnoreCase))
                {
                    foreach (var topic in subMsg.Topics)
                    {
                        long startOffset = 0;
                        if (subMsg.Offsets?.TryGetValue(topic, out var requested) == true)
                        {
                            startOffset = requested switch
                            {
                                -1 => _logManager.GetLog(new TopicPartition { Topic = topic, Partition = 0 })?.HighWatermark ?? 0,
                                -2 => 0,
                                _ => requested,
                            };
                        }
                        else
                        {
                            // Default to latest
                            startOffset = _logManager.GetLog(new TopicPartition { Topic = topic, Partition = 0 })?.HighWatermark ?? 0;
                        }

                        subscribedTopics[topic] = startOffset;
                        _logger.LogDebug("WebSocket client subscribed to {Topic} at offset {Offset}", topic, startOffset);
                    }
                }
                else if (string.Equals(subMsg.Action, "unsubscribe", StringComparison.OrdinalIgnoreCase))
                {
                    foreach (var topic in subMsg.Topics)
                    {
                        subscribedTopics.Remove(topic);
                        _logger.LogDebug("WebSocket client unsubscribed from {Topic}", topic);
                    }
                }
                else
                {
                    await SendErrorAsync(ws, "invalid_request", $"Unknown action: {subMsg.Action}", cancellationToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            await pollCts.CancelAsync().ConfigureAwait(false);
            try { await pollTask.ConfigureAwait(false); } catch (OperationCanceledException) { }
        }
    }

    private async Task PollSubscribedTopicsAsync(System.Net.WebSockets.WebSocket ws, Dictionary<string, long> subscribedTopics, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested && ws.State == WebSocketState.Open)
        {
            try
            {
                bool hadData = false;

                // Snapshot the topics to avoid modification during iteration
                var topics = subscribedTopics.Keys.ToList();
                foreach (var topic in topics)
                {
                    if (!subscribedTopics.TryGetValue(topic, out var offset))
                        continue;

                    var tp = new TopicPartition { Topic = topic, Partition = 0 };
                    var batches = await _logManager.ReadBatchesAsync(tp, offset, cancellationToken: cancellationToken).ConfigureAwait(false);

                    if (batches.Count > 0)
                    {
                        hadData = true;
                        foreach (var batch in batches)
                        {
                            var consumeMsg = new WebSocketConsumeMessage
                            {
                                Topic = topic,
                                Partition = 0,
                                Offset = offset,
                                Value = Encoding.UTF8.GetString(batch),
                                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                            };

                            var json = WebSocketMessageSerializer.SerializeConsumeMessage(consumeMsg);
                            await ws.SendAsync(json, WebSocketMessageType.Text, endOfMessage: true, cancellationToken).ConfigureAwait(false);
                            offset++;
                        }
                        subscribedTopics[topic] = offset;
                    }
                }

                if (!hadData)
                {
                    await Task.Delay(100, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error in WebSocket subscribe poll loop");
                break;
            }
        }
    }

    private static async Task SendErrorAsync(System.Net.WebSockets.WebSocket ws, string error, string message, CancellationToken cancellationToken)
    {
        if (ws.State != WebSocketState.Open)
            return;

        var errorMsg = new WebSocketErrorMessage { Error = error, Message = message };
        var json = WebSocketMessageSerializer.SerializeErrorMessage(errorMsg);
        await ws.SendAsync(json, WebSocketMessageType.Text, endOfMessage: true, cancellationToken).ConfigureAwait(false);
    }
}
