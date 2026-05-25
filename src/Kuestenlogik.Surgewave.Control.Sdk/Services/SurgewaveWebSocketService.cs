using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// WebSocket service implementation for real-time communication with Surgewave Gateway.
/// </summary>
public sealed class SurgewaveWebSocketService : ISurgewaveWebSocketService
{
    private readonly string _gatewayUrl;
    private ClientWebSocket? _webSocket;
    private CancellationTokenSource? _receiveCts;
    private Task? _receiveTask;
    private int _requestId;

    public bool IsConnected => _webSocket?.State == WebSocketState.Open;

    public event EventHandler<WebSocketMessageEventArgs>? MessageReceived;
    public event EventHandler<bool>? ConnectionStateChanged;

    public SurgewaveWebSocketService(Uri gatewayUri)
    {
        _gatewayUrl = gatewayUri.ToString().TrimEnd('/');
    }

    public SurgewaveWebSocketService(string gatewayUrl)
        : this(new Uri(gatewayUrl))
    {
    }

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (IsConnected)
        {
            return;
        }

        _webSocket?.Dispose();
        _webSocket = new ClientWebSocket();

        var wsUrl = _gatewayUrl
            .Replace("http://", "ws://")
            .Replace("https://", "wss://") + "/ws";

        await _webSocket.ConnectAsync(new Uri(wsUrl), cancellationToken);

        _receiveCts = new CancellationTokenSource();
        _receiveTask = ReceiveLoopAsync(_receiveCts.Token);

        ConnectionStateChanged?.Invoke(this, true);
    }

    public async Task DisconnectAsync()
    {
        if (_webSocket == null)
        {
            return;
        }

        _receiveCts?.Cancel();

        if (_webSocket.State == WebSocketState.Open)
        {
            try
            {
                await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", CancellationToken.None);
            }
            catch
            {
                // Ignore close errors
            }
        }

        if (_receiveTask != null)
        {
            try
            {
                await _receiveTask;
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }

        _webSocket.Dispose();
        _webSocket = null;
        _receiveCts?.Dispose();
        _receiveCts = null;

        ConnectionStateChanged?.Invoke(this, false);
    }

    public async Task SubscribeAsync(string topic, string? groupId = null, CancellationToken cancellationToken = default)
    {
        var message = new
        {
            type = "subscribe",
            id = GetNextRequestId(),
            payload = new
            {
                topic,
                group_id = groupId
            }
        };

        await SendMessageAsync(message, cancellationToken);
    }

    public async Task UnsubscribeAsync(string topic, CancellationToken cancellationToken = default)
    {
        var message = new
        {
            type = "unsubscribe",
            id = GetNextRequestId(),
            payload = new
            {
                topic
            }
        };

        await SendMessageAsync(message, cancellationToken);
    }

    public async Task<bool> PingAsync(CancellationToken cancellationToken = default)
    {
        if (!IsConnected)
        {
            return false;
        }

        var message = new
        {
            type = "ping",
            id = GetNextRequestId()
        };

        await SendMessageAsync(message, cancellationToken);
        return true;
    }

    private async Task SendMessageAsync(object message, CancellationToken cancellationToken)
    {
        if (_webSocket == null || _webSocket.State != WebSocketState.Open)
        {
            throw new InvalidOperationException("WebSocket is not connected");
        }

        var json = JsonSerializer.Serialize(message, JsonOptions);
        var bytes = Encoding.UTF8.GetBytes(json);

        await _webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, cancellationToken);
    }

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[8192];

        try
        {
            while (!cancellationToken.IsCancellationRequested && _webSocket?.State == WebSocketState.Open)
            {
                var result = await _webSocket.ReceiveAsync(buffer, cancellationToken);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    break;
                }

                if (result.MessageType == WebSocketMessageType.Text)
                {
                    var json = Encoding.UTF8.GetString(buffer, 0, result.Count);
                    ProcessMessage(json);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when disconnecting
        }
        catch (WebSocketException)
        {
            // Connection lost
        }

        ConnectionStateChanged?.Invoke(this, false);
    }

    private void ProcessMessage(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            if (!root.TryGetProperty("type", out var typeElement))
            {
                return;
            }

            var type = typeElement.GetString() ?? "";

            if (type == "message" && root.TryGetProperty("payload", out var payload))
            {
                var topic = payload.TryGetProperty("topic", out var t) ? t.GetString() ?? "" : "";
                var key = payload.TryGetProperty("key", out var k) ? k.GetString() : null;
                var value = payload.TryGetProperty("value", out var v) ? v.GetString() : null;
                var offset = payload.TryGetProperty("offset", out var o) ? o.GetInt64() : 0;
                var timestamp = payload.TryGetProperty("timestamp", out var ts) ? ts.GetInt64() : 0;

                MessageReceived?.Invoke(this, new WebSocketMessageEventArgs
                {
                    Type = type,
                    Topic = topic,
                    Key = key,
                    Value = value,
                    Offset = offset,
                    Timestamp = timestamp
                });
            }
        }
        catch
        {
            // Ignore parsing errors
        }
    }

    private string GetNextRequestId()
    {
        return $"req-{Interlocked.Increment(ref _requestId)}";
    }

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync();
    }
}
