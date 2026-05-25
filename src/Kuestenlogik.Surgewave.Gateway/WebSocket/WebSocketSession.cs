using System.Net.WebSockets;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Gateway.WebSocket.Protocol;
using SysWebSocketMessageType = System.Net.WebSockets.WebSocketMessageType;

namespace Kuestenlogik.Surgewave.Gateway.WebSocket;

/// <summary>
/// Represents a single WebSocket connection session.
/// </summary>
public sealed class WebSocketSession : IAsyncDisposable
{
    private readonly System.Net.WebSockets.WebSocket _webSocket;
    private readonly Channel<byte[]> _sendChannel;
    private readonly CancellationTokenSource _cts;
    private readonly ILogger<WebSocketSession> _logger;
    private DateTimeOffset _lastActivity;
    private int _disposed;

    /// <summary>
    /// Unique session identifier.
    /// </summary>
    public string SessionId { get; }

    /// <summary>
    /// Cluster ID this session is bound to.
    /// </summary>
    public string ClusterId { get; }

    /// <summary>
    /// Remote endpoint address.
    /// </summary>
    public string? RemoteAddress { get; }

    /// <summary>
    /// When the session was established.
    /// </summary>
    public DateTimeOffset ConnectedAt { get; }

    /// <summary>
    /// Last activity timestamp.
    /// </summary>
    public DateTimeOffset LastActivity => _lastActivity;

    /// <summary>
    /// Cancellation token for this session.
    /// </summary>
    public CancellationToken CancellationToken => _cts.Token;

    /// <summary>
    /// Whether the session is still connected.
    /// </summary>
    public bool IsConnected => _webSocket.State == WebSocketState.Open && _disposed == 0;

    public WebSocketSession(
        System.Net.WebSockets.WebSocket webSocket,
        string clusterId,
        string? remoteAddress,
        WebSocketConfig config,
        ILogger<WebSocketSession> logger)
    {
        SessionId = Guid.NewGuid().ToString("N")[..12];
        ClusterId = clusterId;
        RemoteAddress = remoteAddress;
        ConnectedAt = DateTimeOffset.UtcNow;
        _lastActivity = ConnectedAt;

        _webSocket = webSocket;
        _logger = logger;
        _cts = new CancellationTokenSource();
        _sendChannel = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(config.SendBufferCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });
    }

    /// <summary>
    /// Queues a message to be sent to the client.
    /// </summary>
    public async ValueTask SendAsync<T>(WebSocketMessage<T> message) where T : class
    {
        if (_disposed != 0) return;

        var bytes = WebSocketMessageSerializer.Serialize(message);
        await _sendChannel.Writer.WriteAsync(bytes, _cts.Token);
    }

    /// <summary>
    /// Queues raw bytes to be sent to the client.
    /// </summary>
    public async ValueTask SendBytesAsync(byte[] bytes)
    {
        if (_disposed != 0) return;
        await _sendChannel.Writer.WriteAsync(bytes, _cts.Token);
    }

    /// <summary>
    /// Runs the send loop, writing queued messages to the WebSocket.
    /// </summary>
    public async Task RunSendLoopAsync()
    {
        try
        {
            await foreach (var bytes in _sendChannel.Reader.ReadAllAsync(_cts.Token))
            {
                if (_webSocket.State != WebSocketState.Open) break;

                await _webSocket.SendAsync(
                    new ArraySegment<byte>(bytes),
                    SysWebSocketMessageType.Text,
                    endOfMessage: true,
                    _cts.Token);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown
        }
        catch (WebSocketException ex) when (ex.WebSocketErrorCode == WebSocketError.ConnectionClosedPrematurely)
        {
            _logger.LogDebug("WebSocket connection closed prematurely for session {SessionId}", SessionId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in send loop for session {SessionId}", SessionId);
        }
    }

    /// <summary>
    /// Receives a message from the WebSocket.
    /// </summary>
    public async Task<(SysWebSocketMessageType messageType, byte[]? data)> ReceiveAsync(byte[] buffer)
    {
        var result = await _webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), _cts.Token);
        _lastActivity = DateTimeOffset.UtcNow;

        if (result.MessageType == SysWebSocketMessageType.Close)
        {
            return (SysWebSocketMessageType.Close, null);
        }

        if (result.MessageType == SysWebSocketMessageType.Text || result.MessageType == SysWebSocketMessageType.Binary)
        {
            // Handle multi-part messages
            if (!result.EndOfMessage)
            {
                using var ms = new MemoryStream();
                ms.Write(buffer, 0, result.Count);

                while (!result.EndOfMessage)
                {
                    result = await _webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), _cts.Token);
                    ms.Write(buffer, 0, result.Count);
                }

                return (result.MessageType, ms.ToArray());
            }

            var data = new byte[result.Count];
            Array.Copy(buffer, data, result.Count);
            return (result.MessageType, data);
        }

        return (result.MessageType, null);
    }

    /// <summary>
    /// Updates the last activity timestamp.
    /// </summary>
    public void Touch()
    {
        _lastActivity = DateTimeOffset.UtcNow;
    }

    /// <summary>
    /// Closes the WebSocket connection gracefully.
    /// </summary>
    public async Task CloseAsync(WebSocketCloseStatus status = WebSocketCloseStatus.NormalClosure, string? description = null)
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        _sendChannel.Writer.TryComplete();
        await _cts.CancelAsync();

        if (_webSocket.State == WebSocketState.Open || _webSocket.State == WebSocketState.CloseReceived)
        {
            try
            {
                using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await _webSocket.CloseAsync(status, description ?? "Session closed", timeoutCts.Token);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error closing WebSocket for session {SessionId}", SessionId);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAsync();
        _cts.Dispose();
    }
}
