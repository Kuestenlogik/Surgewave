namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// WebSocket service for real-time communication with Surgewave Gateway.
/// </summary>
public interface ISurgewaveWebSocketService : IAsyncDisposable
{
    /// <summary>
    /// Whether the WebSocket is currently connected.
    /// </summary>
    bool IsConnected { get; }

    /// <summary>
    /// Connect to the WebSocket endpoint.
    /// </summary>
    Task ConnectAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Disconnect from the WebSocket.
    /// </summary>
    Task DisconnectAsync();

    /// <summary>
    /// Subscribe to a topic for real-time messages.
    /// </summary>
    Task SubscribeAsync(string topic, string? groupId = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Unsubscribe from a topic.
    /// </summary>
    Task UnsubscribeAsync(string topic, CancellationToken cancellationToken = default);

    /// <summary>
    /// Send a ping and wait for pong.
    /// </summary>
    Task<bool> PingAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Event raised when a message is received.
    /// </summary>
    event EventHandler<WebSocketMessageEventArgs>? MessageReceived;

    /// <summary>
    /// Event raised when the connection state changes.
    /// </summary>
    event EventHandler<bool>? ConnectionStateChanged;
}

/// <summary>
/// Event args for WebSocket messages.
/// </summary>
public class WebSocketMessageEventArgs : EventArgs
{
    public required string Type { get; init; }
    public required string Topic { get; init; }
    public string? Key { get; init; }
    public string? Value { get; init; }
    public long Offset { get; init; }
    public long Timestamp { get; init; }
}
