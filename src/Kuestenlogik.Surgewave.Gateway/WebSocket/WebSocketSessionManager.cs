using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Gateway.WebSocket.Protocol;

namespace Kuestenlogik.Surgewave.Gateway.WebSocket;

/// <summary>
/// Manages all active WebSocket sessions.
/// </summary>
public sealed class WebSocketSessionManager : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, WebSocketSession> _sessions = new();
    private readonly ILogger<WebSocketSessionManager> _logger;

    public WebSocketSessionManager(ILogger<WebSocketSessionManager> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Gets the number of active sessions.
    /// </summary>
    public int SessionCount => _sessions.Count;

    /// <summary>
    /// Gets all active session IDs.
    /// </summary>
    public IEnumerable<string> SessionIds => _sessions.Keys;

    /// <summary>
    /// Registers a new session.
    /// </summary>
    public void AddSession(WebSocketSession session)
    {
        if (_sessions.TryAdd(session.SessionId, session))
        {
            _logger.LogInformation(
                "WebSocket session {SessionId} connected from {RemoteAddress} to cluster {ClusterId}",
                session.SessionId, session.RemoteAddress, session.ClusterId);
        }
    }

    /// <summary>
    /// Removes a session.
    /// </summary>
    /// <remarks>
    /// The session is not disposed here - the caller (middleware) is responsible
    /// for disposing the session after removal.
    /// </remarks>
#pragma warning disable CA2000 // Session disposal is handled by the caller (WebSocketMiddleware)
    public bool RemoveSession(string sessionId)
    {
        if (_sessions.TryRemove(sessionId, out var session))
        {
            _logger.LogInformation(
                "WebSocket session {SessionId} disconnected after {Duration}",
                sessionId, DateTimeOffset.UtcNow - session.ConnectedAt);
            return true;
        }
        return false;
    }
#pragma warning restore CA2000

    /// <summary>
    /// Gets a session by ID.
    /// </summary>
    public WebSocketSession? GetSession(string sessionId)
    {
        return _sessions.GetValueOrDefault(sessionId);
    }

    /// <summary>
    /// Gets all sessions for a specific cluster.
    /// </summary>
    public IEnumerable<WebSocketSession> GetSessionsByCluster(string clusterId)
    {
        return _sessions.Values.Where(s => s.ClusterId == clusterId && s.IsConnected);
    }

    /// <summary>
    /// Gets all active sessions.
    /// </summary>
    public IEnumerable<WebSocketSession> GetAllSessions()
    {
        return _sessions.Values.Where(s => s.IsConnected);
    }

    /// <summary>
    /// Broadcasts a message to all sessions.
    /// </summary>
    public async Task BroadcastAsync<T>(WebSocketMessage<T> message) where T : class
    {
        var bytes = WebSocketMessageSerializer.Serialize(message);
        var tasks = _sessions.Values
            .Where(s => s.IsConnected)
            .Select(s => s.SendBytesAsync(bytes).AsTask());

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Broadcasts a message to sessions on a specific cluster.
    /// </summary>
    public async Task BroadcastToClusterAsync<T>(string clusterId, WebSocketMessage<T> message) where T : class
    {
        var bytes = WebSocketMessageSerializer.Serialize(message);
        var tasks = _sessions.Values
            .Where(s => s.IsConnected && s.ClusterId == clusterId)
            .Select(s => s.SendBytesAsync(bytes).AsTask());

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Removes stale sessions that have exceeded the timeout.
    /// </summary>
    public async Task CleanupStaleSessionsAsync(TimeSpan timeout)
    {
        var cutoff = DateTimeOffset.UtcNow - timeout;
        var staleSessions = _sessions.Values
            .Where(s => s.LastActivity < cutoff || !s.IsConnected)
            .ToList();

        foreach (var session in staleSessions)
        {
            _logger.LogInformation(
                "Cleaning up stale WebSocket session {SessionId}, last activity: {LastActivity}",
                session.SessionId, session.LastActivity);

            if (_sessions.TryRemove(session.SessionId, out _))
            {
                await session.DisposeAsync();
            }
        }

        if (staleSessions.Count > 0)
        {
            _logger.LogInformation("Cleaned up {Count} stale WebSocket sessions", staleSessions.Count);
        }
    }

    /// <summary>
    /// Closes all sessions gracefully.
    /// </summary>
    public async Task CloseAllSessionsAsync()
    {
        _logger.LogInformation("Closing all {Count} WebSocket sessions", _sessions.Count);

        var sessions = _sessions.Values.ToList();
        _sessions.Clear();

        var closeTasks = sessions.Select(async s =>
        {
            try
            {
                await s.CloseAsync(System.Net.WebSockets.WebSocketCloseStatus.EndpointUnavailable, "Server shutting down");
                await s.DisposeAsync();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error closing session {SessionId}", s.SessionId);
            }
        });

        await Task.WhenAll(closeTasks);
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAllSessionsAsync();
    }
}
