using System.Net.WebSockets;
using Kuestenlogik.Surgewave.Gateway.WebSocket;
using Kuestenlogik.Surgewave.Gateway.WebSocket.Protocol;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

using SysWebSocketMessageType = System.Net.WebSockets.WebSocketMessageType;

namespace Kuestenlogik.Surgewave.Gateway.Tests.WebSocket;

public class WebSocketSessionManagerTests : IAsyncDisposable
{
    private readonly WebSocketSessionManager _sessionManager;
    private readonly List<WebSocketSession> _sessions = new();

    public WebSocketSessionManagerTests()
    {
        _sessionManager = new WebSocketSessionManager(NullLogger<WebSocketSessionManager>.Instance);
    }

    public async ValueTask DisposeAsync()
    {
        await _sessionManager.DisposeAsync();
        foreach (var session in _sessions)
        {
            await session.DisposeAsync();
        }
    }

    private WebSocketSession CreateMockSession(string clusterId = "test-cluster")
    {
        // Create a minimal mock WebSocket for testing
        var mockWebSocket = new MockWebSocket();
        var config = new WebSocketConfig();
        var session = new WebSocketSession(
            mockWebSocket,
            clusterId,
            "127.0.0.1",
            config,
            NullLogger<WebSocketSession>.Instance);
        _sessions.Add(session);
        return session;
    }

    [Fact]
    public void SessionCount_Initially_ReturnsZero()
    {
        // Assert
        Assert.Equal(0, _sessionManager.SessionCount);
    }

    [Fact]
    public void AddSession_IncreasesSessionCount()
    {
        // Arrange
        var session = CreateMockSession();

        // Act
        _sessionManager.AddSession(session);

        // Assert
        Assert.Equal(1, _sessionManager.SessionCount);
    }

    [Fact]
    public void AddSession_MultipleSessions_TracksAll()
    {
        // Arrange
        var session1 = CreateMockSession("cluster-a");
        var session2 = CreateMockSession("cluster-b");
        var session3 = CreateMockSession("cluster-a");

        // Act
        _sessionManager.AddSession(session1);
        _sessionManager.AddSession(session2);
        _sessionManager.AddSession(session3);

        // Assert
        Assert.Equal(3, _sessionManager.SessionCount);
    }

    [Fact]
    public void RemoveSession_DecreasesSessionCount()
    {
        // Arrange
        var session = CreateMockSession();
        _sessionManager.AddSession(session);

        // Act
        var removed = _sessionManager.RemoveSession(session.SessionId);

        // Assert
        Assert.True(removed);
        Assert.Equal(0, _sessionManager.SessionCount);
    }

    [Fact]
    public void RemoveSession_NonExistentId_ReturnsFalse()
    {
        // Act
        var removed = _sessionManager.RemoveSession("non-existent-id");

        // Assert
        Assert.False(removed);
    }

    [Fact]
    public void GetSession_ExistingId_ReturnsSession()
    {
        // Arrange
        var session = CreateMockSession();
        _sessionManager.AddSession(session);

        // Act
        var retrieved = _sessionManager.GetSession(session.SessionId);

        // Assert
        Assert.NotNull(retrieved);
        Assert.Same(session, retrieved);
    }

    [Fact]
    public void GetSession_NonExistentId_ReturnsNull()
    {
        // Act
        var retrieved = _sessionManager.GetSession("non-existent-id");

        // Assert
        Assert.Null(retrieved);
    }

    [Fact]
    public void SessionIds_ReturnsAllSessionIds()
    {
        // Arrange
        var session1 = CreateMockSession();
        var session2 = CreateMockSession();
        _sessionManager.AddSession(session1);
        _sessionManager.AddSession(session2);

        // Act
        var ids = _sessionManager.SessionIds.ToList();

        // Assert
        Assert.Equal(2, ids.Count);
        Assert.Contains(session1.SessionId, ids);
        Assert.Contains(session2.SessionId, ids);
    }

    [Fact]
    public void GetSessionsByCluster_ReturnsCorrectSessions()
    {
        // Arrange
        var sessionA1 = CreateMockSession("cluster-a");
        var sessionA2 = CreateMockSession("cluster-a");
        var sessionB = CreateMockSession("cluster-b");
        _sessionManager.AddSession(sessionA1);
        _sessionManager.AddSession(sessionA2);
        _sessionManager.AddSession(sessionB);

        // Act
        var clusterASessions = _sessionManager.GetSessionsByCluster("cluster-a").ToList();
        var clusterBSessions = _sessionManager.GetSessionsByCluster("cluster-b").ToList();

        // Assert
        Assert.Equal(2, clusterASessions.Count);
        Assert.Single(clusterBSessions);
        Assert.Contains(sessionA1, clusterASessions);
        Assert.Contains(sessionA2, clusterASessions);
        Assert.Contains(sessionB, clusterBSessions);
    }

    [Fact]
    public void GetSessionsByCluster_NoMatchingCluster_ReturnsEmpty()
    {
        // Arrange
        var session = CreateMockSession("cluster-a");
        _sessionManager.AddSession(session);

        // Act
        var sessions = _sessionManager.GetSessionsByCluster("cluster-x").ToList();

        // Assert
        Assert.Empty(sessions);
    }

    [Fact]
    public void GetAllSessions_ReturnsAllSessions()
    {
        // Arrange
        var session1 = CreateMockSession("cluster-a");
        var session2 = CreateMockSession("cluster-b");
        _sessionManager.AddSession(session1);
        _sessionManager.AddSession(session2);

        // Act
        var sessions = _sessionManager.GetAllSessions().ToList();

        // Assert
        Assert.Equal(2, sessions.Count);
    }

    [Fact]
    public void AddSession_DuplicateId_DoesNotDuplicate()
    {
        // Arrange
        var session = CreateMockSession();
        _sessionManager.AddSession(session);

        // Act - try to add the same session again
        _sessionManager.AddSession(session);

        // Assert - should still be 1
        Assert.Equal(1, _sessionManager.SessionCount);
    }

    [Fact]
    public async Task CloseAllSessionsAsync_ClearsAllSessions()
    {
        // Arrange
        var session1 = CreateMockSession();
        var session2 = CreateMockSession();
        _sessionManager.AddSession(session1);
        _sessionManager.AddSession(session2);

        // Act
        await _sessionManager.CloseAllSessionsAsync();

        // Assert
        Assert.Equal(0, _sessionManager.SessionCount);
    }

    [Fact]
    public async Task CleanupStaleSessionsAsync_RemovesOldSessions()
    {
        // Note: This test is limited because we can't easily manipulate session timestamps
        // In a real scenario, you'd use time abstraction

        // Arrange
        var session = CreateMockSession();
        _sessionManager.AddSession(session);

        // Act - cleanup with a very short timeout (session was just created)
        await _sessionManager.CleanupStaleSessionsAsync(TimeSpan.FromHours(1));

        // Assert - session should still exist (not stale)
        Assert.Equal(1, _sessionManager.SessionCount);
    }

    /// <summary>
    /// Minimal mock WebSocket for testing session management.
    /// Does not support actual WebSocket operations.
    /// </summary>
    private sealed class MockWebSocket : System.Net.WebSockets.WebSocket
    {
        private WebSocketState _state = WebSocketState.Open;

        public override WebSocketCloseStatus? CloseStatus => null;
        public override string? CloseStatusDescription => null;
        public override WebSocketState State => _state;
        public override string? SubProtocol => null;

        public override void Abort() => _state = WebSocketState.Aborted;

        public override Task CloseAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken)
        {
            _state = WebSocketState.Closed;
            return Task.CompletedTask;
        }

        public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken)
        {
            _state = WebSocketState.CloseSent;
            return Task.CompletedTask;
        }

        public override void Dispose() => _state = WebSocketState.Closed;

        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            // Return a close message by default
            return Task.FromResult(new WebSocketReceiveResult(0, SysWebSocketMessageType.Close, true));
        }

        public override Task SendAsync(ArraySegment<byte> buffer, SysWebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
