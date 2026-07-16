using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket.Tests;

/// <summary>
/// Minimal <see cref="IHttpWebSocketFeature"/> that flags the request as a WebSocket upgrade
/// and hands out a prepared socket instead of performing a real handshake.
/// </summary>
internal sealed class FakeWebSocketFeature : IHttpWebSocketFeature
{
    private readonly System.Net.WebSockets.WebSocket _socket;

    public FakeWebSocketFeature(System.Net.WebSockets.WebSocket socket) => _socket = socket;

    public bool IsWebSocketRequest => true;

    public Task<System.Net.WebSockets.WebSocket> AcceptAsync(WebSocketAcceptContext context)
        => Task.FromResult(_socket);
}
