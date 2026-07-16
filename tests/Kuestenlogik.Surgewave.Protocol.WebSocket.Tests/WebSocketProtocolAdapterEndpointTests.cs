using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.AspNetCore.Http;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket.Tests;

/// <summary>
/// Pins <see cref="WebSocketProtocolAdapter.MapEndpoints"/> route registration and the shared
/// request guards of all three endpoints: non-WebSocket requests are rejected with 400,
/// exhausted connection limits with 503, and a missing topic route value with 400.
/// </summary>
public sealed class WebSocketProtocolAdapterEndpointTests : IDisposable
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(15);

    private readonly LogManager _logManager = WebSocketAdapterTestHost.CreateInMemoryLogManager();

    public void Dispose() => _logManager.Dispose();

    [Fact]
    public void MapEndpoints_TrimsTrailingSlashAndRegistersAllThreeRoutes()
    {
        var adapter = WebSocketAdapterTestHost.CreateAdapter(_logManager, new WebSocketConfig { Path = "/stream/" });

        var endpoints = WebSocketAdapterTestHost.MapEndpoints(adapter);

        var patterns = endpoints.Select(e => e.RoutePattern.RawText).ToList();
        Assert.Equal(3, patterns.Count);
        Assert.Contains("/stream/produce/{topic}", patterns);
        Assert.Contains("/stream/consume/{topic}", patterns);
        Assert.Contains("/stream/subscribe", patterns);
    }

    [Fact]
    public void ActiveConnections_StartsAtZero()
    {
        var adapter = WebSocketAdapterTestHost.CreateAdapter(_logManager);

        Assert.Equal(0, adapter.ActiveConnections);
    }

    [Theory]
    [InlineData("/produce/")]
    [InlineData("/consume/")]
    [InlineData("/subscribe")]
    public async Task NonWebSocketRequest_IsRejectedWith400(string routeFragment)
    {
        var adapter = WebSocketAdapterTestHost.CreateAdapter(_logManager);
        var endpoint = WebSocketAdapterTestHost.GetEndpoint(WebSocketAdapterTestHost.MapEndpoints(adapter), routeFragment);
        var context = WebSocketAdapterTestHost.CreateHttpContext();

        await endpoint(context).WaitAsync(TestTimeout);

        Assert.Equal(StatusCodes.Status400BadRequest, context.Response.StatusCode);
        Assert.Equal("WebSocket connection required", WebSocketAdapterTestHost.ReadResponseBody(context));
    }

    [Theory]
    [InlineData("/produce/")]
    [InlineData("/consume/")]
    [InlineData("/subscribe")]
    public async Task ConnectionLimitReached_IsRejectedWith503(string routeFragment)
    {
        // MaxConnections = 0 puts the adapter at its limit before the first connection.
        var adapter = WebSocketAdapterTestHost.CreateAdapter(_logManager, new WebSocketConfig { MaxConnections = 0 });
        var endpoint = WebSocketAdapterTestHost.GetEndpoint(WebSocketAdapterTestHost.MapEndpoints(adapter), routeFragment);
        var context = WebSocketAdapterTestHost.CreateWebSocketContext(new ScriptedWebSocket(), topic: "any");

        await endpoint(context).WaitAsync(TestTimeout);

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, context.Response.StatusCode);
        Assert.Equal("Maximum WebSocket connections reached", WebSocketAdapterTestHost.ReadResponseBody(context));
    }

    [Theory]
    [InlineData("/produce/")]
    [InlineData("/consume/")]
    public async Task MissingTopicRouteValue_IsRejectedWith400(string routeFragment)
    {
        var adapter = WebSocketAdapterTestHost.CreateAdapter(_logManager);
        var endpoint = WebSocketAdapterTestHost.GetEndpoint(WebSocketAdapterTestHost.MapEndpoints(adapter), routeFragment);
        var context = WebSocketAdapterTestHost.CreateWebSocketContext(new ScriptedWebSocket());

        await endpoint(context).WaitAsync(TestTimeout);

        Assert.Equal(StatusCodes.Status400BadRequest, context.Response.StatusCode);
        Assert.Equal("Topic name is required", WebSocketAdapterTestHost.ReadResponseBody(context));
        Assert.Equal(0, adapter.ActiveConnections);
    }
}
