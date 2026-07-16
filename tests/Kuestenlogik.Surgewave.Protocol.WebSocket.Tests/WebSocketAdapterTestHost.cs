using System.Text.Json;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket.Tests;

/// <summary>
/// Shared plumbing for <see cref="WebSocketProtocolAdapter"/> tests: in-memory LogManager,
/// adapter construction, endpoint mapping via <see cref="TestEndpointRouteBuilder"/>, and
/// HttpContext factories for invoking the mapped request delegates directly.
/// </summary>
internal static class WebSocketAdapterTestHost
{
    public static LogManager CreateInMemoryLogManager()
        => new(
            Path.Combine(Path.GetTempPath(), $"surgewave-ws-tests-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());

    public static WebSocketProtocolAdapter CreateAdapter(LogManager logManager, WebSocketConfig? config = null)
        => new(
            Options.Create(config ?? new WebSocketConfig()),
            logManager,
            NullLogger<WebSocketProtocolAdapter>.Instance);

    /// <summary>Maps the adapter's endpoints into a standalone route builder and returns them.</summary>
    public static IReadOnlyList<RouteEndpoint> MapEndpoints(WebSocketProtocolAdapter adapter)
    {
        var builder = new TestEndpointRouteBuilder(new ServiceCollection().BuildServiceProvider());
        adapter.MapEndpoints(builder);
        return builder.DataSources.SelectMany(source => source.Endpoints).OfType<RouteEndpoint>().ToList();
    }

    /// <summary>Finds the request delegate of the endpoint whose route contains the given fragment.</summary>
    public static RequestDelegate GetEndpoint(IReadOnlyList<RouteEndpoint> endpoints, string pathFragment)
    {
        var endpoint = endpoints.Single(e => e.RoutePattern.RawText?.Contains(pathFragment, StringComparison.Ordinal) == true);
        return endpoint.RequestDelegate!;
    }

    /// <summary>Plain (non-WebSocket) request context with a readable response body.</summary>
    public static DefaultHttpContext CreateHttpContext()
    {
        var context = new DefaultHttpContext();
        context.Response.Body = new MemoryStream();
        return context;
    }

    /// <summary>Request context that upgrades to the given scripted socket, optionally carrying a topic route value.</summary>
    public static DefaultHttpContext CreateWebSocketContext(ScriptedWebSocket socket, string? topic = null)
    {
        var context = CreateHttpContext();
        context.Features.Set<IHttpWebSocketFeature>(new FakeWebSocketFeature(socket));
        if (topic is not null)
        {
            context.Request.RouteValues["topic"] = topic;
        }

        return context;
    }

    public static string ReadResponseBody(HttpContext context)
    {
        context.Response.Body.Position = 0;
        using var reader = new StreamReader(context.Response.Body, leaveOpen: true);
        return reader.ReadToEnd();
    }

    public static JsonElement ParseFrame(byte[] frame)
        => JsonSerializer.Deserialize<JsonElement>(frame);
}
