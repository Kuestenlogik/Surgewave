using System.Net.WebSockets;

namespace Kuestenlogik.Surgewave.Gateway.WebSocket;

/// <summary>
/// Middleware for handling WebSocket upgrade requests.
/// Routes:
///   /ws - Default cluster
///   /ws/clusters/{clusterId} - Specific cluster
/// </summary>
public sealed class WebSocketMiddleware
{
    private readonly RequestDelegate _next;
    private readonly GatewayConfig _config;
    private readonly ClusterRegistry _clusterRegistry;
    private readonly WebSocketSessionManager _sessionManager;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<WebSocketMiddleware> _logger;

    public WebSocketMiddleware(
        RequestDelegate next,
        GatewayConfig config,
        ClusterRegistry clusterRegistry,
        WebSocketSessionManager sessionManager,
        IServiceProvider serviceProvider,
        ILogger<WebSocketMiddleware> logger)
    {
        _next = next;
        _config = config;
        _clusterRegistry = clusterRegistry;
        _sessionManager = sessionManager;
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        if (!context.WebSockets.IsWebSocketRequest)
        {
            await _next(context);
            return;
        }

        if (!_config.WebSocket.Enabled)
        {
            context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            return;
        }

        // Extract cluster ID from route
        var clusterId = ExtractClusterId(context.Request.Path);

        // Validate cluster exists
        if (!_clusterRegistry.TryGetClient(clusterId, out _))
        {
            _logger.LogWarning("WebSocket connection attempt to unknown cluster: {ClusterId}", clusterId);
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        var effectiveClusterId = string.IsNullOrEmpty(clusterId)
            ? _clusterRegistry.DefaultClusterId
            : clusterId;

        _logger.LogDebug("Accepting WebSocket connection for cluster {ClusterId}", effectiveClusterId);

        using var webSocket = await context.WebSockets.AcceptWebSocketAsync();

        var sessionLogger = _serviceProvider.GetRequiredService<ILogger<WebSocketSession>>();
        var session = new WebSocketSession(
            webSocket,
            effectiveClusterId,
            context.Connection.RemoteIpAddress?.ToString(),
            _config.WebSocket,
            sessionLogger);

        _sessionManager.AddSession(session);

        try
        {
            var handler = _serviceProvider.GetRequiredService<WebSocketHandler>();
            await handler.HandleAsync(session);
        }
        catch (WebSocketException ex) when (ex.WebSocketErrorCode == WebSocketError.ConnectionClosedPrematurely)
        {
            _logger.LogDebug("WebSocket connection closed prematurely for session {SessionId}", session.SessionId);
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("WebSocket session {SessionId} was cancelled", session.SessionId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error handling WebSocket session {SessionId}", session.SessionId);
        }
        finally
        {
            _sessionManager.RemoveSession(session.SessionId);
            await session.DisposeAsync();
        }
    }

    private static string? ExtractClusterId(PathString path)
    {
        var pathValue = path.Value ?? string.Empty;

        // /ws/clusters/{clusterId}
        if (pathValue.StartsWith("/ws/clusters/", StringComparison.OrdinalIgnoreCase))
        {
            var clusterId = pathValue["/ws/clusters/".Length..];
            // Remove trailing slash if present
            return clusterId.TrimEnd('/');
        }

        // /ws - use default cluster
        return null;
    }
}

/// <summary>
/// Extension methods for WebSocket middleware.
/// </summary>
public static class WebSocketMiddlewareExtensions
{
    /// <summary>
    /// Adds WebSocket middleware to the pipeline.
    /// </summary>
    public static IApplicationBuilder UseSurgewaveWebSocket(this IApplicationBuilder app)
    {
        return app.UseMiddleware<WebSocketMiddleware>();
    }
}
