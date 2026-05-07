namespace Kuestenlogik.Surgewave.Gateway.WebSocket;

/// <summary>
/// Background service that periodically cleans up stale WebSocket sessions.
/// </summary>
public sealed class WebSocketCleanupService : BackgroundService
{
    private readonly WebSocketSessionManager _sessionManager;
    private readonly GatewayConfig _config;
    private readonly ILogger<WebSocketCleanupService> _logger;

    public WebSocketCleanupService(
        WebSocketSessionManager sessionManager,
        GatewayConfig config,
        ILogger<WebSocketCleanupService> logger)
    {
        _sessionManager = sessionManager;
        _config = config;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_config.WebSocket.Enabled)
        {
            _logger.LogInformation("WebSocket support is disabled, cleanup service not starting");
            return;
        }

        _logger.LogInformation(
            "WebSocket cleanup service started, session timeout: {Timeout}ms",
            _config.WebSocket.SessionTimeoutMs);

        var cleanupInterval = TimeSpan.FromMilliseconds(_config.WebSocket.SessionTimeoutMs / 2);
        var sessionTimeout = TimeSpan.FromMilliseconds(_config.WebSocket.SessionTimeoutMs);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(cleanupInterval, stoppingToken);
                await _sessionManager.CleanupStaleSessionsAsync(sessionTimeout);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Expected on shutdown
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in WebSocket cleanup service");
            }
        }

        _logger.LogInformation("WebSocket cleanup service stopped");
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Closing all WebSocket sessions...");
        await _sessionManager.CloseAllSessionsAsync();
        await base.StopAsync(cancellationToken);
    }
}
