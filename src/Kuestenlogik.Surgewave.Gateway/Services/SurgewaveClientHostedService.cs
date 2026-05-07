namespace Kuestenlogik.Surgewave.Gateway.Services;

/// <summary>
/// Hosted service that manages the Surgewave cluster connections lifecycle.
/// </summary>
public sealed class SurgewaveClientHostedService : IHostedService
{
    private readonly ClusterRegistry _registry;
    private readonly ILogger<SurgewaveClientHostedService> _logger;

    public SurgewaveClientHostedService(ClusterRegistry registry, ILogger<SurgewaveClientHostedService> logger)
    {
        _registry = registry;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Connecting to Surgewave clusters...");
        await _registry.ConnectAllAsync(cancellationToken);
        _logger.LogInformation("Connected to all Surgewave clusters");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Disposing Surgewave cluster connections...");
        await _registry.DisposeAllAsync();
        _logger.LogInformation("Disposed all Surgewave cluster connections");
    }
}
