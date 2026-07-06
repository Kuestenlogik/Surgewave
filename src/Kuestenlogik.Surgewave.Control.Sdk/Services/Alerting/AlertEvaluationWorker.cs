using Kuestenlogik.Surgewave.Control.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Kuestenlogik.Surgewave.Control.Services.Alerting;

/// <summary>
/// Background loop that feeds the <see cref="IAlertingService"/> with fresh
/// broker state on a fixed interval — this is what makes alerts fire without
/// an open browser. Metrics and lag are fetched through the same typed clients
/// the UI uses; a scope is created per cycle because they are HttpClient-typed.
/// </summary>
public sealed class AlertEvaluationWorker : BackgroundService
{
    /// <summary>Named client (broker base address) used for the health probe.</summary>
    public const string HealthClientName = "SurgewaveApi";

    private readonly IAlertingService _alerting;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly TimeSpan _interval;
    private readonly ILogger<AlertEvaluationWorker>? _logger;

    public AlertEvaluationWorker(
        IAlertingService alerting,
        IServiceScopeFactory scopeFactory,
        TimeSpan interval,
        ILogger<AlertEvaluationWorker>? logger = null)
    {
        _alerting = alerting;
        _scopeFactory = scopeFactory;
        _interval = interval;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger?.LogInformation("Server-side alert evaluation running every {Interval}s", _interval.TotalSeconds);

        using var timer = new PeriodicTimer(_interval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                if (!_alerting.HasRules)
                    continue;

                try
                {
                    await EvaluateOnceAsync(stoppingToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger?.LogWarning(ex, "Alert evaluation cycle failed");
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown.
        }
    }

    private async Task EvaluateOnceAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var services = scope.ServiceProvider;

        var brokerReachable = await ProbeBrokerHealthAsync(
            services.GetRequiredService<IHttpClientFactory>(), cancellationToken);

        var metrics = new MetricsSnapshot();
        IReadOnlyList<ConsumerGroupLag> lags = [];
        if (brokerReachable)
        {
            metrics = await services.GetRequiredService<IMetricsClient>().GetMetricsAsync(cancellationToken);
            lags = await services.GetRequiredService<ISurgewaveApiClient>().GetAllConsumerLagsAsync(null, cancellationToken);
        }

        await _alerting.EvaluateAsync(metrics, lags, brokerReachable, cancellationToken);
    }

    private async Task<bool> ProbeBrokerHealthAsync(IHttpClientFactory httpClientFactory, CancellationToken cancellationToken)
    {
        try
        {
            var client = httpClientFactory.CreateClient(HealthClientName);
            using var response = await client.GetAsync("/health", cancellationToken);
            return response.IsSuccessStatusCode;
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            _logger?.LogDebug(ex, "Broker health probe failed");
            return false;
        }
    }
}
