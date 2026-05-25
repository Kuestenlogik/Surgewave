using Kuestenlogik.Surgewave.Clustering.GeoReplication;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Startup;

/// <summary>
/// <see cref="IBrokerPlugin"/> that activates built-in geo-replication via
/// <see cref="ClusterLinkManager"/>. Opt-in via <c>Surgewave:GeoReplicationEnabled=true</c>
/// plus at least one <c>Surgewave:ClusterLinks</c> entry.
/// </summary>
public sealed class SurgewaveGeoReplicationBrokerPlugin : IBrokerPlugin
{
    /// <inheritdoc />
    public string FeatureId => "Surgewave.GeoReplication";

    /// <inheritdoc />
    public string DisplayName => "Geo-Replication";

    /// <inheritdoc />
    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>("Surgewave:GeoReplicationEnabled", false);

    /// <inheritdoc />
    public void ConfigureServices(IServiceCollection services, IConfiguration configuration) { }

    /// <inheritdoc />
    public async Task ConfigureAsync(object host, IServiceProvider services)
    {
        var config = services.GetRequiredService<BrokerConfig>();
        if (config.ClusterLinks is not { Length: > 0 }) return;

        var logManager = services.GetRequiredService<LogManager>();
        var metrics = services.GetRequiredService<BrokerMetrics>();
        var peerTransport = services.GetRequiredService<Kuestenlogik.Surgewave.Transport.IPeerTransport>();
        var loggerFactory = services.GetRequiredService<ILoggerFactory>();
        var clusterLinkLogger = loggerFactory.CreateLogger<ClusterLinkManager>();
        var programLogger = services.GetRequiredService<ILogger<Program>>();

#pragma warning disable CA2000 // Lifecycle managed by the application
        var clusterLinkManager = new ClusterLinkManager(logManager, peerTransport, metrics, clusterLinkLogger);
#pragma warning restore CA2000
        await clusterLinkManager.StartAsync(config.ClusterLinks, CancellationToken.None);
        programLogger.LogInformation("Geo-replication started with {LinkCount} cluster links", config.ClusterLinks.Length);
    }
}
