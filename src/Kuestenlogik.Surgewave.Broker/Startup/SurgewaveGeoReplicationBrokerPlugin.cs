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
/// <see cref="ClusterLinkManager"/>. Opt-in via <c>Surgewave:GeoReplicationEnabled=true</c>.
/// Statically configured <c>Surgewave:ClusterLinks</c> entries are connected at startup;
/// additional links can be created at runtime through the management REST API
/// (<c>/api/cluster-links</c>, see <see cref="ClusterLinkRestApi"/>), so an empty
/// link list is valid.
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
        var app = (WebApplication)host;
        var config = services.GetRequiredService<BrokerConfig>();

        var logManager = services.GetRequiredService<LogManager>();
        var metrics = services.GetRequiredService<BrokerMetrics>();
        var peerTransport = services.GetRequiredService<Kuestenlogik.Surgewave.Transport.IPeerTransport>();
        var loggerFactory = services.GetRequiredService<ILoggerFactory>();
        var clusterLinkLogger = loggerFactory.CreateLogger<ClusterLinkManager>();
        var programLogger = services.GetRequiredService<ILogger<Program>>();

#pragma warning disable CA2000 // Lifecycle managed by the application
        var clusterLinkManager = new ClusterLinkManager(logManager, peerTransport, metrics, clusterLinkLogger);
#pragma warning restore CA2000

        // No ">= 1 ClusterLinks" gate anymore: links can be created at runtime via the
        // REST API, so the flag alone activates the engine — an empty list is fine
        // (StartAsync just spins up the metadata sync loop without connecting anywhere).
        var initialLinks = config.ClusterLinks ?? [];
        await clusterLinkManager.StartAsync(initialLinks, CancellationToken.None);
        programLogger.LogInformation("Geo-replication started with {LinkCount} cluster links", initialLinks.Length);

        app.MapSurgewaveClusterLinks(clusterLinkManager);
        programLogger.LogInformation("  - Cluster Links API:   {Host}:{GrpcPort}/api/cluster-links",
            config.Host, config.GrpcPort);
    }
}
