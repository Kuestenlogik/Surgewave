using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Reassignment;
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.CruiseControl;

/// <summary>
/// <see cref="IBrokerPlugin"/> that activates the Cruise Control auto-balance service.
/// Discovered automatically by <see cref="Plugins.BrokerPluginActivator"/> at broker
/// startup when the feature is enabled via <c>Surgewave:CruiseControl:Enabled=true</c>.
///
/// <para>
/// All dependencies (<see cref="ClusterState"/>, <see cref="ReassignmentPlanner"/>,
/// <see cref="ReassignmentExecutor"/>) are resolved from DI in <see cref="Configure"/>.
/// They were migrated from local variables in <c>Program.cs</c> to factory singletons
/// so that this plugin — and any future plugin that needs the reassignment
/// infrastructure — can access them through the standard <see cref="IServiceProvider"/>.
/// </para>
/// </summary>
public sealed class SurgewaveCruiseControlBrokerPlugin : IBrokerPlugin
{
    /// <inheritdoc />
    public string FeatureId => "Surgewave.CruiseControl";

    /// <inheritdoc />
    public string DisplayName => "Cruise Control";

    /// <inheritdoc />
    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>("Surgewave:CruiseControl:Enabled", false);

    /// <inheritdoc />
    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<CruiseControlConfig>(configuration.GetSection(CruiseControlConfig.SectionName));
    }

    /// <inheritdoc />
    public void Configure(object host, IServiceProvider services)
    {
        var app = (WebApplication)host;
        var config = services.GetRequiredService<BrokerConfig>();
        var clusterState = services.GetRequiredService<ClusterState>();
        var reassignmentPlanner = services.GetRequiredService<ReassignmentPlanner>();
        var reassignmentExecutor = services.GetRequiredService<ReassignmentExecutor>();
        var programLogger = services.GetRequiredService<ILogger<Program>>();
        var ccLogger = services.GetRequiredService<ILogger<CruiseControlService>>();

        var loadCollector = new LoadCollector(clusterState);
        var balanceCalculator = new BalanceCalculator();

#pragma warning disable CA2000 // Lifecycle managed by the application (runs until broker shutdown)
        var service = new CruiseControlService(
            config.CruiseControl, loadCollector, balanceCalculator,
            reassignmentPlanner, reassignmentExecutor, ccLogger);
#pragma warning restore CA2000

        _ = service.StartAsync(app.Lifetime.ApplicationStopping);

        programLogger.LogInformation("Cruise Control enabled (mode={Mode}, interval={Interval}s)",
            config.CruiseControl.Mode, config.CruiseControl.AnalysisIntervalSeconds);

        app.MapCruiseControl(service);
        programLogger.LogInformation("  - Cruise Control API:  {Host}:{GrpcPort}/api/cruise-control",
            config.Host, config.GrpcPort);
    }
}
