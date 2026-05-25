using Kuestenlogik.Surgewave.Broker.IntentConfig;
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.AutoTuning;

/// <summary>
/// <see cref="IBrokerPlugin"/> that activates the Auto-Tuning background service.
/// Discovered automatically by <see cref="Plugins.BrokerPluginActivator"/> at broker
/// startup when the feature is enabled via <c>Surgewave:AutoTuning:Enabled=true</c>.
///
/// <para>
/// The plugin resolves <see cref="BrokerConfig"/>, <see cref="DynamicBrokerConfig"/> and
/// <see cref="BrokerMetrics"/> from DI (all registered as singletons in <c>Program.cs</c>
/// before <c>app.Build()</c>), constructs the <see cref="AutoTuningService"/>, starts its
/// background analysis loop, and maps the REST API (<c>/api/auto-tuning</c>).
/// </para>
///
/// <para>
/// This is the first broker-internal feature to be fully promoted from a hardcoded
/// <c>if (config.X.Enabled)</c> block in <c>Program.cs</c> into a discoverable
/// <c>IBrokerPlugin</c>. The refactoring pattern:
/// <list type="number">
///   <item><description>Register the feature's DI dependencies as singletons before
///     <c>app.Build()</c> (done in <c>Program.cs</c>).</description></item>
///   <item><description>Let <c>BrokerPluginActivator.ActivatePlugins</c> discover and
///     call <see cref="ConfigureServices"/>.</description></item>
///   <item><description>Let <c>BrokerPluginActivator.ConfigureBrokerPlugins</c> call
///     <see cref="Configure"/> after <c>app.Build()</c>, where DI is fully populated
///     and the plugin can resolve everything it needs.</description></item>
/// </list>
/// </para>
/// </summary>
public sealed class SurgewaveAutoTuningBrokerPlugin : IBrokerPlugin
{
    /// <inheritdoc />
    public string FeatureId => "Surgewave.AutoTuning";

    /// <inheritdoc />
    public string DisplayName => "Auto-Tuning";

    /// <inheritdoc />
    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>("Surgewave:AutoTuning:Enabled", false);

    /// <inheritdoc />
    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<AutoTuningConfig>(configuration.GetSection(AutoTuningConfig.SectionName));
    }

    /// <inheritdoc />
    public void Configure(object host, IServiceProvider services)
    {
        var app = (WebApplication)host;
        var config = services.GetRequiredService<BrokerConfig>();
        var dynamicConfig = services.GetRequiredService<DynamicBrokerConfig>();
        var brokerMetrics = services.GetRequiredService<BrokerMetrics>();
        var logger = services.GetRequiredService<ILogger<AutoTuningService>>();
        var programLogger = services.GetRequiredService<ILogger<Program>>();

#pragma warning disable CA2000 // Lifecycle managed by the application (runs until broker shutdown)
        var service = new AutoTuningService(config.AutoTuning, config, dynamicConfig, brokerMetrics, logger);
#pragma warning restore CA2000
        _ = service.StartAsync(CancellationToken.None);

        programLogger.LogInformation("Auto-tuning enabled (mode={Mode}, interval={Interval}s)",
            config.AutoTuning.Mode, config.AutoTuning.AnalysisIntervalSeconds);

        app.MapAutoTuning(service);
        programLogger.LogInformation("  - Auto-Tuning API:     {Host}:{GrpcPort}/api/auto-tuning",
            config.Host, config.GrpcPort);
    }
}
