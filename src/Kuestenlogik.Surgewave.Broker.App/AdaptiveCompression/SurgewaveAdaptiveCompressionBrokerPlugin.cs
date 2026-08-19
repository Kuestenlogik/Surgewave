using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.AdaptiveCompression;

/// <summary>
/// <see cref="IBrokerPlugin"/> that activates the adaptive-compression
/// background service. Discovered by <see cref="BrokerPluginActivator"/> at
/// broker startup when <c>Surgewave:AdaptiveCompression:Enabled=true</c>.
/// </summary>
public sealed class SurgewaveAdaptiveCompressionBrokerPlugin : IBrokerPlugin
{
    /// <inheritdoc/>
    public string FeatureId => "Surgewave.AdaptiveCompression";

    /// <inheritdoc/>
    public string DisplayName => "Adaptive Compression";

    /// <inheritdoc/>
    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>($"{AdaptiveCompressionConfig.SectionName}:Enabled", false);

    /// <inheritdoc/>
    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        var config = new AdaptiveCompressionConfig { Enabled = true };
        configuration.GetSection(AdaptiveCompressionConfig.SectionName).Bind(config);
        services.AddSingleton(config);
    }

    /// <inheritdoc/>
    public void Configure(object host, IServiceProvider services)
    {
        _ = (WebApplication)host;
        var config = services.GetRequiredService<AdaptiveCompressionConfig>();
        var logManager = services.GetRequiredService<LogManager>();
        var logger = services.GetRequiredService<ILogger<AdaptiveCompressionService>>();
        var programLogger = services.GetRequiredService<ILogger<Program>>();

#pragma warning disable CA2000 // Lifecycle managed by the application (runs until broker shutdown)
        var service = new AdaptiveCompressionService(config, logManager, logger);
#pragma warning restore CA2000
        _ = service.StartAsync(CancellationToken.None);

        programLogger.LogInformation(
            "Adaptive compression enabled (interval={Interval}s, maxScanBytes={MaxBytes}/partition)",
            config.ScanIntervalSeconds, config.MaxScanBytesPerPartition);
    }
}
