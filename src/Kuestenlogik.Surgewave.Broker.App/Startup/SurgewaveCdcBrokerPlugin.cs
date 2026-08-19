using Kuestenlogik.Surgewave.Cdc;
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Startup;

/// <summary>
/// <see cref="IBrokerPlugin"/> that hosts the CDC (Change Data Capture) service
/// inside the broker: <see cref="CdcService"/> as background service plus the
/// REST API at <c>/api/cdc/sources</c> (create/list/status/delete), which the
/// Control UI's CDC page consumes.
/// Enabled via <c>Surgewave:Cdc:Enabled=true</c>; sources are added at runtime
/// through the REST API with their own connection configuration.
/// </summary>
public sealed class SurgewaveCdcBrokerPlugin : IBrokerPlugin
{
    /// <inheritdoc />
    public string FeatureId => "Surgewave.Cdc";

    /// <inheritdoc />
    public string DisplayName => "CDC (PostgreSQL)";

    /// <inheritdoc />
    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>("Surgewave:Cdc:Enabled", false);

    /// <inheritdoc />
    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        services.AddSingleton(sp =>
        {
            var cdcConfig = new CdcConfig();
            configuration.GetSection(CdcConfig.SectionName).Bind(cdcConfig);
            return cdcConfig;
        });
        services.AddSingleton<CdcService>();
        services.AddHostedService(sp => sp.GetRequiredService<CdcService>());
    }

    /// <inheritdoc />
    public void Configure(object host, IServiceProvider services)
    {
        var app = (WebApplication)host;
        app.MapSurgewaveCdc(services.GetRequiredService<CdcService>());
        app.Logger.LogInformation("CDC REST API mapped at /api/cdc/sources");
    }
}
