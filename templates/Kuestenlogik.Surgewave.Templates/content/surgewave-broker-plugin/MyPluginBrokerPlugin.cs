using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MyPlugin;

/// <summary>
/// Surgewave broker plugin entry point. Discovered automatically when the
/// .swpkg is dropped into the broker's plugins/ folder. Register your
/// services in <see cref="ConfigureServices"/>; configure middleware /
/// endpoints in <see cref="Configure"/>.
/// </summary>
public sealed class MyPluginBrokerPlugin : IBrokerPlugin
{
    public string FeatureId => "FEATURE_ID";
    public string DisplayName => "MyPlugin";

    public bool IsConfigEnabled(IConfiguration configuration) =>
        configuration.GetValue<bool>("MyPlugin:Enabled", defaultValue: true);

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        // TODO: register your services here.
    }

    public void Configure(object host, IServiceProvider services)
    {
        // TODO: configure middleware / endpoints here. Cast `host` to
        // WebApplication when you need MapGet/MapPost etc.
    }
}
