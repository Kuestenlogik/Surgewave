using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MyProtocol;

/// <summary>
/// Surgewave protocol adapter plugin. Exposes a wire-format-specific entry
/// point (TCP listener, HTTP route, …). Adapters that share the HTTP port
/// return <c>0</c> from <see cref="DefaultPort"/>.
/// </summary>
public sealed class MyProtocolPlugin : IProtocolPlugin
{
    public string FeatureId => "FEATURE_ID";
    public string DisplayName => "MyProtocol";
    public int DefaultPort => DEFAULT_PORT;

    public bool IsConfigEnabled(IConfiguration configuration) =>
        configuration.GetValue<bool>("MyProtocol:Enabled", defaultValue: true);

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        // TODO: register your protocol's services (decoders, hosted listeners, ...) here.
    }

    public void Configure(object host, IServiceProvider services)
    {
        // TODO: configure middleware / endpoints if you piggyback on the HTTP host.
    }
}
