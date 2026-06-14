using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Samples.BrokerPlugin;

/// <summary>
/// Reference <see cref="IBrokerPlugin"/> implementation. Registers a
/// singleton <see cref="RequestCounter"/> in the broker's DI container.
/// A real plugin would also subscribe to the broker's request pipeline
/// (e.g. via a custom <c>IKafkaRequestInterceptor</c>) to bump the
/// counter on every Produce/Fetch; this sample stops at the DI
/// registration to keep the surface area minimal.
///
/// Lifecycle this sample covers:
///  - parameterless ctor (required by the activator, satisfies SRWV004)
///  - sealed class      (satisfies SRWV001)
///  - <see cref="IsConfigEnabled"/> reads <c>SampleBrokerPlugin:Enabled</c>
///    with a default of true so the plugin loads when no config is set
///  - <see cref="ConfigureServices"/> registers the singleton
///  - <see cref="Configure"/> is left as the default no-op (no
///    endpoints, no middleware to wire up)
/// </summary>
public sealed class RequestCounterBrokerPlugin : IBrokerPlugin
{
    public string FeatureId => "Kuestenlogik.Surgewave.Samples.BrokerPlugin";
    public string DisplayName => "Sample Broker Plugin";

    public bool IsConfigEnabled(IConfiguration configuration) =>
        configuration.GetValue<bool>("SampleBrokerPlugin:Enabled", defaultValue: true);

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        services.AddSingleton<RequestCounter>();
    }
}
