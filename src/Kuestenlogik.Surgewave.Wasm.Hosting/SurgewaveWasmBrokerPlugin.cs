using Kuestenlogik.Surgewave.Plugins;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Wasm;

/// <summary>
/// Broker plugin for the WebAssembly plugin subsystem.
/// </summary>
public sealed class SurgewaveWasmBrokerPlugin : IBrokerPlugin
{
    public string FeatureId => "Surgewave.Wasm";
    public string DisplayName => "WebAssembly Plugins";

    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>("Surgewave:Wasm:Enabled", false);

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
        => services.AddSurgewaveWasm(configuration);

    public void Configure(object host, IServiceProvider services)
    {
        if (host is IEndpointRouteBuilder endpoints)
            endpoints.MapWasmPluginApi();
    }
}
