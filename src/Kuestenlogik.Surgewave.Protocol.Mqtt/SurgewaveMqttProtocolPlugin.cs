using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Protocol.Mqtt;

/// <summary>
/// Protocol plugin for the MQTT adapter (MQTTnet embedded server).
/// Runs as a BackgroundService — no endpoint mapping needed.
/// </summary>
public sealed class SurgewaveMqttProtocolPlugin : IProtocolPlugin
{
    public string FeatureId => "Surgewave.Protocol.Mqtt";
    public string DisplayName => "MQTT Protocol";
    public int DefaultPort => 1883;

    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>("Surgewave:Mqtt:Enabled", false);

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
        => services.AddSurgewaveMqtt(configuration);
}
