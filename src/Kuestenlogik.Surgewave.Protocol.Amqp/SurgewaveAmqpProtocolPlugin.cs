using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Protocol.Amqp;

/// <summary>
/// Protocol plugin for the AMQP 0.9.1 adapter (RabbitMQ-compatible).
/// Runs as a BackgroundService with its own TCP listener.
/// </summary>
public sealed class SurgewaveAmqpProtocolPlugin : IProtocolPlugin
{
    public string FeatureId => "Surgewave.Protocol.Amqp";
    public string DisplayName => "AMQP 0.9.1 Protocol";
    public int DefaultPort => 5672;

    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>("Surgewave:Amqp:Enabled", false);

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
        => services.AddSurgewaveAmqp(configuration);
}
