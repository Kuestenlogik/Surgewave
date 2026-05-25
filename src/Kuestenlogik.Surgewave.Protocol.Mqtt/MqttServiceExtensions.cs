using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Protocol.Mqtt;

/// <summary>
/// Extension methods for registering the MQTT protocol adapter with dependency injection.
/// </summary>
public static class MqttServiceExtensions
{
    /// <summary>
    /// Add the MQTT protocol adapter as a hosted service.
    /// The adapter is only active when Surgewave:Mqtt:Enabled is true.
    /// </summary>
    public static IServiceCollection AddSurgewaveMqtt(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<MqttConfig>(configuration.GetSection(MqttConfig.SectionName));
        services.AddHostedService<MqttProtocolAdapter>();
        return services;
    }
}
