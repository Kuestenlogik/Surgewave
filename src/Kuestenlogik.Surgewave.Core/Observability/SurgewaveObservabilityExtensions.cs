using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Surgewave.Core.Observability;

/// <summary>
/// DI registration for <see cref="SurgewaveBrokerObservability"/>, bound from the
/// <c>Surgewave:Observability</c> configuration section.
/// </summary>
public static class SurgewaveObservabilityExtensions
{
    /// <summary>
    /// Registers <see cref="SurgewaveBrokerObservability"/> as both <see cref="ISurgewaveBrokerObservability"/>
    /// (consumer-facing) and the concrete type (publisher-facing) when
    /// <see cref="SurgewaveObservabilityOptions.Enabled"/> is <c>true</c>. When disabled, no
    /// registration happens — broker pipeline code that resolves the service via
    /// <c>GetService</c> sees <c>null</c> and takes the zero-cost no-op branch.
    /// </summary>
    public static IServiceCollection AddSurgewaveBrokerObservability(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.Configure<SurgewaveObservabilityOptions>(
            configuration.GetSection(SurgewaveObservabilityOptions.ConfigSection));

        var enabled = configuration.GetValue($"{SurgewaveObservabilityOptions.ConfigSection}:Enabled", defaultValue: true);
        if (!enabled) return services;

        services.AddSingleton(sp =>
        {
            var opts = sp.GetRequiredService<IOptions<SurgewaveObservabilityOptions>>().Value;
            var logger = (Microsoft.Extensions.Logging.ILogger<SurgewaveBrokerObservability>?)
                sp.GetService(typeof(Microsoft.Extensions.Logging.ILogger<SurgewaveBrokerObservability>));
            return new SurgewaveBrokerObservability(logger, opts.SubscriberCapacity);
        });
        services.AddSingleton<ISurgewaveBrokerObservability>(sp =>
            sp.GetRequiredService<SurgewaveBrokerObservability>());

        return services;
    }
}
