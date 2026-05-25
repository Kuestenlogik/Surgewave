using Kuestenlogik.Surgewave.Core.Queue;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Protocol.Amqp;

/// <summary>
/// Extension methods for registering the AMQP 0.9.1 protocol adapter with dependency injection.
/// </summary>
public static class AmqpServiceExtensions
{
    /// <summary>
    /// Adds the AMQP 0.9.1 protocol adapter as a hosted service.
    /// The adapter only listens when Surgewave:Amqp:Enabled is true (default: false).
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    /// <param name="configuration">Application configuration (reads Surgewave:Amqp section).</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <remarks>
    /// When an <see cref="IQueueViewManager"/> implementation has been registered in DI
    /// (e.g. via <c>AddSurgewaveQueueView()</c> in the Broker project), the AMQP adapter will
    /// automatically use it for visibility-timeout-based delivery semantics.
    /// Without it, the adapter falls back to simple offset-commit tracking.
    /// </remarks>
    public static IServiceCollection AddSurgewaveAmqp(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<AmqpConfig>(configuration.GetSection(AmqpConfig.SectionName));
        services.AddHostedService<AmqpBrokerAdapter>();
        return services;
    }
}
