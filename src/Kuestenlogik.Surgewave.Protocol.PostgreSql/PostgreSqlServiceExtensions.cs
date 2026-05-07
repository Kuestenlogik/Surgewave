using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Protocol.PostgreSql;

/// <summary>
/// Extension methods for registering the PostgreSQL wire protocol adapter with dependency injection.
/// </summary>
public static class PostgreSqlServiceExtensions
{
    /// <summary>
    /// Adds the PostgreSQL wire protocol adapter as a hosted service.
    /// The adapter only listens when Surgewave:PostgreSql:Enabled is true (default: false).
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    /// <param name="configuration">Application configuration (reads Surgewave:PostgreSql section).</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    public static IServiceCollection AddSurgewavePostgreSql(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<PostgreSqlConfig>(configuration.GetSection(PostgreSqlConfig.SectionName));
        services.AddHostedService<PostgreSqlServer>();
        return services;
    }
}
