using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Kuestenlogik.Surgewave.Streams.Sql.MaterializedViews;

/// <summary>
/// DI registration for the materialized view subsystem.
/// </summary>
public static class MaterializedViewServiceExtensions
{
    /// <summary>
    /// Registers <see cref="MaterializedViewRegistry"/>, the default
    /// <see cref="LogManagerRawTopicReader"/>, and the
    /// <see cref="MaterializedViewRefreshService"/> hosted background loop.
    ///
    /// Bind options from <c>Surgewave:Streams:MaterializedViews</c>.
    /// The refresh loop only runs when <c>Enabled = true</c>.
    /// </summary>
    public static IServiceCollection AddSurgewaveMaterializedViews(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<MaterializedViewOptions>(
            configuration.GetSection(MaterializedViewOptions.SectionName));

        services.TryAddSingleton<MaterializedViewRegistry>();
        services.TryAddSingleton<IRawTopicReader, LogManagerRawTopicReader>();
        services.AddHostedService<MaterializedViewRefreshService>();
        return services;
    }
}
