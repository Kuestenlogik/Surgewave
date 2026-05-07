using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Linq;

/// <summary>
/// DI registration for Surgewave LINQ.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers SurgewaveQueryContext as a singleton.
    /// </summary>
    public static IServiceCollection AddSurgewaveLinq(this IServiceCollection services, string bootstrapServers)
        => services.AddSingleton(new SurgewaveQueryContext(bootstrapServers));

    /// <summary>
    /// Registers SurgewaveQueryContext with custom options.
    /// </summary>
    public static IServiceCollection AddSurgewaveLinq(this IServiceCollection services, SurgewaveQueryOptions options)
        => services.AddSingleton(new SurgewaveQueryContext(options));
}
