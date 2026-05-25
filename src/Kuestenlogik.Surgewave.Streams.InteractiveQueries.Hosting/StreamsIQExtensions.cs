using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Extension methods for registering Interactive Query Service dependencies.
/// </summary>
public static class StreamsIQExtensions
{
    /// <summary>
    /// Adds the Surgewave Interactive Query Service to the service collection.
    /// Registers <see cref="IStateStoreRegistry"/> and <see cref="StateStoreQueryExecutor"/>
    /// as singletons so that state stores can be queried via the REST API.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSurgewaveInteractiveQueries(this IServiceCollection services)
    {
        services.AddSingleton<IStateStoreRegistry, StateStoreRegistry>();
        services.AddSingleton<StateStoreQueryExecutor>();
        return services;
    }
}
