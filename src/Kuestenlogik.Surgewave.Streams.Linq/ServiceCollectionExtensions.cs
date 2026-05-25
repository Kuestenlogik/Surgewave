using Kuestenlogik.Surgewave.Linq;
using Kuestenlogik.Surgewave.Streams.InteractiveQueries;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Streams.Linq;

/// <summary>
/// DI registration for Surgewave Streams LINQ integration.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers StreamsQueryContext that can query both topics and state stores.
    /// Requires AddSurgewaveLinq() and Surgewave Streams to be configured.
    /// </summary>
    public static IServiceCollection AddSurgewaveStreamsLinq(this IServiceCollection services)
    {
        services.AddSingleton(sp =>
        {
            var topicContext = sp.GetRequiredService<SurgewaveQueryContext>();
            var storeRegistry = sp.GetRequiredService<IStateStoreRegistry>();
            return new StreamsQueryContext(topicContext, storeRegistry);
        });
        return services;
    }
}
