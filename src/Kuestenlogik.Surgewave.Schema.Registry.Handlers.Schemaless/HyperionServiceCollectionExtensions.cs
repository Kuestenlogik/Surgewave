using Kuestenlogik.Surgewave.Schema.Registry;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Extension methods for registering the Hyperion schema handler.
/// </summary>
public static class HyperionServiceCollectionExtensions
{
    /// <summary>
    /// Adds the Hyperion schema handler to the service collection.
    /// </summary>
    public static IServiceCollection AddHyperionSchemaHandler(this IServiceCollection services)
    {
        services.AddSingleton<ISchemaTypeHandler, HyperionSchemaHandler>();
        return services;
    }
}
