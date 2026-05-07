using Kuestenlogik.Surgewave.Schema.Registry;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Extension methods for registering the Orleans schema handler.
/// </summary>
public static class OrleansServiceCollectionExtensions
{
    /// <summary>
    /// Adds the Orleans schema handler to the service collection.
    /// </summary>
    public static IServiceCollection AddOrleansSchemaHandler(this IServiceCollection services)
    {
        services.AddSingleton<ISchemaTypeHandler, OrleansSchemaHandler>();
        return services;
    }
}
