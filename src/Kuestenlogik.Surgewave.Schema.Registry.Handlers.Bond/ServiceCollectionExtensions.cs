using Kuestenlogik.Surgewave.Schema.Registry;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Extension methods for registering the Bond schema handler.
/// </summary>
public static class BondServiceCollectionExtensions
{
    /// <summary>
    /// Adds the Bond schema handler to the service collection.
    /// </summary>
    public static IServiceCollection AddBondSchemaHandler(this IServiceCollection services)
    {
        services.AddSingleton<ISchemaTypeHandler, BondSchemaHandler>();
        return services;
    }
}
