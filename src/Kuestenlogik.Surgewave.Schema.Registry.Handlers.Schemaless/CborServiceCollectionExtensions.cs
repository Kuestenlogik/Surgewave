using Kuestenlogik.Surgewave.Schema.Registry;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Extension methods for registering the CBOR schema handler.
/// </summary>
public static class CborServiceCollectionExtensions
{
    /// <summary>
    /// Adds the CBOR schema handler to the service collection.
    /// </summary>
    public static IServiceCollection AddCborSchemaHandler(this IServiceCollection services)
    {
        services.AddSingleton<ISchemaTypeHandler, CborSchemaHandler>();
        return services;
    }
}
