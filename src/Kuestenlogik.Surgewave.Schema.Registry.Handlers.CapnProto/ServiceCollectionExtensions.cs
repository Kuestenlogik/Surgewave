using Kuestenlogik.Surgewave.Schema.Registry;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Extension methods for registering the Cap'n Proto schema handler.
/// </summary>
public static class CapnProtoServiceCollectionExtensions
{
    /// <summary>
    /// Adds the Cap'n Proto schema handler to the service collection.
    /// </summary>
    public static IServiceCollection AddCapnProtoSchemaHandler(this IServiceCollection services)
    {
        services.AddSingleton<ISchemaTypeHandler, CapnProtoSchemaHandler>();
        return services;
    }
}
