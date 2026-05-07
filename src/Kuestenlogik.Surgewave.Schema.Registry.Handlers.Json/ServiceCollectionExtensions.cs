using Kuestenlogik.Surgewave.Schema.Registry;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Extension methods for registering JSON schema handler.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the JSON schema handler to the service collection.
    /// </summary>
    public static IServiceCollection AddJsonSchemaHandler(this IServiceCollection services)
    {
        services.AddSingleton<ISchemaTypeHandler, JsonSchemaHandler>();
        return services;
    }
}
