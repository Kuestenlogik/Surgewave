using Kuestenlogik.Surgewave.Schema.Registry;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Extension methods for registering Avro schema handler.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the Avro schema handler to the service collection.
    /// </summary>
    public static IServiceCollection AddAvroSchemaHandler(this IServiceCollection services)
    {
        services.AddSingleton<ISchemaTypeHandler, AvroSchemaHandler>();
        return services;
    }
}
