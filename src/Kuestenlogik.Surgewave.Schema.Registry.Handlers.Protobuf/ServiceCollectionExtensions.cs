using Kuestenlogik.Surgewave.Schema.Registry;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Extension methods for registering Protobuf schema handler.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the Protobuf schema handler to the service collection.
    /// </summary>
    public static IServiceCollection AddProtobufSchemaHandler(this IServiceCollection services)
    {
        services.AddSingleton<ISchemaTypeHandler, ProtobufSchemaHandler>();
        return services;
    }
}
