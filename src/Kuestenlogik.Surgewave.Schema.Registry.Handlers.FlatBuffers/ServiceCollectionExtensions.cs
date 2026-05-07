using Kuestenlogik.Surgewave.Schema.Registry;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Extension methods for registering FlatBuffers schema handler.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the FlatBuffers schema handler to the service collection.
    /// </summary>
    public static IServiceCollection AddFlatBuffersSchemaHandler(this IServiceCollection services)
    {
        services.AddSingleton<ISchemaTypeHandler, FlatBuffersSchemaHandler>();
        return services;
    }
}
