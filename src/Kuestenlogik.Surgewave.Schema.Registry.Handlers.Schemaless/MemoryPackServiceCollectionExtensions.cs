using Kuestenlogik.Surgewave.Schema.Registry;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Extension methods for registering the MemoryPack schema handler.
/// </summary>
public static class MemoryPackServiceCollectionExtensions
{
    /// <summary>
    /// Adds the MemoryPack schema handler to the service collection.
    /// </summary>
    public static IServiceCollection AddMemoryPackSchemaHandler(this IServiceCollection services)
    {
        services.AddSingleton<ISchemaTypeHandler, MemoryPackSchemaHandler>();
        return services;
    }
}
