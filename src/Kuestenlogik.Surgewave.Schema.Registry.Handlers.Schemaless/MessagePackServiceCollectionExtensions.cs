using Kuestenlogik.Surgewave.Schema.Registry;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Extension methods for registering the MessagePack schema handler.
/// </summary>
public static class MessagePackServiceCollectionExtensions
{
    /// <summary>
    /// Adds the MessagePack schema handler to the service collection.
    /// </summary>
    public static IServiceCollection AddMessagePackSchemaHandler(this IServiceCollection services)
    {
        services.AddSingleton<ISchemaTypeHandler, MessagePackSchemaHandler>();
        return services;
    }
}
