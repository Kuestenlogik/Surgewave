using Kuestenlogik.Surgewave.Schema.Registry;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Extension methods for registering the Thrift schema handler.
/// </summary>
public static class ThriftServiceCollectionExtensions
{
    /// <summary>
    /// Adds the Thrift schema handler to the service collection.
    /// </summary>
    public static IServiceCollection AddThriftSchemaHandler(this IServiceCollection services)
    {
        services.AddSingleton<ISchemaTypeHandler, ThriftSchemaHandler>();
        return services;
    }
}
