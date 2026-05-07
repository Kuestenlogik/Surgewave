using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket;

/// <summary>
/// Extension methods for registering the WebSocket protocol adapter with dependency injection.
/// </summary>
public static class WebSocketServiceExtensions
{
    /// <summary>
    /// Add the WebSocket protocol adapter services.
    /// The adapter is only active when Surgewave:WebSocket:Enabled is true.
    /// </summary>
    public static IServiceCollection AddSurgewaveWebSocket(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<WebSocketConfig>(configuration.GetSection(WebSocketConfig.SectionName));
        services.AddSingleton<WebSocketProtocolAdapter>();
        return services;
    }

    /// <summary>
    /// Map WebSocket protocol adapter endpoints onto the ASP.NET Core routing pipeline.
    /// Call this after app.UseWebSockets() has been added to the middleware pipeline.
    /// </summary>
    public static IEndpointRouteBuilder MapSurgewaveWebSocket(this IEndpointRouteBuilder endpoints)
    {
        var adapter = endpoints.ServiceProvider.GetRequiredService<WebSocketProtocolAdapter>();
        adapter.MapEndpoints(endpoints);
        return endpoints;
    }
}
