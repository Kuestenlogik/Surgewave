using Kuestenlogik.Surgewave.Plugins;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
namespace Kuestenlogik.Surgewave.Protocol.WebSocket;

/// <summary>
/// Protocol plugin for the WebSocket adapter (browser-based streaming).
/// Maps endpoints onto the ASP.NET Core pipeline.
/// </summary>
public sealed class SurgewaveWebSocketProtocolPlugin : IProtocolPlugin
{
    public string FeatureId => "Surgewave.Protocol.WebSocket";
    public string DisplayName => "WebSocket Protocol";
    public int DefaultPort => 0;

    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>("Surgewave:WebSocket:Enabled", false);

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
        => services.AddSurgewaveWebSocket(configuration);

    public void Configure(object host, IServiceProvider services)
    {
        if (host is not WebApplication app) return;

        app.UseWebSockets(new WebSocketOptions { KeepAliveInterval = TimeSpan.FromSeconds(30) });
        app.MapSurgewaveWebSocket();
    }
}
