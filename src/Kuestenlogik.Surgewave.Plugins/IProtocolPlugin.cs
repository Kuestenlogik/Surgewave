using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Plugins;

/// <summary>
/// Plugin that provides a protocol adapter for the Surgewave Broker
/// (e.g., MQTT, WebSocket, AMQP). Protocol plugins register services,
/// map endpoints, and manage their own lifecycle.
/// </summary>
public interface IProtocolPlugin : IPlugin
{
    /// <summary>
    /// Default TCP port for this protocol (0 if the protocol shares the HTTP port).
    /// </summary>
    int DefaultPort { get; }

    /// <summary>
    /// Checks whether this protocol is enabled in the provided configuration.
    /// </summary>
    bool IsConfigEnabled(IConfiguration configuration);

    /// <summary>
    /// Registers services into the DI container.
    /// </summary>
    void ConfigureServices(IServiceCollection services, IConfiguration configuration);

    /// <summary>
    /// Configures middleware and maps endpoints after app.Build().
    /// Receives the host (WebApplication) and the built service provider for resolving
    /// IConfiguration, IOptions&lt;T&gt;, ILogger&lt;T&gt;, etc.
    /// Default implementation does nothing (for protocols that run purely as hosted services).
    /// </summary>
    void Configure(object host, IServiceProvider services) { }
}
