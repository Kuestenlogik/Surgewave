using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Plugins;

/// <summary>
/// Plugin that extends the Surgewave Broker with enterprise features.
/// Broker plugins register services via DI and optionally map HTTP endpoints.
/// Discovered automatically via interface scanning at broker startup.
/// </summary>
public interface IBrokerPlugin : IPlugin
{
    /// <summary>
    /// Checks whether this plugin is enabled in the provided configuration.
    /// </summary>
    bool IsConfigEnabled(IConfiguration configuration);

    /// <summary>
    /// Registers services into the DI container.
    /// Called during broker startup before the application is built.
    /// </summary>
    void ConfigureServices(IServiceCollection services, IConfiguration configuration);

    /// <summary>
    /// Maps endpoints and configures middleware after app.Build().
    /// Receives the host (WebApplication) and the service provider.
    /// Default implementation does nothing.
    /// </summary>
    void Configure(object host, IServiceProvider services) { }

    /// <summary>
    /// Async variant of <see cref="Configure"/> for plugins that need asynchronous
    /// initialization (network connections, background service startup, pipeline
    /// orchestration, etc.). Called by <c>BrokerPluginActivator.ConfigureBrokerPluginsAsync</c>
    /// after <c>app.Build()</c>.
    ///
    /// <para>
    /// Default implementation calls the synchronous <see cref="Configure"/> method and
    /// returns a completed task, so existing plugins that override only <see cref="Configure"/>
    /// do not need any changes. New plugins with async lifecycle should override this
    /// method instead and leave <see cref="Configure"/> at its default no-op.
    /// </para>
    /// </summary>
    Task ConfigureAsync(object host, IServiceProvider services)
    {
        Configure(host, services);
        return Task.CompletedTask;
    }
}
