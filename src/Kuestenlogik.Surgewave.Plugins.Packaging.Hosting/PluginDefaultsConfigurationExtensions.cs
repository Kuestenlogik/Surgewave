using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using Microsoft.Extensions.FileProviders;

namespace Kuestenlogik.Surgewave.Plugins.Packaging.Hosting;

/// <summary>
/// <see cref="IConfigurationBuilder"/> extension methods for layering plugin-bundled
/// default settings into a host's effective configuration.
///
/// <para>
/// The Surgewave broker, Connect worker, marketplace, control plane and any other host
/// that loads plugins via the <c>plugins/&lt;id&gt;/</c> convention should call
/// <see cref="AddPluginDefaults(IConfigurationBuilder, string)"/> immediately after
/// <c>WebApplication.CreateBuilder(args)</c>. The helper enumerates every installed
/// plugin's bundled settings file (looked up via the manifest's <c>pluginSettings</c>
/// field, defaulting to <c>pluginsettings.json</c>) and inserts each one as a
/// configuration source at the FRONT of <see cref="IConfigurationBuilder.Sources"/>
/// — the lowest precedence — so plugin-shipped recommendations only take effect when
/// the host operator has not overridden them in <c>appsettings.json</c>, environment
/// variables, or command-line arguments.
/// </para>
///
/// <para>
/// This assembly carries an <c>Microsoft.AspNetCore.App</c> framework reference so
/// the <see cref="JsonConfigurationSource"/> + <see cref="PhysicalFileProvider"/>
/// types resolve cleanly. Hosts that already opt into the AspNet shared framework
/// (every <c>Microsoft.NET.Sdk.Web</c> project, which covers all Surgewave services)
/// can reference this assembly at zero additional cost. Pure console consumers
/// like the <c>surgewave</c> CLI keep referencing only <c>Kuestenlogik.Surgewave.Plugins.Packaging</c>
/// and avoid the AspNet types entirely.
/// </para>
/// </summary>
public static class PluginDefaultsConfigurationExtensions
{
    /// <summary>
    /// Layers every installed plugin's bundled settings file from <paramref name="pluginsDir"/>
    /// into <paramref name="builder"/> as the lowest-priority configuration sources, with
    /// file-watcher reload enabled by default.
    /// </summary>
    /// <param name="builder">The configuration builder to mutate. Must already have the
    /// host's own appsettings sources added (e.g. via <c>WebApplication.CreateBuilder</c>);
    /// the new plugin sources are inserted at the front so existing sources keep precedence.</param>
    /// <param name="pluginsDir">Absolute path to the directory containing installed plugin
    /// subdirectories (typically <c>./plugins</c> next to the host executable). A non-existent
    /// path is treated as "no plugins installed" and the call is a no-op.</param>
    /// <param name="reloadOnChange">Whether to enable the file system watcher on each plugin
    /// settings file. Default <c>true</c>: a plugin author shipping an updated
    /// <c>pluginsettings.json</c> can drop the new file in place and consumers using
    /// <see cref="Microsoft.Extensions.Options.IOptionsMonitor{TOptions}"/> will pick up the
    /// change without a broker restart. Consumers using <see cref="Microsoft.Extensions.Options.IOptions{TOptions}"/>
    /// (the singleton snapshot) still need a restart to see the new values — that is a
    /// property of the consumer side, not the configuration source. Set to <c>false</c>
    /// for tests, embedded scenarios, or environments where file-system watchers cause
    /// noise.</param>
    public static IConfigurationBuilder AddPluginDefaults(
        this IConfigurationBuilder builder,
        string pluginsDir,
        bool reloadOnChange = true)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(pluginsDir);

        var insertIndex = 0;
        foreach (var path in PluginPackageManager.EnumerateInstalledPluginSettingsFiles(pluginsDir))
        {
            // Build a JsonConfigurationSource manually so we can control where it lands
            // in builder.Sources. AddJsonFile would append it as the highest-priority
            // source, which is the wrong direction for plugin defaults.
            var directory = Path.GetDirectoryName(path)!;
            var fileName = Path.GetFileName(path);
            var source = new JsonConfigurationSource
            {
                FileProvider = new PhysicalFileProvider(directory),
                Path = fileName,
                Optional = false,
                ReloadOnChange = reloadOnChange,
            };
            source.ResolveFileProvider();
            builder.Sources.Insert(insertIndex++, source);
        }
        return builder;
    }
}
