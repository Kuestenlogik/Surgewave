using System.Reflection;
using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Plugins.Licensing;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Plugins;

/// <summary>
/// Discovers and activates <see cref="IBrokerPlugin"/> and <see cref="IProtocolPlugin"/>
/// implementations at broker startup via assembly scanning.
/// </summary>
public static class BrokerPluginActivator
{
    // Names of assemblies loaded from the plugins/ directory. Populated by
    // EnsurePluginsLoaded() and used by Discover<T>() so that third-party
    // assemblies with non-Kuestenlogik namespaces are also scanned, without relying on
    // Assembly.Location (which is empty for bundled single-file assemblies).
    private static readonly HashSet<string> _pluginAssemblyNames =
        new(StringComparer.OrdinalIgnoreCase);

    private static readonly JsonSerializerOptions _jsonOptions = new() { PropertyNameCaseInsensitive = true };

    /// <summary>
    /// Scans all loaded assemblies for <see cref="IBrokerPlugin"/> implementations,
    /// checks configuration and licensing, and calls ConfigureServices on each enabled plugin.
    /// When no license provider is registered, enterprise features are not available.
    /// </summary>
    public static IReadOnlyList<IBrokerPlugin> ActivatePlugins(
        IServiceCollection services,
        IConfiguration configuration,
        ILicenseProvider? license = null,
        ILogger? logger = null)
    {
        var activated = new List<IBrokerPlugin>();
        var discoverySw = System.Diagnostics.Stopwatch.StartNew();
        var discovered = Discover<IBrokerPlugin>();
        discoverySw.Stop();

        logger?.LogInformation("Discovered {Count} broker plugin(s) in {ElapsedMs}ms: {Plugins}",
            discovered.Count, discoverySw.ElapsedMilliseconds,
            string.Join(", ", discovered.Select(p => p.DisplayName)));

        foreach (var plugin in discovered)
        {
            if (!plugin.IsConfigEnabled(configuration))
            {
                logger?.LogDebug("Broker plugin '{Plugin}' is not enabled in configuration", plugin.DisplayName);
                continue;
            }

            // License check: enterprise features require a license provider
            if (SurgewaveFeatures.IsEnterpriseFeature(plugin.FeatureId))
            {
                if (license is null || !license.IsFeatureEnabled(plugin.FeatureId))
                {
                    logger?.LogWarning(
                        "Broker plugin '{Plugin}' requires enterprise feature '{Feature}' — skipped (no license)",
                        plugin.DisplayName, plugin.FeatureId);
                    continue;
                }
            }

            try
            {
                plugin.ConfigureServices(services, configuration);
                activated.Add(plugin);
                logger?.LogInformation("Activated broker plugin: {Plugin} (feature: {Feature})",
                    plugin.DisplayName, plugin.FeatureId);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Failed to activate broker plugin '{Plugin}'", plugin.DisplayName);
            }
        }

        services.AddSingleton<IReadOnlyList<IBrokerPlugin>>(activated);
        return activated;
    }

    /// <summary>
    /// Calls <see cref="IBrokerPlugin.ConfigureAsync"/> on all activated broker plugins
    /// after app.Build(). Plugins map their own endpoints, configure middleware, and
    /// perform async initialization (connect clients, start background services, etc.).
    /// </summary>
    public static async Task ConfigureBrokerPluginsAsync(object host, IServiceProvider services, IReadOnlyList<IBrokerPlugin> activated)
    {
        var logger = services.GetService<ILoggerFactory>()?.CreateLogger(typeof(BrokerPluginActivator));
        var totalSw = System.Diagnostics.Stopwatch.StartNew();

        foreach (var plugin in activated)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            await plugin.ConfigureAsync(host, services);
            sw.Stop();
            logger?.LogInformation("Plugin Configure: {Plugin} completed in {ElapsedMs}ms",
                plugin.DisplayName, sw.ElapsedMilliseconds);
        }

        totalSw.Stop();
        logger?.LogInformation("All {Count} broker plugin(s) configured in {TotalMs}ms",
            activated.Count, totalSw.ElapsedMilliseconds);
    }

    /// <summary>
    /// Disposes all activated broker plugins during graceful shutdown.
    /// Plugins that acquired resources (network connections, background services, etc.)
    /// in <see cref="IBrokerPlugin.ConfigureAsync"/> should release them in
    /// <see cref="IAsyncDisposable.DisposeAsync"/>.
    /// </summary>
    public static async Task DisposeBrokerPluginsAsync(IReadOnlyList<IBrokerPlugin> activated, ILogger? logger = null)
    {
        foreach (var plugin in activated)
        {
            // Plugins that need cleanup implement IAsyncDisposable on their own class.
            // No interface constraint required — opt-in pattern.
            if (plugin is IAsyncDisposable disposable)
            {
                try
                {
                    await disposable.DisposeAsync();
                    logger?.LogDebug("Disposed plugin: {Plugin}", plugin.DisplayName);
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Error disposing plugin '{Plugin}'", plugin.DisplayName);
                }
            }
        }
    }

    /// <summary>
    /// Scans all loaded assemblies for <see cref="IProtocolPlugin"/> implementations,
    /// checks configuration, and calls ConfigureServices on each enabled protocol.
    /// Protocol plugins are not license-gated (community features).
    /// </summary>
    public static IReadOnlyList<IProtocolPlugin> ActivateProtocols(
        IServiceCollection services,
        IConfiguration configuration,
        ILogger? logger = null)
    {
        var activated = new List<IProtocolPlugin>();
        var discovered = Discover<IProtocolPlugin>();

        logger?.LogInformation("Discovered {Count} protocol plugin(s): {Plugins}",
            discovered.Count, string.Join(", ", discovered.Select(p => p.DisplayName)));

        foreach (var plugin in discovered)
        {
            if (!plugin.IsConfigEnabled(configuration))
            {
                logger?.LogDebug("Protocol plugin '{Plugin}' is not enabled in configuration", plugin.DisplayName);
                continue;
            }

            try
            {
                plugin.ConfigureServices(services, configuration);
                activated.Add(plugin);
                logger?.LogInformation("Activated protocol plugin: {Plugin} (port: {Port})",
                    plugin.DisplayName, plugin.DefaultPort > 0 ? plugin.DefaultPort : "shared");
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Failed to activate protocol plugin '{Plugin}'", plugin.DisplayName);
            }
        }

        services.AddSingleton<IReadOnlyList<IProtocolPlugin>>(activated);
        return activated;
    }

    /// <inheritdoc cref="ConfigureBrokerPlugins"/>
    public static void ConfigureProtocols(object host, IServiceProvider services, IReadOnlyList<IProtocolPlugin> activated)
    {
        foreach (var plugin in activated)
        {
            plugin.Configure(host, services);
        }
    }

    /// <summary>
    /// Discovers all concrete implementations of T in loaded Kuestenlogik.Surgewave assemblies
    /// and any assemblies loaded from the plugins/ directory.
    /// </summary>
    public static List<T> Discover<T>() where T : class, IPlugin
    {
        EnsurePluginsLoaded();

        // Filter to Surgewave assemblies (Kuestenlogik.Surgewave.* + any loaded from plugins/) then
        // delegate the actual type scanning + instantiation to the shared helper.
        var surgewaveAssemblies = AppDomain.CurrentDomain.GetAssemblies()
            .Where(a =>
            {
                var name = a.GetName().Name;
                if (name is null) return false;
                return name.StartsWith(SurgewavePackageConventions.HostAssemblyPrefix, StringComparison.OrdinalIgnoreCase)
                    || _pluginAssemblyNames.Contains(name);
            });

        return PluginAssemblyScanner.FindImplementations<T>(surgewaveAssemblies).ToList();
    }

    /// <summary>
    /// Loads Surgewave assemblies from the application directory (dev mode) and
    /// manifest-declared assemblies from each plugins/&lt;id&gt;/ subdirectory.
    /// Already-loaded assemblies are skipped.
    /// </summary>
    private static void EnsurePluginsLoaded()
    {
        var loadedNames = new HashSet<string>(
            AppDomain.CurrentDomain.GetAssemblies()
                .Select(a => a.GetName().Name)
                .Where(n => n != null)!,
            StringComparer.OrdinalIgnoreCase);

        // Dev mode: load Kuestenlogik.Surgewave.* assemblies that live alongside the exe.
        // In single-file publish this directory has no managed DLLs — the loop is a no-op.
        // SurgewavePackageConventions.HostAssemblyPrefix is the single authoritative definition
        // of the Surgewave assembly naming convention used throughout broker and packager.
        var baseDir = AppContext.BaseDirectory;
        foreach (var dll in Directory.GetFiles(baseDir, $"{SurgewavePackageConventions.HostAssemblyPrefix}*.dll"))
            TryLoadAssembly(dll, isPluginAssembly: false, loadedNames);

        // Plugin loading: each installed plugin lives in plugins/<id>/ and declares its
        // assemblies in a plugin.json manifest. Loading is driven by the manifest,
        // so no naming convention filter is needed here.
        var pluginsDir = Path.Combine(baseDir, "plugins");
        if (!Directory.Exists(pluginsDir)) return;

        foreach (var manifestFile in Directory.GetFiles(pluginsDir, SurgewavePackageConventions.ManifestFileName, SearchOption.AllDirectories))
        {
            var pluginDir = Path.GetDirectoryName(manifestFile)!;
            var manifest = TryReadManifest(manifestFile);
            if (manifest == null) continue;

            // Load the plugin's own assemblies — these are scanned for IPlugin implementations
            foreach (var assemblyFile in manifest.Assemblies)
                TryLoadAssembly(Path.Combine(pluginDir, assemblyFile), isPluginAssembly: true, loadedNames);

            // Load exclusive third-party deps from deps/ — needed for resolution, not scanned
            var depsDir = Path.Combine(pluginDir, "deps");
            if (Directory.Exists(depsDir))
                foreach (var dep in Directory.GetFiles(depsDir, "*.dll"))
                    TryLoadAssembly(dep, isPluginAssembly: false, loadedNames);
        }
    }

    private static void TryLoadAssembly(string path, bool isPluginAssembly, HashSet<string> loadedNames)
    {
        try
        {
            if (!File.Exists(path)) return;
            var asmName = AssemblyName.GetAssemblyName(path).Name;
            if (asmName == null || loadedNames.Contains(asmName)) return;

            Assembly.LoadFrom(path);
            loadedNames.Add(asmName);

            // Track plugin assemblies so Discover<T>() includes them even when their
            // namespace doesn't start with Kuestenlogik.Surgewave. (third-party protocols).
            if (isPluginAssembly)
                _pluginAssemblyNames.Add(asmName);
        }
        catch
        {
            // Ignore DLLs that cannot be loaded (native, corrupt, incompatible platform, etc.)
        }
    }

    private static PluginManifest? TryReadManifest(string path)
    {
        try
        {
            var json = File.ReadAllText(path);
            return JsonSerializer.Deserialize<PluginManifest>(json, _jsonOptions);
        }
        catch
        {
            return null;
        }
    }
}
