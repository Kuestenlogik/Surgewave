using System.Reflection;
using System.Runtime.Loader;
using Kuestenlogik.Surgewave.Plugins;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Discovers and holds <see cref="IControlPlugin"/> instances from the
/// <c>plugins/</c> directory. Provides page assemblies for Blazor Router
/// and navigation items for the sidebar menu.
/// </summary>
public sealed class ControlPluginRegistry
{
    private readonly List<IControlPlugin> _plugins = [];
    private readonly List<Assembly> _pageAssemblies = [];
    private readonly List<ControlNavItem> _navItems = [];

    public IReadOnlyList<Assembly> PageAssemblies => _pageAssemblies;
    public IReadOnlyList<ControlNavItem> NavItems => _navItems;
    public IReadOnlyList<IControlPlugin> Plugins => _plugins;

    /// <summary>
    /// Scans every directory in the given order. Control's plugins live under the
    /// same scopes as the broker's (#158) — one shipped with the installation, one
    /// the machine added, one the current user added — so passing the resolved
    /// order keeps Control from disagreeing with the broker about what is
    /// installed. It used to scan the literal <c>plugins</c>, relative to whatever
    /// directory Control happened to be started from.
    /// </summary>
    public void DiscoverPlugins(IReadOnlyList<string> pluginsDirectories, ILogger<ControlPluginRegistry> logger)
    {
        foreach (var directory in pluginsDirectories)
            DiscoverPlugins(directory, logger);
    }

    public void DiscoverPlugins(string pluginsDirectory, ILogger<ControlPluginRegistry> logger)
    {
        var pluginsDir = Path.GetFullPath(pluginsDirectory);
        if (!Directory.Exists(pluginsDir))
        {
            logger.LogDebug("Plugins directory not found: {Dir}", pluginsDir);
            return;
        }

        foreach (var pluginDir in Directory.GetDirectories(pluginsDir))
        {
            foreach (var dll in Directory.GetFiles(pluginDir, "*.dll"))
            {
                try
                {
                    var context = new AssemblyLoadContext(
                        Path.GetFileNameWithoutExtension(dll), isCollectible: false);
                    var assembly = context.LoadFromAssemblyPath(Path.GetFullPath(dll));

                    var pluginTypes = assembly.GetTypes()
                        .Where(t => t is { IsAbstract: false, IsInterface: false }
                                    && typeof(IControlPlugin).IsAssignableFrom(t));

                    foreach (var type in pluginTypes)
                    {
                        if (Activator.CreateInstance(type) is IControlPlugin plugin)
                        {
                            _plugins.Add(plugin);
                            _pageAssemblies.Add(plugin.PageAssembly);
                            _navItems.AddRange(plugin.GetNavItems());
                            logger.LogInformation(
                                "Control plugin '{Name}' loaded: {NavCount} nav items from {Assembly}",
                                plugin.DisplayName, plugin.GetNavItems().Count(),
                                plugin.PageAssembly.GetName().Name);
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger.LogDebug(ex, "Skipping {Dll} (no control plugin)", Path.GetFileName(dll));
                }
            }
        }

        _navItems.Sort((a, b) => a.Order.CompareTo(b.Order));
    }
}
