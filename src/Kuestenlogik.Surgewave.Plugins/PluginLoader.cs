using System.Reflection;
using System.Runtime.Loader;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Plugins;

/// <summary>
/// Loads plugin assemblies with proper isolation.
/// </summary>
public sealed class PluginLoader : IDisposable
{
    private readonly ILogger<PluginLoader> _logger;
    private readonly Dictionary<string, LoadedPlugin> _loadedPlugins = new();
    private readonly object _lock = new();

    public PluginLoader(ILogger<PluginLoader> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Loads a plugin assembly from the specified path.
    /// </summary>
    public Assembly? LoadPlugin(string assemblyPath)
    {
        lock (_lock)
        {
            if (_loadedPlugins.TryGetValue(assemblyPath, out var existing))
            {
                return existing.Assembly;
            }

            try
            {
                var loadContext = new PluginAssemblyLoadContext(assemblyPath);
                var assembly = loadContext.LoadFromAssemblyPath(assemblyPath);

                _loadedPlugins[assemblyPath] = new LoadedPlugin(assemblyPath, loadContext, assembly);
                _logger.LogInformation("Loaded plugin assembly: {Assembly}", assembly.FullName);

                return assembly;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to load plugin from {Path}", assemblyPath);
                return null;
            }
        }
    }

    /// <summary>
    /// Unloads a previously loaded plugin.
    /// </summary>
    public bool UnloadPlugin(string assemblyPath)
    {
        lock (_lock)
        {
            if (!_loadedPlugins.TryGetValue(assemblyPath, out var plugin))
            {
                return false;
            }

            _loadedPlugins.Remove(assemblyPath);

            plugin.LoadContext.Unload();
            _logger.LogInformation("Unloaded plugin: {Path}", assemblyPath);

            return true;
        }
    }

    /// <summary>
    /// Gets all loaded plugin assemblies.
    /// </summary>
    public IReadOnlyList<Assembly> GetLoadedAssemblies()
    {
        lock (_lock)
        {
            return _loadedPlugins.Values.Select(p => p.Assembly).ToList();
        }
    }

    /// <summary>
    /// Creates an instance of a plugin type from a loaded plugin assembly.
    /// </summary>
    public T? CreateInstance<T>(string className) where T : class
    {
        lock (_lock)
        {
            foreach (var plugin in _loadedPlugins.Values)
            {
                var type = plugin.Assembly.GetType(className);
                if (type != null && typeof(T).IsAssignableFrom(type))
                {
                    try
                    {
                        return Activator.CreateInstance(type) as T;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to create plugin instance: {Type}", className);
                        return null;
                    }
                }
            }

            // Fall back to Type.GetType for plugins in the main assembly
            var fallbackType = Type.GetType(className);
            if (fallbackType != null && typeof(T).IsAssignableFrom(fallbackType))
            {
                try
                {
                    return Activator.CreateInstance(fallbackType) as T;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to create plugin instance: {Type}", className);
                }
            }

            _logger.LogWarning("Plugin type not found: {Type}", className);
            return null;
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            foreach (var plugin in _loadedPlugins.Values)
            {
                try
                {
                    plugin.LoadContext.Unload();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error unloading plugin: {Path}", plugin.Path);
                }
            }
            _loadedPlugins.Clear();
        }
    }

    private sealed record LoadedPlugin(
        string Path,
        AssemblyLoadContext LoadContext,
        Assembly Assembly);
}
