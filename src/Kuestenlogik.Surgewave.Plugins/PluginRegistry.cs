using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Plugins;

/// <summary>
/// Generic thread-safe registry for discovered plugins.
/// </summary>
public sealed class PluginRegistry
{
    private readonly ConcurrentDictionary<string, PluginInfo> _plugins = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Registers a plugin. Overwrites if a plugin with the same class name exists.
    /// </summary>
    public void Register(PluginInfo plugin) => _plugins[plugin.Class] = plugin;

    /// <summary>
    /// Removes a plugin by class name.
    /// </summary>
    public bool Remove(string className) => _plugins.TryRemove(className, out _);

    /// <summary>
    /// Gets a plugin by class name.
    /// </summary>
    public PluginInfo? Get(string className) =>
        _plugins.TryGetValue(className, out var plugin) ? plugin : null;

    /// <summary>
    /// Gets all registered plugins.
    /// </summary>
    public IReadOnlyList<PluginInfo> GetAll() => _plugins.Values.ToList();

    /// <summary>
    /// Gets all plugins of a specific type.
    /// </summary>
    public IReadOnlyList<PluginInfo> GetByType(string type) =>
        _plugins.Values.Where(p => p.Type.Equals(type, StringComparison.OrdinalIgnoreCase)).ToList();

    /// <summary>
    /// Gets the total number of registered plugins.
    /// </summary>
    public int Count => _plugins.Count;

    /// <summary>
    /// Checks if a plugin is registered.
    /// </summary>
    public bool Contains(string className) => _plugins.ContainsKey(className);

    /// <summary>
    /// Removes all registered plugins.
    /// </summary>
    public void Clear() => _plugins.Clear();
}
