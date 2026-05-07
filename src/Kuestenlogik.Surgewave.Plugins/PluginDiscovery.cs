using System.Reflection;
using System.Runtime.Loader;
using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Kuestenlogik.Surgewave.Plugins.Pipeline;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Plugins;

/// <summary>
/// Discovers plugins from directories by scanning assemblies for IPlugin implementations.
/// Supports manifest-based discovery (plugin.json) and assembly scanning.
/// </summary>
public sealed class PluginDiscovery
{
    private static readonly JsonSerializerOptions ManifestJsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly PluginLoader _loader;
    private readonly ILogger<PluginDiscovery> _logger;
    private readonly List<PluginInfo> _discoveredPlugins = new();
    private readonly List<Assembly> _builtInAssemblies = new();
    private readonly object _lock = new();

    public PluginDiscovery(PluginLoader loader, ILogger<PluginDiscovery> logger)
    {
        _loader = loader;
        _logger = logger;
    }

    /// <summary>
    /// Gets the underlying plugin loader (for hot-swap operations).
    /// </summary>
    public PluginLoader GetPluginLoader() => _loader;

    /// <summary>
    /// Registers an assembly to be scanned for built-in plugins.
    /// </summary>
    public void RegisterBuiltInAssembly(Assembly assembly)
    {
        lock (_lock)
        {
            if (!_builtInAssemblies.Contains(assembly))
            {
                _builtInAssemblies.Add(assembly);
                _logger.LogInformation("Registered built-in plugin assembly: {Assembly}", assembly.GetName().Name);
            }
        }
    }

    /// <summary>
    /// Registers an assembly by a marker type.
    /// </summary>
    public void RegisterBuiltInAssembly<T>()
    {
        RegisterBuiltInAssembly(typeof(T).Assembly);
    }

    /// <summary>
    /// Discovers all plugins in the specified directory.
    /// </summary>
    public void DiscoverPlugins(string pluginsDirectory, bool useDefaultContext = false)
    {
        if (!Directory.Exists(pluginsDirectory))
        {
            _logger.LogInformation("Plugins directory does not exist: {Directory}", pluginsDirectory);
            return;
        }

        _logger.LogInformation("Scanning for plugins in: {Directory} (useDefaultContext={UseDefault})", pluginsDirectory, useDefaultContext);

        foreach (var pluginDir in Directory.GetDirectories(pluginsDirectory))
        {
            ScanPluginDirectory(pluginDir, useDefaultContext);
        }

        foreach (var dllPath in Directory.GetFiles(pluginsDirectory, "*.dll"))
        {
            ScanAssembly(dllPath, useDefaultContext);
        }

        _logger.LogInformation("Discovered {Count} plugins", _discoveredPlugins.Count);
    }

    /// <summary>
    /// Unloads a plugin by its directory path, removes its discovered entries,
    /// then reloads from the same directory. Used for hot-swap after .swpkg install.
    /// </summary>
    public int HotSwapPlugin(string pluginDirectory)
    {
        lock (_lock)
        {
            var dllFiles = Directory.Exists(pluginDirectory)
                ? Directory.GetFiles(pluginDirectory, "*.dll")
                : [];

            foreach (var dll in dllFiles)
            {
                _loader.UnloadPlugin(dll);
            }

            var dirName = Path.GetFileName(pluginDirectory);
            _discoveredPlugins.RemoveAll(p =>
                p.Class.StartsWith(dirName, StringComparison.OrdinalIgnoreCase));
        }

        var countBefore = _discoveredPlugins.Count;
        DiscoverPlugins(pluginDirectory);
        return _discoveredPlugins.Count - countBefore;
    }

    /// <summary>
    /// Gets all discovered plugins.
    /// </summary>
    public IReadOnlyList<PluginInfo> GetDiscoveredPlugins()
    {
        lock (_lock)
        {
            return _discoveredPlugins.ToList();
        }
    }

    /// <summary>
    /// Gets the combined list of discovered plugins and built-in plugins (deduplicated).
    /// </summary>
    public IReadOnlyList<PluginInfo> GetAllPlugins()
    {
        var seen = new HashSet<string>();
        var plugins = new List<PluginInfo>();

        lock (_lock)
        {
            foreach (var plugin in _discoveredPlugins)
            {
                if (seen.Add(plugin.Class))
                {
                    plugins.Add(plugin);
                }
            }
        }

        foreach (var plugin in GetBuiltInPlugins())
        {
            if (seen.Add(plugin.Class))
            {
                plugins.Add(plugin);
            }
        }

        return plugins;
    }

    /// <summary>
    /// Loads a plugin type by its class name.
    /// </summary>
    public Type? LoadPluginType(string className)
    {
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            try
            {
                var type = assembly.GetExportedTypes()
                    .FirstOrDefault(t => t.FullName == className && ImplementsPluginInterface(t));
                if (type != null)
                    return type;
            }
            catch
            {
                // Skip assemblies that fail to enumerate types
            }
        }

        foreach (var assembly in _loader.GetLoadedAssemblies())
        {
            try
            {
                var type = assembly.GetExportedTypes()
                    .FirstOrDefault(t => t.FullName == className && ImplementsPluginInterface(t));
                if (type != null)
                    return type;
            }
            catch
            {
                // Skip assemblies that fail to enumerate types
            }
        }

        var fallbackType = Type.GetType(className);
        if (fallbackType != null && ImplementsPluginInterface(fallbackType))
            return fallbackType;

        return null;
    }

    private void ScanPluginDirectory(string pluginDir, bool useDefaultContext)
    {
        var pluginName = Path.GetFileName(pluginDir);

        var manifestPath = Path.Combine(pluginDir, "plugin.json");
        if (File.Exists(manifestPath))
        {
            if (TryLoadFromManifest(pluginDir, manifestPath, useDefaultContext))
                return;
        }

        var mainDll = Path.Combine(pluginDir, $"{pluginName}.dll");
        if (File.Exists(mainDll))
        {
            ScanAssembly(mainDll, useDefaultContext);
            return;
        }

        foreach (var dllPath in Directory.GetFiles(pluginDir, "*.dll"))
        {
            ScanAssembly(dllPath, useDefaultContext);
        }
    }

    private bool TryLoadFromManifest(string pluginDir, string manifestPath, bool useDefaultContext)
    {
        try
        {
            var json = File.ReadAllText(manifestPath);
            var manifest = JsonSerializer.Deserialize<PluginManifest>(json, ManifestJsonOptions);

            if (manifest == null)
            {
                _logger.LogDebug("Failed to parse manifest, falling back to assembly scanning: {Path}", manifestPath);
                return false;
            }

            // Scan assemblies listed in manifest
            var loaded = false;
            foreach (var assemblyName in manifest.Assemblies)
            {
                var dllPath = Path.Combine(pluginDir, assemblyName);
                if (File.Exists(dllPath))
                {
                    ScanAssembly(dllPath, useDefaultContext);
                    loaded = true;
                }
                else
                {
                    _logger.LogWarning("Assembly {Assembly} listed in manifest not found at {Path}", assemblyName, dllPath);
                }
            }

            if (!loaded)
            {
                _logger.LogDebug("No assemblies from manifest found for {Id}, falling back to directory scan", manifest.Id);
                return false;
            }

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load plugin from manifest: {Path}", manifestPath);
            return false;
        }
    }

    private void ScanAssembly(string assemblyPath, bool useDefaultContext)
    {
        try
        {
            var assemblyName = AssemblyName.GetAssemblyName(assemblyPath);

            Assembly? assembly = null;
            foreach (var loadedAssembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                if (loadedAssembly.GetName().Name == assemblyName.Name)
                {
                    assembly = loadedAssembly;
                    _logger.LogDebug("Using already-loaded assembly: {Assembly}", assembly.FullName);
                    break;
                }
            }

            if (assembly == null)
            {
                assembly = useDefaultContext
                    ? AssemblyLoadContext.Default.LoadFromAssemblyPath(assemblyPath)
                    : _loader.LoadPlugin(assemblyPath);
            }

            if (assembly == null) return;

            foreach (var type in assembly.GetExportedTypes())
            {
                if (type.IsAbstract || type.IsInterface)
                    continue;

                if (!ImplementsPluginInterface(type))
                    continue;

                var pluginType = DeterminePluginType(type);
                var version = GetPluginVersion(type);
                var metadata = GetPluginMetadata(type);

                var plugin = new PluginInfo
                {
                    Class = type.FullName!,
                    Type = pluginType,
                    Version = version,
                    DisplayName = metadata?.Name,
                    Description = metadata?.Description,
                    Icon = metadata?.Icon,
                    Category = metadata?.Tags != null
                        ? DeriveCategory(metadata.Tags)
                        : null
                };

                lock (_lock)
                {
                    if (!_discoveredPlugins.Any(p => p.Class == plugin.Class))
                    {
                        _discoveredPlugins.Add(plugin);
                        _logger.LogInformation("Discovered plugin: {Class} ({Type})", plugin.Class, plugin.Type);
                    }
                }
            }
        }
        catch (BadImageFormatException)
        {
            // Not a .NET assembly — skip silently
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to scan assembly: {Path}", assemblyPath);
        }
    }

    /// <summary>
    /// Checks if a type implements any plugin interface (IPlugin, IConnector, or known base classes).
    /// Uses name-based matching to avoid type identity issues across assembly contexts.
    /// </summary>
    internal static bool ImplementsPluginInterface(Type type)
    {
        foreach (var iface in type.GetInterfaces())
        {
            // New unified interfaces
            if (iface.Name == "IPlugin" && iface.Namespace == "Kuestenlogik.Surgewave.Plugins")
                return true;

            // Backward compat: legacy IConnector
            if (iface.Name == "IConnector" && iface.Namespace == "Kuestenlogik.Surgewave.Connect")
                return true;
        }

        // Check base class hierarchy for legacy connector base classes
        var baseType = type.BaseType;
        while (baseType != null)
        {
            if (baseType.Name is "SourceConnector" or "SinkConnector")
                return true;
            baseType = baseType.BaseType;
        }

        return false;
    }

    /// <summary>
    /// Determines the plugin type from implemented interfaces and class hierarchy.
    /// </summary>
    internal static string DeterminePluginType(Type type)
    {
        // Check new interfaces first (by name to handle cross-context)
        foreach (var iface in type.GetInterfaces())
        {
            switch (iface.Name)
            {
                case "ISourceNode" when iface.Namespace == "Kuestenlogik.Surgewave.Plugins.Pipeline":
                    return "source";
                case "ISinkNode" when iface.Namespace == "Kuestenlogik.Surgewave.Plugins.Pipeline":
                    return "sink";
                case "IProcessorNode" when iface.Namespace == "Kuestenlogik.Surgewave.Plugins.Pipeline":
                    return "processor";
                case "ITriggerNode" when iface.Namespace == "Kuestenlogik.Surgewave.Plugins.Pipeline":
                    return "trigger";
                case "ISingleMessageTransform" when iface.Namespace == "Kuestenlogik.Surgewave.Plugins.Pipeline":
                    return "transform";
                case "IBrokerPlugin" when iface.Namespace == "Kuestenlogik.Surgewave.Plugins":
                    return "broker";
                case "IProtocolPlugin" when iface.Namespace == "Kuestenlogik.Surgewave.Plugins":
                    return "protocol";
            }
        }

        // Legacy: check class hierarchy for SourceConnector/SinkConnector
        var baseType = type.BaseType;
        while (baseType != null)
        {
            if (baseType.Name == "SourceConnector")
                return "source";
            if (baseType.Name == "SinkConnector")
                return "sink";
            baseType = baseType.BaseType;
        }

        // Fallback: try class name heuristics
        var name = type.Name;
        if (name.Contains("Source", StringComparison.OrdinalIgnoreCase)) return "source";
        if (name.Contains("Sink", StringComparison.OrdinalIgnoreCase)) return "sink";
        if (name.Contains("Producer", StringComparison.OrdinalIgnoreCase)) return "source";
        if (name.Contains("Consumer", StringComparison.OrdinalIgnoreCase)) return "sink";
        if (name.Contains("Writer", StringComparison.OrdinalIgnoreCase)) return "sink";
        if (name.Contains("Reader", StringComparison.OrdinalIgnoreCase)) return "source";

        return "unknown";
    }

    /// <summary>
    /// Extracts plugin metadata from attributes.
    /// Supports both PluginMetadataAttribute and legacy ConnectorMetadataAttribute.
    /// </summary>
    private static PluginMetadataAttribute? GetPluginMetadata(Type type)
    {
        try
        {
            var directAttr = type.GetCustomAttribute<PluginMetadataAttribute>();
            if (directAttr != null)
                return directAttr;

            // Fall back to name-based matching for cross-context or legacy attributes
            foreach (var attr in type.GetCustomAttributes(false))
            {
                var attrTypeName = attr.GetType().Name;
                if (attrTypeName is not ("PluginMetadataAttribute" or "ConnectorMetadataAttribute"))
                    continue;

                var attrType = attr.GetType();
                var name = attrType.GetProperty("Name")?.GetValue(attr) as string;
                var description = attrType.GetProperty("Description")?.GetValue(attr) as string;
                var icon = attrType.GetProperty("Icon")?.GetValue(attr) as string;
                var tags = attrType.GetProperty("Tags")?.GetValue(attr) as string;
                var author = attrType.GetProperty("Author")?.GetValue(attr) as string;
                var version = attrType.GetProperty("Version")?.GetValue(attr) as string;

                if (name != null)
                {
                    return new PluginMetadataAttribute
                    {
                        Name = name,
                        Description = description,
                        Icon = icon,
                        Tags = tags,
                        Author = author,
                        Version = version
                    };
                }
            }
        }
        catch
        {
            // Ignore reflection errors
        }

        return null;
    }

    /// <summary>
    /// Derives a category from a comma-separated tags string.
    /// </summary>
    internal static string DeriveCategory(string tags)
    {
        var tagList = tags.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var lowerTags = new HashSet<string>(tagList.Select(t => t.ToLowerInvariant()));

        if (lowerTags.Overlaps(["messaging", "chat", "bot"])) return "Messaging";
        if (lowerTags.Overlaps(["database", "sql", "nosql"])) return "Database";
        if (lowerTags.Overlaps(["cloud", "aws", "azure", "gcp"])) return "Cloud";
        if (lowerTags.Overlaps(["ai", "ml", "llm", "nlp"])) return "AI";
        if (lowerTags.Overlaps(["iot", "smart-home"])) return "IoT";
        if (lowerTags.Overlaps(["social", "social-media"])) return "Social";
        if (lowerTags.Overlaps(["stream", "streaming"])) return "Streaming";
        if (lowerTags.Overlaps(["file", "storage"])) return "Storage";
        if (lowerTags.Overlaps(["queue", "mq"])) return "Queue";
        if (lowerTags.Overlaps(["search"])) return "Search";
        if (lowerTags.Overlaps(["graph"])) return "Graph";
        if (lowerTags.Overlaps(["time-series"])) return "TimeSeries";
        if (lowerTags.Overlaps(["protocol", "transport"])) return "Transport";
        if (lowerTags.Overlaps(["logic", "transform"])) return "Logic";

        return "Integration";
    }

    private static string GetPluginVersion(Type type)
    {
        try
        {
            var versionProp = type.GetProperty("Version");
            if (versionProp != null)
            {
                var instance = Activator.CreateInstance(type);
                var version = versionProp.GetValue(instance) as string;
                (instance as IDisposable)?.Dispose();
                if (version != null) return version;
            }
        }
        catch
        {
            // Ignore — use assembly version
        }

        return type.Assembly.GetName().Version?.ToString() ?? "1.0.0";
    }

    private List<PluginInfo> GetBuiltInPlugins()
    {
        var plugins = new List<PluginInfo>();
        var assemblies = new List<Assembly>();

        lock (_lock)
        {
            assemblies.AddRange(_builtInAssemblies);
        }

        // Also scan loaded assemblies that look like plugin assemblies
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            var name = assembly.GetName().Name;
            if (name != null &&
                (name.StartsWith("Kuestenlogik.Surgewave.Connector.", StringComparison.OrdinalIgnoreCase) ||
                 name.Equals("Kuestenlogik.Surgewave.Connect", StringComparison.OrdinalIgnoreCase)) &&
                !assemblies.Contains(assembly))
            {
                assemblies.Add(assembly);
            }
        }

        foreach (var assembly in assemblies)
        {
            try
            {
                foreach (var type in assembly.GetExportedTypes())
                {
                    if (type.IsAbstract || type.IsInterface)
                        continue;

                    if (!ImplementsPluginInterface(type))
                        continue;

                    if (plugins.Any(p => p.Class == type.FullName))
                        continue;

                    var metadata = GetPluginMetadata(type);
                    plugins.Add(new PluginInfo
                    {
                        Class = type.FullName!,
                        Type = DeterminePluginType(type),
                        Version = GetPluginVersion(type),
                        DisplayName = metadata?.Name,
                        Description = metadata?.Description,
                        Icon = metadata?.Icon,
                        Category = metadata?.Tags != null
                            ? DeriveCategory(metadata.Tags)
                            : null
                    });
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to scan assembly for built-in plugins: {Assembly}", assembly.GetName().Name);
            }
        }

        return plugins;
    }
}
