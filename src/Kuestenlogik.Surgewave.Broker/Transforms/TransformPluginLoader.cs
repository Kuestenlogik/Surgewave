using System.Reflection;
using System.Runtime.Loader;
using Kuestenlogik.Surgewave.Core.Transforms;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Transforms;

/// <summary>
/// Loads transform plugin assemblies with proper isolation via AssemblyLoadContext.
/// Supports loading from individual assemblies or scanning a directory.
/// </summary>
public sealed partial class TransformPluginLoader : IDisposable
{
    private readonly ILogger<TransformPluginLoader> _logger;
    private readonly List<LoadedTransformAssembly> _loaded = [];
    private readonly Lock _lock = new();

    public TransformPluginLoader(ILogger<TransformPluginLoader> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Loads a single assembly and returns all IInlineTransform implementations found in it.
    /// Each discovered transform is instantiated via its parameterless constructor.
    /// </summary>
    public IReadOnlyList<IInlineTransform> LoadFromAssembly(string assemblyPath)
    {
        lock (_lock)
        {
            try
            {
                var loadContext = new TransformAssemblyLoadContext(assemblyPath);
                var assembly = loadContext.LoadFromAssemblyPath(assemblyPath);
                var transforms = DiscoverTransforms(assembly);

                _loaded.Add(new LoadedTransformAssembly(assemblyPath, loadContext, assembly));
                LogAssemblyLoaded(assemblyPath, transforms.Count);

                return transforms;
            }
            catch (Exception ex)
            {
                LogAssemblyLoadFailed(assemblyPath, ex);
                return [];
            }
        }
    }

    /// <summary>
    /// Scans a directory for DLLs containing IInlineTransform implementations.
    /// Returns all discovered transforms across all assemblies.
    /// </summary>
    public IReadOnlyList<IInlineTransform> LoadFromDirectory(string pluginsDir)
    {
        if (!Directory.Exists(pluginsDir))
        {
            LogDirectoryNotFound(pluginsDir);
            return [];
        }

        var allTransforms = new List<IInlineTransform>();
        var dllFiles = Directory.GetFiles(pluginsDir, "*.dll", SearchOption.TopDirectoryOnly);

        foreach (var dll in dllFiles)
        {
            var transforms = LoadFromAssembly(dll);
            allTransforms.AddRange(transforms);
        }

        LogDirectoryScanned(pluginsDir, allTransforms.Count);
        return allTransforms;
    }

    private static List<IInlineTransform> DiscoverTransforms(Assembly assembly)
    {
        var transforms = new List<IInlineTransform>();
        var transformInterface = typeof(IInlineTransform);

        foreach (var type in assembly.GetExportedTypes())
        {
            if (!type.IsAbstract && !type.IsInterface && transformInterface.IsAssignableFrom(type))
            {
                var instance = Activator.CreateInstance(type) as IInlineTransform;
                if (instance != null)
                {
                    transforms.Add(instance);
                }
            }
        }

        return transforms;
    }

    public void Dispose()
    {
        lock (_lock)
        {
            foreach (var loaded in _loaded)
            {
                try
                {
                    loaded.LoadContext.Unload();
                }
                catch (Exception ex)
                {
                    LogUnloadFailed(loaded.Path, ex);
                }
            }
            _loaded.Clear();
        }
    }

    private sealed record LoadedTransformAssembly(
        string Path,
        AssemblyLoadContext LoadContext,
        Assembly Assembly);

    [LoggerMessage(Level = LogLevel.Information, Message = "Loaded transform assembly {AssemblyPath} with {Count} transform(s)")]
    private partial void LogAssemblyLoaded(string assemblyPath, int count);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to load transform assembly {AssemblyPath}")]
    private partial void LogAssemblyLoadFailed(string assemblyPath, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Transform plugins directory not found: {PluginsDir}")]
    private partial void LogDirectoryNotFound(string pluginsDir);

    [LoggerMessage(Level = LogLevel.Information, Message = "Scanned {PluginsDir}, discovered {Count} transform(s)")]
    private partial void LogDirectoryScanned(string pluginsDir, int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Error unloading transform assembly: {Path}")]
    private partial void LogUnloadFailed(string path, Exception ex);
}
