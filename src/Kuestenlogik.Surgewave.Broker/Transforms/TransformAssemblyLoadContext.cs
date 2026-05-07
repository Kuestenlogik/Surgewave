using System.Reflection;
using System.Runtime.Loader;

namespace Kuestenlogik.Surgewave.Broker.Transforms;

/// <summary>
/// Custom AssemblyLoadContext for loading transform plugins in isolation.
/// Each transform assembly gets its own collectible context to allow unloading.
/// </summary>
internal sealed class TransformAssemblyLoadContext : AssemblyLoadContext
{
    private readonly AssemblyDependencyResolver _resolver;

    public TransformAssemblyLoadContext(string pluginPath) : base(isCollectible: true)
    {
        _resolver = new AssemblyDependencyResolver(pluginPath);
    }

    protected override Assembly? Load(AssemblyName assemblyName)
    {
        // Try to resolve from the plugin's own dependencies first
        var assemblyPath = _resolver.ResolveAssemblyToPath(assemblyName);
        if (assemblyPath != null)
        {
            return LoadFromAssemblyPath(assemblyPath);
        }

        // Fall back to default context for shared assemblies (Kuestenlogik.Surgewave.Core interfaces)
        return null;
    }

    protected override nint LoadUnmanagedDll(string unmanagedDllName)
    {
        var libraryPath = _resolver.ResolveUnmanagedDllToPath(unmanagedDllName);
        if (libraryPath != null)
        {
            return LoadUnmanagedDllFromPath(libraryPath);
        }

        return nint.Zero;
    }
}
