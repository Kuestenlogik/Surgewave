using System.Reflection;
using System.Runtime.Loader;

namespace Kuestenlogik.Surgewave.Cli.Commands.Pipelines;

/// <summary>
/// Loads a user pipeline assembly in isolation while sharing the DSL contract assemblies with
/// the CLI's own load context — that keeps <c>ISurgewavePipeline</c> and the pipeline model
/// types identity-equal across the boundary. Collectible, so watch mode can reload on rebuild.
/// </summary>
internal sealed class PipelineAssemblyLoadContext : AssemblyLoadContext
{
    private static readonly string[] SharedAssemblies =
    [
        "Kuestenlogik.Surgewave.Pipelines",
        "Kuestenlogik.Surgewave.Connect.Abstractions",
    ];

    private readonly AssemblyDependencyResolver _resolver;

    public PipelineAssemblyLoadContext(string assemblyPath)
        : base($"pipelines:{Path.GetFileNameWithoutExtension(assemblyPath)}", isCollectible: true)
    {
        _resolver = new AssemblyDependencyResolver(assemblyPath);
    }

    protected override Assembly? Load(AssemblyName assemblyName)
    {
        if (SharedAssemblies.Contains(assemblyName.Name, StringComparer.OrdinalIgnoreCase))
        {
            return null; // defer to the default context — shared contract types
        }

        var path = _resolver.ResolveAssemblyToPath(assemblyName);
        return path is null ? null : LoadFromAssemblyPath(path);
    }

    protected override nint LoadUnmanagedDll(string unmanagedDllName)
    {
        var path = _resolver.ResolveUnmanagedDllToPath(unmanagedDllName);
        return path is null ? nint.Zero : LoadUnmanagedDllFromPath(path);
    }
}
