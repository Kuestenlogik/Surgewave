using System.Reflection;
using Kuestenlogik.Surgewave.Pipelines;

namespace Kuestenlogik.Surgewave.Cli.Commands.Pipelines;

/// <summary>
/// Discovers <see cref="ISurgewavePipeline"/> implementations in a compiled assembly and builds
/// their pipeline definitions — the deploy path for pipeline-as-code libraries.
/// </summary>
internal static class PipelineAssemblyScanner
{
    /// <summary>
    /// Loads <paramref name="assemblyPath"/> in an isolated, collectible context, builds every
    /// discovered pipeline, and unloads the context again. The target assembly is loaded from
    /// a byte stream so the file itself is never locked, and the unload is driven to
    /// completion so dependency files are free again for the next rebuild (watch mode).
    /// </summary>
    public static IReadOnlyList<ScannedPipeline> Scan(string assemblyPath)
    {
        var fullPath = Path.GetFullPath(assemblyPath);
        if (!File.Exists(fullPath))
        {
            throw new FileNotFoundException($"Assembly not found: {fullPath}", fullPath);
        }

        var (result, contextRef) = ScanCore(fullPath);

        // Unload() only completes once the GC collects the context — an idle CLI process
        // may never trigger that on its own, keeping dependency dlls locked.
        for (var i = 0; contextRef.IsAlive && i < 10; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        return result;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static (IReadOnlyList<ScannedPipeline> Result, WeakReference ContextRef) ScanCore(string fullPath)
    {
        var context = new PipelineAssemblyLoadContext(fullPath);
        try
        {
            using var stream = File.OpenRead(fullPath);
            var assembly = context.LoadFromStream(stream);
            return (ScanAssembly(assembly), new WeakReference(context));
        }
        finally
        {
            context.Unload();
        }
    }

    private static List<ScannedPipeline> ScanAssembly(Assembly assembly)
    {
        Type[] types;
        try
        {
            types = assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            types = ex.Types.Where(t => t is not null).ToArray()!;
        }

        var results = new List<ScannedPipeline>();

        foreach (var type in types)
        {
            if (type is not { IsClass: true, IsAbstract: false } || !typeof(ISurgewavePipeline).IsAssignableFrom(type))
            {
                continue;
            }

            ISurgewavePipeline instance;
            try
            {
                instance = (ISurgewavePipeline)Activator.CreateInstance(type)!;
            }
            catch (Exception ex)
            {
                throw new PipelineBuildException(
                    $"Cannot instantiate pipeline class {type.FullName} — it needs a public parameterless constructor. ({ex.Message})", ex);
            }

            BuiltPipeline pipeline;
            try
            {
                pipeline = instance.Define();
            }
            catch (PipelineBuildException ex)
            {
                throw new PipelineBuildException($"{type.FullName}: {ex.Message}", ex);
            }

            if (pipeline.Name is null)
            {
                pipeline = pipeline with { Name = KebabCase(type.Name) };
            }

            results.Add(new ScannedPipeline { TypeName = type.FullName ?? type.Name, Pipeline = pipeline });
        }

        return results;
    }

    private static string KebabCase(string typeName)
    {
        var result = new System.Text.StringBuilder(typeName.Length + 4);
        foreach (var c in typeName)
        {
            if (char.IsUpper(c))
            {
                if (result.Length > 0)
                {
                    result.Append('-');
                }

                result.Append(char.ToLowerInvariant(c));
            }
            else
            {
                result.Append(c);
            }
        }

        return result.ToString();
    }
}
