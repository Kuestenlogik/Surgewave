using System.Reflection;

namespace Kuestenlogik.Surgewave.Plugins;

/// <summary>
/// Shared assembly-scanning helper for plugin discovery. Both
/// <see cref="PluginDiscovery"/> (connector plugins, filesystem-scanned) and
/// <c>BrokerPluginActivator</c> (broker plugins, AppDomain-scanned) need to
/// walk a set of assemblies, find concrete types implementing a given interface,
/// and instantiate them. This utility centralises the inner loop so type
/// filtering, <see cref="ReflectionTypeLoadException"/> handling and
/// <see cref="Activator.CreateInstance(Type)"/> are done in one place.
/// </summary>
public static class PluginAssemblyScanner
{
    /// <summary>
    /// Scans <paramref name="assemblies"/> for concrete, non-abstract public types
    /// that implement <typeparamref name="T"/>, instantiates them via parameterless
    /// constructor, and yields each instance. Assemblies that throw during type
    /// enumeration (e.g. missing transitive deps) are silently skipped — the
    /// discovery pass is best-effort, not validation.
    /// </summary>
    public static IEnumerable<T> FindImplementations<T>(IEnumerable<Assembly> assemblies) where T : class
    {
        foreach (var assembly in assemblies)
        {
            Type[] types;
            try
            {
                types = assembly.GetExportedTypes();
            }
            catch (ReflectionTypeLoadException ex)
            {
                // Partial load — scan what we can.
                types = ex.Types.Where(t => t is not null).ToArray()!;
            }
            catch
            {
                continue;
            }

            foreach (var type in types)
            {
                if (type.IsAbstract || type.IsInterface) continue;
                if (!typeof(T).IsAssignableFrom(type)) continue;
                if (type.GetConstructor(Type.EmptyTypes) is null) continue;

                T? instance;
                try
                {
                    instance = Activator.CreateInstance(type) as T;
                }
                catch
                {
                    continue;
                }

                if (instance is not null)
                    yield return instance;
            }
        }
    }
}
