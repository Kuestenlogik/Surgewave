using System.Reflection;
using System.Runtime.Loader;

namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// Discovers and instantiates <see cref="ISppSigner"/> implementations from installed plugins.
/// The <see cref="BuiltinEcdsaSignerProvider"/> is always present; additional providers are
/// loaded from subdirectories under a plugins root via per-plugin <see cref="AssemblyLoadContext"/>
/// instances so a crashed or incompatible signer plugin cannot corrupt the host's load context.
/// </summary>
/// <remarks>
/// <para>
/// Convention: each plugin directory is scanned for DLLs whose <c>plugin.json</c> manifest lists
/// assemblies containing types implementing <see cref="ISppSignerProvider"/>. For simplicity,
/// this registry scans <c>*.dll</c> files directly in the plugin folder (and <c>lib/</c>, matching
/// the .swpkg install layout). Types that cannot be loaded or instantiated are logged and skipped —
/// a malformed plugin should not break signer discovery for the rest.
/// </para>
/// <para>
/// The registry is disposable because each plugin load context holds file handles on the loaded
/// DLLs; dispose when shutting down to allow the runtime to collect the contexts.
/// </para>
/// </remarks>
public sealed class PluginPackageSignerRegistry : IDisposable
{
    private readonly Dictionary<string, ISppSignerProvider> _providers = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<AssemblyLoadContext> _loadContexts = [];

    /// <summary>All known providers, keyed by <see cref="ISppSignerProvider.Name"/>.</summary>
    public IReadOnlyDictionary<string, ISppSignerProvider> Providers => _providers;

    private PluginPackageSignerRegistry() { }

    /// <summary>
    /// Builds a registry pre-populated with the built-in ECDSA provider and any additional
    /// providers discovered under the given plugin directories. Missing or unreadable
    /// directories are silently skipped.
    /// </summary>
    public static PluginPackageSignerRegistry LoadFrom(params string[] pluginDirs)
    {
        var registry = new PluginPackageSignerRegistry();
        registry.Register(new BuiltinEcdsaSignerProvider());

        foreach (var dir in pluginDirs)
        {
            if (string.IsNullOrEmpty(dir) || !Directory.Exists(dir))
                continue;

            registry.DiscoverFromDirectory(dir);
        }

        return registry;
    }

    /// <summary>Resolves a provider by name. Matching is case-insensitive.</summary>
    public ISppSignerProvider GetProvider(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (!_providers.TryGetValue(name, out var provider))
        {
            var known = string.Join(", ", _providers.Keys);
            throw new KeyNotFoundException(
                $"No signer provider named '{name}' is registered. Known providers: {known}");
        }
        return provider;
    }

    private void Register(ISppSignerProvider provider)
    {
        _providers[provider.Name] = provider;
    }

    private void DiscoverFromDirectory(string rootDir)
    {
        foreach (var pluginDir in Directory.EnumerateDirectories(rootDir))
        {
            var libDir = Path.Combine(pluginDir, "lib");
            var scanDir = Directory.Exists(libDir) ? libDir : pluginDir;

            var context = new AssemblyLoadContext(Path.GetFileName(pluginDir), isCollectible: true);
            _loadContexts.Add(context);

            foreach (var dll in Directory.EnumerateFiles(scanDir, "*.dll"))
            {
                Assembly asm;
                try
                {
                    asm = context.LoadFromAssemblyPath(dll);
                }
                catch (BadImageFormatException) { continue; }
                catch (FileLoadException) { continue; }

                foreach (var type in SafeGetTypes(asm))
                {
                    if (!typeof(ISppSignerProvider).IsAssignableFrom(type) ||
                        type.IsAbstract ||
                        type.IsInterface)
                        continue;

                    try
                    {
                        if (Activator.CreateInstance(type) is ISppSignerProvider provider)
                            Register(provider);
                    }
                    catch (MissingMethodException) { /* no parameterless ctor */ }
                    catch (TargetInvocationException) { /* constructor threw */ }
                }
            }
        }
    }

    private static IEnumerable<Type> SafeGetTypes(Assembly asm)
    {
        try
        {
            return asm.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            return ex.Types.Where(t => t != null)!;
        }
    }

    public void Dispose()
    {
        foreach (var ctx in _loadContexts)
        {
            try { ctx.Unload(); }
            catch (InvalidOperationException) { /* non-collectible or already unloaded */ }
        }
        _loadContexts.Clear();
        _providers.Clear();
    }
}
