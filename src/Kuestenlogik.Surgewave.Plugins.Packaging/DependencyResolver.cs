namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// Resolves plugin dependencies using topological sorting with cycle detection.
/// Supports both synchronous (manifest-dictionary) and async (callback-based) resolution.
/// </summary>
public sealed class DependencyResolver
{
    private readonly Func<string, CancellationToken, Task<PluginManifest?>>? _manifestLoader;
    private readonly Func<string, string?>? _installedVersionProvider;

    /// <summary>
    /// Creates a resolver for synchronous resolution with pre-loaded manifests.
    /// </summary>
    public DependencyResolver() { }

    /// <summary>
    /// Creates a resolver for async resolution with on-demand manifest loading.
    /// </summary>
    /// <param name="manifestLoader">Async callback to load a manifest by plugin ID.</param>
    /// <param name="installedVersionProvider">Callback to check installed version by plugin ID.</param>
    public DependencyResolver(
        Func<string, CancellationToken, Task<PluginManifest?>> manifestLoader,
        Func<string, string?> installedVersionProvider)
    {
        _manifestLoader = manifestLoader;
        _installedVersionProvider = installedVersionProvider;
    }

    /// <summary>
    /// Resolves the installation order for a plugin and its dependencies (synchronous).
    /// </summary>
    public DependencyResolutionResult Resolve(
        PluginManifest rootManifest,
        IReadOnlyDictionary<string, PluginManifest> availablePlugins,
        IReadOnlyDictionary<string, InstalledPlugin> installedPlugins)
    {
        var toInstall = new List<ResolvedDependency>();
        var alreadyInstalled = new List<ResolvedDependency>();
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var inStack = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        Visit(rootManifest, rootManifest.Id, availablePlugins, installedPlugins, visited, inStack, toInstall, alreadyInstalled);

        return new DependencyResolutionResult(toInstall, alreadyInstalled);
    }

    /// <summary>
    /// Resolves the installation order for a plugin and its dependencies (async, callback-based).
    /// </summary>
    public async Task<DependencyResolutionResult> ResolveAsync(
        string rootPluginId,
        CancellationToken cancellationToken = default)
    {
        if (_manifestLoader == null || _installedVersionProvider == null)
            throw new InvalidOperationException("Async resolution requires manifestLoader and installedVersionProvider");

        var toInstall = new List<ResolvedDependency>();
        var alreadyInstalled = new List<ResolvedDependency>();
        var errors = new List<string>();
        var warnings = new List<string>();
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        await VisitAsync(rootPluginId, isRoot: true, visited, toInstall, alreadyInstalled, errors, warnings, cancellationToken);

        if (errors.Count > 0)
            return DependencyResolutionResult.Failed(errors, warnings);

        return DependencyResolutionResult.Succeeded(toInstall, alreadyInstalled, warnings);
    }

    private async Task VisitAsync(
        string pluginId,
        bool isRoot,
        HashSet<string> visited,
        List<ResolvedDependency> toInstall,
        List<ResolvedDependency> alreadyInstalled,
        List<string> errors,
        List<string> warnings,
        CancellationToken ct)
    {
        if (!visited.Add(pluginId))
            return;

        var installedVersion = _installedVersionProvider!(pluginId);
        var manifest = await _manifestLoader!(pluginId, ct);

        if (manifest == null)
        {
            if (installedVersion != null)
            {
                alreadyInstalled.Add(new ResolvedDependency(pluginId, installedVersion) { IsRoot = isRoot, InstalledVersion = installedVersion });
                return;
            }
            errors.Add($"Plugin {pluginId} not found");
            return;
        }

        if (manifest.SurgewaveDependencies != null)
        {
            foreach (var dep in manifest.SurgewaveDependencies)
            {
                await VisitAsync(dep.Id, isRoot: false, visited, toInstall, alreadyInstalled, errors, warnings, ct);
            }
        }

        toInstall.Add(new ResolvedDependency(manifest.Id, manifest.Version) { IsRoot = isRoot, InstalledVersion = installedVersion });
    }

    private static void Visit(
        PluginManifest manifest,
        string rootId,
        IReadOnlyDictionary<string, PluginManifest> available,
        IReadOnlyDictionary<string, InstalledPlugin> installed,
        HashSet<string> visited,
        HashSet<string> inStack,
        List<ResolvedDependency> toInstall,
        List<ResolvedDependency> alreadyInstalled)
    {
        if (!visited.Add(manifest.Id))
            return;

        if (!inStack.Add(manifest.Id))
            throw new InvalidOperationException($"Circular dependency detected involving {manifest.Id}");

        if (manifest.SurgewaveDependencies != null)
        {
            foreach (var dep in manifest.SurgewaveDependencies)
            {
                if (installed.TryGetValue(dep.Id, out var existing) && dep.IsSatisfiedBy(existing.Version))
                {
                    alreadyInstalled.Add(new ResolvedDependency(dep.Id, existing.Version));
                    continue;
                }

                if (available.TryGetValue(dep.Id, out var depManifest))
                {
                    Visit(depManifest, rootId, available, installed, visited, inStack, toInstall, alreadyInstalled);
                }
                else if (!dep.Optional)
                {
                    throw new InvalidOperationException(
                        $"Required dependency {dep.Id} (version {dep.Version}) not found");
                }
            }
        }

        toInstall.Add(new ResolvedDependency(manifest.Id, manifest.Version) { IsRoot = manifest.Id == rootId });
        inStack.Remove(manifest.Id);
    }
}

/// <summary>
/// A resolved dependency with its ID and version.
/// </summary>
public sealed record ResolvedDependency(string Id, string Version)
{
    /// <summary>Whether this is the root package (not a transitive dependency).</summary>
    public bool IsRoot { get; init; }

    /// <summary>Currently installed version, or null if not installed.</summary>
    public string? InstalledVersion { get; init; }
}

/// <summary>
/// Result of dependency resolution.
/// </summary>
public sealed record DependencyResolutionResult
{
    public bool IsSuccess { get; private init; }
    public IReadOnlyList<ResolvedDependency> ToInstall { get; private init; } = [];
    public IReadOnlyList<ResolvedDependency> AlreadyInstalled { get; private init; } = [];
    public IReadOnlyList<string> Errors { get; private init; } = [];
    public IReadOnlyList<string> Warnings { get; private init; } = [];

    public DependencyResolutionResult(IReadOnlyList<ResolvedDependency> toInstall, IReadOnlyList<ResolvedDependency> alreadyInstalled)
    {
        IsSuccess = true;
        ToInstall = toInstall;
        AlreadyInstalled = alreadyInstalled;
    }

    public static DependencyResolutionResult Succeeded(
        IReadOnlyList<ResolvedDependency> toInstall,
        IReadOnlyList<ResolvedDependency> alreadyInstalled,
        IReadOnlyList<string>? warnings = null) =>
        new(toInstall, alreadyInstalled) { Warnings = warnings ?? [] };

    public static DependencyResolutionResult Failed(
        IReadOnlyList<string> errors,
        IReadOnlyList<string>? warnings = null) =>
        new([], []) { IsSuccess = false, Errors = errors, Warnings = warnings ?? [] };
}
