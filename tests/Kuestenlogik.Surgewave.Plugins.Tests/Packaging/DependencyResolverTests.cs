using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

/// <summary>
/// Coverage fuer <see cref="DependencyResolver"/> + <see cref="ResolvedDependency"/> +
/// <see cref="DependencyResolutionResult"/>. Topo-Sort + Cycle-Detection + Already-installed
/// short-circuiting laufen in beiden Modi (sync via Dict, async via callback).
/// </summary>
public sealed class DependencyResolverTests
{
    private static PluginManifest Manifest(string id, string version, params PluginDependency[] deps) => new()
    {
        Id = id,
        Name = id,
        Version = version,
        Assemblies = [$"{id}.dll"],
        SurgewaveDependencies = deps.Length == 0 ? null : deps,
    };

    private static PluginDependency Dep(string id, string version = "*", bool optional = false) =>
        new() { Id = id, Version = version, Optional = optional };

    private static InstalledPlugin Installed(string id, string version) => new()
    {
        Id = id,
        Name = id,
        Version = version,
        InstallPath = $"/plugins/{id}",
        Manifest = Manifest(id, version),
    };

    // --- Sync: Resolve ---

    [Fact]
    public void Resolve_LeafPlugin_OnlyContainsRoot()
    {
        var root = Manifest("a", "1.0.0");
        var resolver = new DependencyResolver();

        var result = resolver.Resolve(
            root,
            availablePlugins: new Dictionary<string, PluginManifest>(),
            installedPlugins: new Dictionary<string, InstalledPlugin>());

        Assert.True(result.IsSuccess);
        Assert.Single(result.ToInstall);
        Assert.Equal("a", result.ToInstall[0].Id);
        Assert.True(result.ToInstall[0].IsRoot);
        Assert.Empty(result.AlreadyInstalled);
    }

    [Fact]
    public void Resolve_LinearChain_TopologicalOrderDepFirstRootLast()
    {
        // a -> b -> c    expected install order: c, b, a (deps first)
        var c = Manifest("c", "1.0.0");
        var b = Manifest("b", "1.0.0", Dep("c"));
        var a = Manifest("a", "1.0.0", Dep("b"));
        var available = new Dictionary<string, PluginManifest>
        {
            ["b"] = b,
            ["c"] = c,
        };

        var result = new DependencyResolver().Resolve(a, available, new Dictionary<string, InstalledPlugin>());

        Assert.True(result.IsSuccess);
        Assert.Equal(3, result.ToInstall.Count);
        Assert.Equal("c", result.ToInstall[0].Id);
        Assert.Equal("b", result.ToInstall[1].Id);
        Assert.Equal("a", result.ToInstall[2].Id);
        Assert.True(result.ToInstall[2].IsRoot);
        Assert.False(result.ToInstall[0].IsRoot);
    }

    [Fact]
    public void Resolve_DiamondGraph_DedupesSharedDependency()
    {
        // a -> b, a -> c, b -> d, c -> d    => d only listed once
        var d = Manifest("d", "1.0.0");
        var b = Manifest("b", "1.0.0", Dep("d"));
        var c = Manifest("c", "1.0.0", Dep("d"));
        var a = Manifest("a", "1.0.0", Dep("b"), Dep("c"));
        var available = new Dictionary<string, PluginManifest>
        {
            ["b"] = b,
            ["c"] = c,
            ["d"] = d,
        };

        var result = new DependencyResolver().Resolve(a, available, new Dictionary<string, InstalledPlugin>());

        Assert.Equal(4, result.ToInstall.Count);
        Assert.Single(result.ToInstall, r => r.Id == "d");
    }

    [Fact]
    public void Resolve_AlreadyInstalledSatisfyingVersion_SkipsTransitive()
    {
        // a depends on b@>=1.0.0, b is installed @1.0.0 -> b not visited, only a queued
        var a = Manifest("a", "1.0.0", Dep("b", ">=1.0.0"));
        var installed = new Dictionary<string, InstalledPlugin>
        {
            ["b"] = Installed("b", "1.0.0"),
        };

        var result = new DependencyResolver().Resolve(a, new Dictionary<string, PluginManifest>(), installed);

        Assert.True(result.IsSuccess);
        Assert.Single(result.ToInstall, r => r.Id == "a");
        Assert.Single(result.AlreadyInstalled);
        Assert.Equal("b", result.AlreadyInstalled[0].Id);
        Assert.Equal("1.0.0", result.AlreadyInstalled[0].Version);
    }

    [Fact]
    public void Resolve_MissingRequiredDependency_Throws()
    {
        var a = Manifest("a", "1.0.0", Dep("missing", "1.0.0"));

        Assert.Throws<InvalidOperationException>(() =>
            new DependencyResolver().Resolve(a, new Dictionary<string, PluginManifest>(), new Dictionary<string, InstalledPlugin>()));
    }

    [Fact]
    public void Resolve_MissingOptionalDependency_DoesNotThrow()
    {
        var a = Manifest("a", "1.0.0", Dep("missing", "1.0.0", optional: true));

        var result = new DependencyResolver().Resolve(a, new Dictionary<string, PluginManifest>(), new Dictionary<string, InstalledPlugin>());

        Assert.True(result.IsSuccess);
        Assert.Single(result.ToInstall);
    }

    [Fact]
    public void Resolve_CycleDedupedViaVisited_BothListedOnce()
    {
        // a <-> b. The visited-set short-circuits the second visit so the resolver
        // returns successfully with each node listed once — practical cycles are
        // tolerated rather than throwing.
        var b = Manifest("b", "1.0.0", Dep("a"));
        var a = Manifest("a", "1.0.0", Dep("b"));
        var available = new Dictionary<string, PluginManifest>
        {
            ["a"] = a,
            ["b"] = b,
        };

        var result = new DependencyResolver().Resolve(a, available, new Dictionary<string, InstalledPlugin>());

        Assert.True(result.IsSuccess);
        Assert.Equal(2, result.ToInstall.Count);
        Assert.Single(result.ToInstall, r => r.Id == "a");
        Assert.Single(result.ToInstall, r => r.Id == "b");
    }

    // --- Async: ResolveAsync ---

    [Fact]
    public async Task ResolveAsync_WithoutCallbacks_Throws()
    {
        var resolver = new DependencyResolver();

        await Assert.ThrowsAsync<InvalidOperationException>(() => resolver.ResolveAsync("x"));
    }

    [Fact]
    public async Task ResolveAsync_LinearChain_OrdersDependenciesFirst()
    {
        var graph = new Dictionary<string, PluginManifest>
        {
            ["a"] = Manifest("a", "1.0.0", Dep("b")),
            ["b"] = Manifest("b", "1.0.0", Dep("c")),
            ["c"] = Manifest("c", "1.0.0"),
        };
        var resolver = new DependencyResolver(
            manifestLoader: (id, _) => Task.FromResult(graph.TryGetValue(id, out var m) ? m : null),
            installedVersionProvider: _ => null);

        var result = await resolver.ResolveAsync("a");

        Assert.True(result.IsSuccess);
        Assert.Equal(["c", "b", "a"], result.ToInstall.Select(r => r.Id));
        Assert.True(result.ToInstall[^1].IsRoot);
    }

    [Fact]
    public async Task ResolveAsync_MissingPluginNotInstalled_RecordsError()
    {
        var resolver = new DependencyResolver(
            manifestLoader: (_, _) => Task.FromResult<PluginManifest?>(null),
            installedVersionProvider: _ => null);

        var result = await resolver.ResolveAsync("ghost");

        Assert.False(result.IsSuccess);
        Assert.NotEmpty(result.Errors);
        Assert.Contains(result.Errors, e => e.Contains("ghost", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task ResolveAsync_MissingManifestButInstalled_MarksAsAlreadyInstalled()
    {
        var resolver = new DependencyResolver(
            manifestLoader: (_, _) => Task.FromResult<PluginManifest?>(null),
            installedVersionProvider: id => id == "already" ? "1.2.3" : null);

        var result = await resolver.ResolveAsync("already");

        Assert.True(result.IsSuccess);
        Assert.Single(result.AlreadyInstalled);
        Assert.Equal("1.2.3", result.AlreadyInstalled[0].Version);
        Assert.True(result.AlreadyInstalled[0].IsRoot);
    }

    [Fact]
    public async Task ResolveAsync_RevisitingSamePlugin_OnlyVisitedOnce()
    {
        // Diamond: a -> b, a -> c, b -> d, c -> d. d should only appear once.
        var graph = new Dictionary<string, PluginManifest>
        {
            ["a"] = Manifest("a", "1.0.0", Dep("b"), Dep("c")),
            ["b"] = Manifest("b", "1.0.0", Dep("d")),
            ["c"] = Manifest("c", "1.0.0", Dep("d")),
            ["d"] = Manifest("d", "1.0.0"),
        };
        var loadCount = new Dictionary<string, int>();
        var resolver = new DependencyResolver(
            manifestLoader: (id, _) =>
            {
                loadCount[id] = loadCount.GetValueOrDefault(id) + 1;
                return Task.FromResult(graph.TryGetValue(id, out var m) ? m : null);
            },
            installedVersionProvider: _ => null);

        var result = await resolver.ResolveAsync("a");

        Assert.True(result.IsSuccess);
        Assert.Equal(4, result.ToInstall.Count);
        Assert.Equal(1, loadCount["d"]);
    }

    [Fact]
    public async Task ResolveAsync_PropagatesCancellation()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var resolver = new DependencyResolver(
            manifestLoader: (_, ct) => { ct.ThrowIfCancellationRequested(); return Task.FromResult<PluginManifest?>(null); },
            installedVersionProvider: _ => null);

        await Assert.ThrowsAsync<OperationCanceledException>(() => resolver.ResolveAsync("a", cts.Token));
    }

    // --- ResolvedDependency / DependencyResolutionResult ---

    [Fact]
    public void ResolvedDependency_RecordEquality_ByValue()
    {
        var a = new ResolvedDependency("x", "1.0.0") { IsRoot = true, InstalledVersion = "0.9.0" };
        var b = new ResolvedDependency("x", "1.0.0") { IsRoot = true, InstalledVersion = "0.9.0" };

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void ResolvedDependency_Defaults_InstalledVersionNull_IsRootFalse()
    {
        var d = new ResolvedDependency("x", "1.0.0");
        Assert.False(d.IsRoot);
        Assert.Null(d.InstalledVersion);
    }

    [Fact]
    public void DependencyResolutionResult_Succeeded_Factory_DefaultsAndWarnings()
    {
        var r = DependencyResolutionResult.Succeeded(
            toInstall: [new ResolvedDependency("a", "1.0.0")],
            alreadyInstalled: [new ResolvedDependency("b", "1.0.0")],
            warnings: ["heads-up"]);

        Assert.True(r.IsSuccess);
        Assert.Single(r.ToInstall);
        Assert.Single(r.AlreadyInstalled);
        Assert.Single(r.Warnings, "heads-up");
        Assert.Empty(r.Errors);
    }

    [Fact]
    public void DependencyResolutionResult_Failed_Factory_ClearsListsKeepsErrors()
    {
        var r = DependencyResolutionResult.Failed(["boom"], warnings: ["w"]);

        Assert.False(r.IsSuccess);
        Assert.Empty(r.ToInstall);
        Assert.Empty(r.AlreadyInstalled);
        Assert.Single(r.Errors, "boom");
        Assert.Single(r.Warnings, "w");
    }
}
