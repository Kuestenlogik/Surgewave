using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Repository.Tests;

/// <summary>
/// Tests fuer <see cref="ConnectorRepositoryManager"/> — Orchestrierung mehrerer
/// IConnectorRepository-Implementierungen plus ConnectorInstaller. Wir injizieren
/// <see cref="FakeRepository"/>-Instanzen via <c>AddRepository</c> und entfernen das
/// per-default angemeldete <c>nuget.org</c>, sodass keine Netz-Anfragen passieren.
/// </summary>
public sealed class ConnectorRepositoryManagerTests : IDisposable
{
    private readonly string _installRoot;

    public ConnectorRepositoryManagerTests()
    {
        _installRoot = Path.Combine(Path.GetTempPath(), $"sw-mgr-tests-{Guid.NewGuid():N}");
    }

    private ConnectorRepositoryManager NewManager()
    {
        var mgr = new ConnectorRepositoryManager(_installRoot);
        // Drop the auto-registered nuget.org so tests are deterministic.
        mgr.RemoveRepository("nuget.org");
        return mgr;
    }

    [Fact]
    public void Constructor_RegistersNuGetOrgByDefault()
    {
        using var mgr = new ConnectorRepositoryManager(_installRoot);

        Assert.Single(mgr.Repositories);
        Assert.Equal("nuget.org", mgr.Repositories[0].Name);
    }

    [Fact]
    public void AddRepository_AppendsToList()
    {
        using var mgr = NewManager();

        mgr.AddRepository(new FakeRepository("custom"));

        Assert.Single(mgr.Repositories);
        Assert.Equal("custom", mgr.Repositories[0].Name);
    }

    [Fact]
    public void RemoveRepository_DisposesAndRemoves()
    {
        using var mgr = NewManager();
        var fake = new FakeRepository("disposable");
        mgr.AddRepository(fake);

        mgr.RemoveRepository("disposable");

        Assert.Empty(mgr.Repositories);
        Assert.True(fake.Disposed);
    }

    [Fact]
    public void RemoveRepository_UnknownName_NoOp()
    {
        using var mgr = NewManager();
        mgr.AddRepository(new FakeRepository("a"));

        mgr.RemoveRepository("not-there");

        Assert.Single(mgr.Repositories);
    }

    [Fact]
    public async Task SearchAsync_DedupesAcrossRepositories_AndAddsInstalledStatus()
    {
        using var mgr = NewManager();
        mgr.AddRepository(new FakeRepository("a")
        {
            Packages =
            {
                ["pkg.A"] = Package("pkg.A", "1.0.0", downloads: 100),
            },
        });
        mgr.AddRepository(new FakeRepository("b")
        {
            Packages =
            {
                ["pkg.A"] = Package("pkg.A", "1.0.0", downloads: 999),   // dedupe — first wins
                ["pkg.B"] = Package("pkg.B", "2.0.0", downloads: 200),
            },
        });

        var results = await mgr.SearchAsync(query: null);

        Assert.Equal(2, results.Count);
        // Sorted desc by DownloadCount; pkg.B (200) > pkg.A (100)
        Assert.Equal("pkg.B", results[0].PackageId);
        Assert.Equal("pkg.A", results[1].PackageId);
        Assert.All(results, p => Assert.False(p.IsInstalled));
    }

    [Fact]
    public async Task SearchAsync_RepositoryThrows_SkipsButContinues()
    {
        using var mgr = NewManager();
        mgr.AddRepository(new FakeRepository("broken") { ThrowOnSearch = true });
        mgr.AddRepository(new FakeRepository("ok")
        {
            Packages = { ["x"] = Package("x", "1.0.0") },
        });

        var results = await mgr.SearchAsync(query: null);

        Assert.Single(results);
        Assert.Equal("x", results[0].PackageId);
    }

    [Fact]
    public async Task GetPackageAsync_ReturnsFromFirstRepoThatHasIt()
    {
        using var mgr = NewManager();
        mgr.AddRepository(new FakeRepository("a") { Packages = { ["pkg.A"] = Package("pkg.A", "1.0.0") } });
        mgr.AddRepository(new FakeRepository("b") { Packages = { ["pkg.A"] = Package("pkg.A", "2.0.0") } });

        var pkg = await mgr.GetPackageAsync("pkg.A");

        Assert.NotNull(pkg);
        Assert.Equal("1.0.0", pkg!.Version);
    }

    [Fact]
    public async Task GetPackageAsync_AllReposFail_ReturnsNull()
    {
        using var mgr = NewManager();
        mgr.AddRepository(new FakeRepository("a") { ThrowOnGetPackage = true });
        mgr.AddRepository(new FakeRepository("b"));

        var pkg = await mgr.GetPackageAsync("missing");

        Assert.Null(pkg);
    }

    [Fact]
    public async Task ResolveDependenciesAsync_NoDependencies_SuccessWithSingleEntry()
    {
        using var mgr = NewManager();
        mgr.AddRepository(new FakeRepository("a")
        {
            Packages = { ["root"] = Package("root", "1.0.0") },
        });

        var result = await mgr.ResolveDependenciesAsync("root");

        Assert.True(result.IsSuccess);
        Assert.Single(result.ToInstall);
        Assert.Equal("root", result.ToInstall[0].Id);
        Assert.True(result.ToInstall[0].IsRoot);
    }

    [Fact]
    public async Task ResolveDependenciesAsync_WithMissingPackage_RecordsError()
    {
        using var mgr = NewManager();
        mgr.AddRepository(new FakeRepository("a"));  // empty

        var result = await mgr.ResolveDependenciesAsync("nonexistent");

        Assert.False(result.IsSuccess);
        Assert.NotEmpty(result.Errors);
    }

    [Fact]
    public async Task GetDependencyTreeAsync_PackageNotFound_ReturnsNull()
    {
        using var mgr = NewManager();
        mgr.AddRepository(new FakeRepository("a"));

        var tree = await mgr.GetDependencyTreeAsync("ghost");

        Assert.Null(tree);
    }

    [Fact]
    public async Task GetDependencyTreeAsync_NoDependencies_ReturnsRootLeaf()
    {
        using var mgr = NewManager();
        mgr.AddRepository(new FakeRepository("a")
        {
            Packages = { ["leaf"] = Package("leaf", "1.0.0") },
        });

        var tree = await mgr.GetDependencyTreeAsync("leaf");

        Assert.NotNull(tree);
        Assert.Equal("leaf", tree!.PackageId);
        Assert.Empty(tree.Children);
    }

    [Fact]
    public async Task InstallAsync_PackageNotFound_Throws()
    {
        using var mgr = NewManager();
        mgr.AddRepository(new FakeRepository("a"));

        await Assert.ThrowsAsync<InvalidOperationException>(() => mgr.InstallAsync("nope"));
    }

    [Fact]
    public async Task UpdateAsync_PackageNotFound_Throws()
    {
        using var mgr = NewManager();
        mgr.AddRepository(new FakeRepository("a"));

        await Assert.ThrowsAsync<InvalidOperationException>(() => mgr.UpdateAsync("nope"));
    }

    [Fact]
    public void Uninstall_UnknownPackage_NoOp()
    {
        using var mgr = NewManager();

        // Should not throw — delegates to installer which silently skips unknowns.
        mgr.Uninstall("never.installed");
    }

    [Fact]
    public void Installer_Property_ExposesUnderlyingInstaller()
    {
        using var mgr = NewManager();

        Assert.NotNull(mgr.Installer);
        Assert.IsType<ConnectorInstaller>(mgr.Installer);
    }

    [Fact]
    public async Task InstallWithDependenciesAsync_ResolutionFails_ReturnsFailedResult()
    {
        using var mgr = NewManager();
        mgr.AddRepository(new FakeRepository("a"));  // empty

        var result = await mgr.InstallWithDependenciesAsync("nonexistent");

        Assert.False(result.IsSuccess);
        Assert.NotEmpty(result.Errors);
    }

    [Fact]
    public void Dispose_DisposesAllRepositoriesAndInstaller()
    {
        var mgr = new ConnectorRepositoryManager(_installRoot);
        mgr.RemoveRepository("nuget.org");
        var f1 = new FakeRepository("f1");
        var f2 = new FakeRepository("f2");
        mgr.AddRepository(f1);
        mgr.AddRepository(f2);

        mgr.Dispose();

        Assert.True(f1.Disposed);
        Assert.True(f2.Disposed);
        Assert.Empty(mgr.Repositories);
    }

    private static ConnectorPackageInfo Package(string id, string version, long downloads = 0) => new()
    {
        PackageId = id,
        Version = version,
        Name = id,
        DownloadCount = downloads,
        AvailableVersions = [version],
    };

    public void Dispose()
    {
        if (Directory.Exists(_installRoot))
        {
            try { Directory.Delete(_installRoot, recursive: true); }
            catch { /* best effort */ }
        }
    }

    private sealed class FakeRepository : IConnectorRepository, IDisposable
    {
        public string Name { get; }
        public string Source => "fake://" + Name;
        public Dictionary<string, ConnectorPackageInfo> Packages { get; } = new(StringComparer.OrdinalIgnoreCase);
        public bool ThrowOnSearch { get; set; }
        public bool ThrowOnGetPackage { get; set; }
        public bool Disposed { get; private set; }

        public FakeRepository(string name) => Name = name;

        public Task<IReadOnlyList<ConnectorPackageInfo>> SearchAsync(string? query, int skip = 0, int take = 20, CancellationToken cancellationToken = default)
        {
            if (ThrowOnSearch) throw new InvalidOperationException("repo down");
            IReadOnlyList<ConnectorPackageInfo> list = Packages.Values.Skip(skip).Take(take).ToList();
            return Task.FromResult(list);
        }

        public Task<ConnectorPackageInfo?> GetPackageAsync(string packageId, string? version = null, CancellationToken cancellationToken = default)
        {
            if (ThrowOnGetPackage) throw new InvalidOperationException("repo down");
            return Task.FromResult(Packages.TryGetValue(packageId, out var p) ? p : null);
        }

        public Task<IReadOnlyList<string>> GetVersionsAsync(string packageId, CancellationToken cancellationToken = default)
        {
            IReadOnlyList<string> v = Packages.TryGetValue(packageId, out var p) ? p.AvailableVersions.ToList() : [];
            return Task.FromResult(v);
        }

        public Task<string> DownloadAsync(string packageId, string version, string targetDirectory, CancellationToken cancellationToken = default)
            => throw new NotSupportedException("FakeRepository does not download");

        public void Dispose() => Disposed = true;
    }
}
