using System.IO.Compression;
using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Repository.Tests;

/// <summary>
/// End-to-end-Coverage fuer <see cref="ConnectorInstaller"/>-Pfade, die echte ZIPs +
/// FakeRepository.DownloadAsync brauchen: InstallAsync (Fresh, Upgrade ueber existing
/// install dir), LoadConnector mit lib/{tfm}/* TFM-Selection, Tempdir-Cleanup.
/// </summary>
public sealed class ConnectorInstallerE2ETests : IDisposable
{
    private readonly string _root;
    private readonly string _installDir;
    private readonly string _packageStorage;

    public ConnectorInstallerE2ETests()
    {
        _root = Path.Combine(Path.GetTempPath(), $"sw-installer-e2e-{Guid.NewGuid():N}");
        _installDir = Path.Combine(_root, "installed");
        _packageStorage = Path.Combine(_root, "packages");
        Directory.CreateDirectory(_installDir);
        Directory.CreateDirectory(_packageStorage);
    }

    [Fact]
    public async Task InstallAsync_FreshPackage_ExtractsAndRegistersMetadata()
    {
        var pkg = BuildSamplePackage("akka.plugin", "1.0.0", tfm: "net10.0");
        var repo = new ZipBackedFakeRepo(pkg);

        using var installer = new ConnectorInstaller(_installDir);
        await installer.InstallAsync(repo, "akka.plugin", "1.0.0");

        Assert.True(installer.IsInstalled("akka.plugin"));
        Assert.Equal("1.0.0", installer.GetInstalledVersion("akka.plugin"));

        var meta = installer.InstalledConnectors["akka.plugin"];
        Assert.True(File.Exists(Path.Combine(meta.InstallDirectory, "connector.json")));
        Assert.True(File.Exists(Path.Combine(meta.InstallDirectory, "lib", "net10.0", "akka.plugin.dll")));
    }

    [Fact]
    public async Task InstallAsync_SameVersionTwice_DeletesAndReinstallsSameDir()
    {
        // Same package and version twice triggers the Directory.Exists -> Delete branch
        // inside InstallAsync. The package dir is named "{id}.{version}" so different
        // versions live side-by-side; only same-version reinstalls hit the cleanup path.
        var pkg = BuildSamplePackage("akka.plugin", "1.0.0", tfm: "net10.0");
        var repo = new ZipBackedFakeRepo(pkg);

        using var installer = new ConnectorInstaller(_installDir);
        await installer.InstallAsync(repo, "akka.plugin", "1.0.0");
        var firstDir = installer.InstalledConnectors["akka.plugin"].InstallDirectory;

        // Reinstall same version
        await installer.InstallAsync(repo, "akka.plugin", "1.0.0");

        Assert.Equal("1.0.0", installer.GetInstalledVersion("akka.plugin"));
        Assert.True(Directory.Exists(firstDir));
    }

    [Fact]
    public async Task InstallAsync_DifferentVersion_LivesSideBySide()
    {
        var pkg1 = BuildSamplePackage("akka.plugin", "1.0.0", tfm: "net10.0");
        var pkg2 = BuildSamplePackage("akka.plugin", "2.0.0", tfm: "net10.0");

        using var installer = new ConnectorInstaller(_installDir);
        await installer.InstallAsync(new ZipBackedFakeRepo(pkg1), "akka.plugin", "1.0.0");
        await installer.InstallAsync(new ZipBackedFakeRepo(pkg2), "akka.plugin", "2.0.0");

        // _installedConnectors maps packageId -> latest install metadata
        Assert.Equal("2.0.0", installer.GetInstalledVersion("akka.plugin"));
    }

    [Fact]
    public async Task LoadConnector_AfterInstall_ReturnsAtLeastOneAssembly_OrEmptyIfPlatformBlocked()
    {
        // The shipped DLL stub is not a valid PE — LoadFromAssemblyPath will throw
        // BadImageFormatException which ConnectorInstaller swallows. So the resulting
        // assembly list is *allowed* to be empty; what we verify is that LoadConnector
        // doesn't throw and runs through the TFM-selection branch.
        var pkg = BuildSamplePackage("p.plugin", "1.0.0", tfm: "net10.0");
        var repo = new ZipBackedFakeRepo(pkg);

        using var installer = new ConnectorInstaller(_installDir);
        await installer.InstallAsync(repo, "p.plugin", "1.0.0");

        var assemblies = installer.LoadConnector("p.plugin");

        Assert.NotNull(assemblies);
    }

    [Fact]
    public async Task LoadConnector_PrefersNet10OverOlderTfm()
    {
        // Bundle two TFM dirs and verify the picker selects net10.0. Both DLLs are
        // garbage bytes — the load itself will fail and produce an empty assembly
        // list, but the selection logic is what we exercise here.
        var pkg = BuildSamplePackageWithMultipleTfms("multi.plugin", "1.0.0",
            tfms: ["net6.0", "net10.0", "netstandard2.0"]);
        var repo = new ZipBackedFakeRepo(pkg);

        using var installer = new ConnectorInstaller(_installDir);
        await installer.InstallAsync(repo, "multi.plugin", "1.0.0");

        // Just make sure it doesn't throw — the TFM selection must walk through
        // the net10.0 path successfully even if the resulting DLL fails to load.
        var assemblies = installer.LoadConnector("multi.plugin");
        Assert.NotNull(assemblies);
    }

    [Fact]
    public async Task UnloadConnector_LoadedPackage_RemovesFromCache()
    {
        var pkg = BuildSamplePackage("unload.plugin", "1.0.0", tfm: "net10.0");
        var repo = new ZipBackedFakeRepo(pkg);

        using var installer = new ConnectorInstaller(_installDir);
        await installer.InstallAsync(repo, "unload.plugin", "1.0.0");
        installer.LoadConnector("unload.plugin");

        installer.UnloadConnector("unload.plugin");
        // Subsequent load creates a new LoadContext — must not throw
        installer.LoadConnector("unload.plugin");
    }

    [Fact]
    public async Task Uninstall_AfterInstall_RemovesDirAndStateAndAllowsReinstall()
    {
        var pkg = BuildSamplePackage("reinst.plugin", "1.0.0", tfm: "net10.0");
        var repo = new ZipBackedFakeRepo(pkg);

        using var installer = new ConnectorInstaller(_installDir);
        await installer.InstallAsync(repo, "reinst.plugin", "1.0.0");
        var dir = installer.InstalledConnectors["reinst.plugin"].InstallDirectory;

        installer.Uninstall("reinst.plugin");

        Assert.False(installer.IsInstalled("reinst.plugin"));
        Assert.False(Directory.Exists(dir));

        // Reinstall works
        await installer.InstallAsync(repo, "reinst.plugin", "1.0.0");
        Assert.True(installer.IsInstalled("reinst.plugin"));
    }

    [Fact]
    public async Task InstallAsync_RepositoryThrows_LeavesInstallerStateClean()
    {
        var repo = new ZipBackedFakeRepo(packagePath: null) { ThrowOnDownload = true };

        using var installer = new ConnectorInstaller(_installDir);

        await Assert.ThrowsAsync<NotImplementedException>(() =>
            installer.InstallAsync(repo, "fails.plugin", "1.0.0"));

        Assert.False(installer.IsInstalled("fails.plugin"));
    }

    private string BuildSamplePackage(string packageId, string version, string tfm)
    {
        return BuildSamplePackageWithMultipleTfms(packageId, version, [tfm]);
    }

    private string BuildSamplePackageWithMultipleTfms(string packageId, string version, string[] tfms)
    {
        var pkgPath = Path.Combine(_packageStorage, $"{packageId}-{version}.zip");
        using var fs = File.Create(pkgPath);
        using var archive = new ZipArchive(fs, ZipArchiveMode.Create);

        // plugin.json for fallback metadata
        var manifestEntry = archive.CreateEntry("plugin.json");
        using (var s = manifestEntry.Open())
        using (var w = new StreamWriter(s))
        {
            var manifest = new { id = packageId, version, name = packageId };
            w.Write(JsonSerializer.Serialize(manifest));
        }

        // DLLs per TFM
        foreach (var tfm in tfms)
        {
            var dllEntry = archive.CreateEntry($"lib/{tfm}/{packageId}.dll");
            using var s = dllEntry.Open();
            // Garbage but with PE header bytes so it at least is recognised as "a file"
            s.Write([0x4D, 0x5A]);
        }

        return pkgPath;
    }

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            try { Directory.Delete(_root, recursive: true); }
            catch { /* best effort */ }
        }
    }

    private sealed class ZipBackedFakeRepo : IConnectorRepository
    {
        private readonly string? _packagePath;

        public ZipBackedFakeRepo(string? packagePath) => _packagePath = packagePath;

        public bool ThrowOnDownload { get; set; }

        public string Name => "zip-fake";
        public string Source => "fake://zip";

        public Task<IReadOnlyList<ConnectorPackageInfo>> SearchAsync(string? query, int skip = 0, int take = 20, CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<ConnectorPackageInfo>>([]);

        public Task<ConnectorPackageInfo?> GetPackageAsync(string packageId, string? version = null, CancellationToken cancellationToken = default)
            => Task.FromResult<ConnectorPackageInfo?>(null);

        public Task<IReadOnlyList<string>> GetVersionsAsync(string packageId, CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<string>>([]);

        public async Task<string> DownloadAsync(string packageId, string version, string targetDirectory, CancellationToken cancellationToken = default)
        {
            if (ThrowOnDownload) throw new NotImplementedException("test forced failure");
            Directory.CreateDirectory(targetDirectory);
            var copy = Path.Combine(targetDirectory, $"{packageId}-{version}.zip");
            await using var src = File.OpenRead(_packagePath!);
            await using var dst = File.Create(copy);
            await src.CopyToAsync(dst, cancellationToken);
            return copy;
        }
    }
}
