using System.IO.Compression;
using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

/// <summary>
/// End-to-end Roundtrip-Tests fuer den zentralen Pack -> Validate -> Install -> Uninstall
/// Lifecycle. Erzeugt im temp-Dir einen Mini-Build-Output mit einem stub-Manifest und einer
/// stub-DLL (leere binary), packt es zu .swpkg, validiert, installiert und prueft den
/// Disk-State.
/// </summary>
public sealed class PluginPackageManagerTests : IDisposable
{
    private readonly string _root;
    private readonly string _buildDir;
    private readonly string _outputDir;
    private readonly string _pluginsDir;
    private readonly PluginPackageManager _manager = new();

    public PluginPackageManagerTests()
    {
        _root = Path.Combine(Path.GetTempPath(), $"sw-pkgmgr-tests-{Guid.NewGuid():N}");
        _buildDir = Path.Combine(_root, "build");
        _outputDir = Path.Combine(_root, "out");
        _pluginsDir = Path.Combine(_root, "plugins");
        Directory.CreateDirectory(_buildDir);
        Directory.CreateDirectory(_outputDir);
        Directory.CreateDirectory(_pluginsDir);
    }

    [Fact]
    public async Task PackThenValidate_ProducesValidPackage()
    {
        var manifestPath = WriteManifest("test.plugin", "1.0.0", ["test-plugin.dll"]);
        WriteStubDll("test-plugin.dll");

        var pkg = await _manager.PackAsync(_buildDir, manifestPath, _outputDir);

        Assert.True(File.Exists(pkg));
        Assert.EndsWith(".swpkg", pkg);
        Assert.Contains("test.plugin-1.0.0", pkg);

        var validation = await _manager.ValidateAsync(pkg);
        Assert.True(validation.IsValid);
        Assert.NotNull(validation.Manifest);
        Assert.Equal("test.plugin", validation.Manifest!.Id);
        Assert.Equal("1.0.0", validation.Manifest!.Version);
    }

    [Fact]
    public async Task Pack_AlsoWritesSha256Sidecar()
    {
        var manifestPath = WriteManifest("with.checksum", "0.1.0", ["with-checksum.dll"]);
        WriteStubDll("with-checksum.dll");

        var pkg = await _manager.PackAsync(_buildDir, manifestPath, _outputDir);

        var checksumFile = pkg + ".sha256";
        Assert.True(File.Exists(checksumFile));

        var content = await File.ReadAllTextAsync(checksumFile);
        var parts = content.Trim().Split("  ");
        Assert.Equal(2, parts.Length);
        Assert.Equal(64, parts[0].Length);  // SHA256 hex
        Assert.Equal(Path.GetFileName(pkg), parts[1]);

        var recomputed = await PackageChecksumCalculator.ComputeAsync(pkg);
        Assert.Equal(recomputed, parts[0]);
    }

    [Fact]
    public async Task PackInstall_Roundtrip_ExtractsManifestAndAssembly()
    {
        var manifestPath = WriteManifest("rt.plugin", "0.1.0", ["rt-plugin.dll"]);
        WriteStubDll("rt-plugin.dll");

        var pkg = await _manager.PackAsync(_buildDir, manifestPath, _outputDir);

        var result = await _manager.InstallAsync(pkg, _pluginsDir);

        Assert.True(result.Success, result.Error);
        Assert.NotNull(result.InstallPath);
        Assert.True(Directory.Exists(result.InstallPath!));
        Assert.True(File.Exists(Path.Combine(result.InstallPath!, "plugin.json")));
        // GetTargetPath strips the lib/ prefix on install, so the DLL ends up
        // directly under the plugin's install dir, not in a nested lib/ folder.
        Assert.True(File.Exists(Path.Combine(result.InstallPath!, "rt-plugin.dll")));
        Assert.False(result.WasUpgrade);
        Assert.Null(result.PreviousVersion);
    }

    [Fact]
    public async Task Install_AlreadyInstalled_FailsWithoutForce()
    {
        var manifestPath = WriteManifest("dup.plugin", "1.0.0", ["dup.dll"]);
        WriteStubDll("dup.dll");

        var pkg = await _manager.PackAsync(_buildDir, manifestPath, _outputDir);

        var first = await _manager.InstallAsync(pkg, _pluginsDir);
        Assert.True(first.Success);

        var second = await _manager.InstallAsync(pkg, _pluginsDir);
        Assert.False(second.Success);
        Assert.NotNull(second.Error);
        Assert.Contains("already installed", second.Error!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Install_AlreadyInstalled_SucceedsWithForce_AsUpgrade()
    {
        var manifestPath = WriteManifest("upg.plugin", "1.0.0", ["upg.dll"]);
        WriteStubDll("upg.dll");

        var pkg1 = await _manager.PackAsync(_buildDir, manifestPath, _outputDir);
        await _manager.InstallAsync(pkg1, _pluginsDir);

        // Rewrite manifest with new version
        var manifestPath2 = WriteManifest("upg.plugin", "1.1.0", ["upg.dll"]);
        var pkg2 = await _manager.PackAsync(_buildDir, manifestPath2, _outputDir);

        var result = await _manager.InstallAsync(pkg2, _pluginsDir, force: true);

        Assert.True(result.Success, result.Error);
        Assert.True(result.WasUpgrade);
        Assert.Equal("1.0.0", result.PreviousVersion);
        Assert.Equal("1.1.0", result.Manifest!.Version);
    }

    [Fact]
    public async Task Install_ExpectedSha256Mismatch_Rejects()
    {
        var manifestPath = WriteManifest("sha.plugin", "1.0.0", ["sha.dll"]);
        WriteStubDll("sha.dll");
        var pkg = await _manager.PackAsync(_buildDir, manifestPath, _outputDir);

        var wrongSha = new string('a', 64);
        var result = await _manager.InstallAsync(pkg, _pluginsDir, expectedSha256: wrongSha);

        Assert.False(result.Success);
        Assert.NotNull(result.Error);
        Assert.Contains("SHA256", result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Install_ExpectedSha256Matching_Accepts()
    {
        var manifestPath = WriteManifest("sha.plugin", "1.0.0", ["sha.dll"]);
        WriteStubDll("sha.dll");
        var pkg = await _manager.PackAsync(_buildDir, manifestPath, _outputDir);
        var sha = await PackageChecksumCalculator.ComputeAsync(pkg);

        var result = await _manager.InstallAsync(pkg, _pluginsDir, expectedSha256: sha);

        Assert.True(result.Success, result.Error);
    }

    [Fact]
    public async Task Validate_MissingFile_ReturnsInvalid()
    {
        var bogus = Path.Combine(_outputDir, "does-not-exist.swpkg");

        var result = await _manager.ValidateAsync(bogus);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("not found", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Validate_NonZipFile_Invalid()
    {
        var bogus = Path.Combine(_outputDir, "not-a-zip.swpkg");
        await File.WriteAllTextAsync(bogus, "this is not a zip archive");

        var result = await _manager.ValidateAsync(bogus);

        Assert.False(result.IsValid);
    }

    [Fact]
    public async Task Validate_PackageWithoutManifest_Invalid()
    {
        var bogus = Path.Combine(_outputDir, "no-manifest.swpkg");
        using (var archive = ZipFile.Open(bogus, ZipArchiveMode.Create))
        {
            archive.CreateEntry("lib/something.dll");
        }

        var result = await _manager.ValidateAsync(bogus);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("plugin.json", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Validate_ManifestMissingId_ReportsError()
    {
        var bogus = Path.Combine(_outputDir, "no-id.swpkg");
        using (var archive = ZipFile.Open(bogus, ZipArchiveMode.Create))
        {
            var entry = archive.CreateEntry("plugin.json");
            await using var stream = entry.Open();
            await using var writer = new StreamWriter(stream);
            await writer.WriteAsync("""{ "id": "", "name": "n", "version": "1.0.0", "assemblies": ["a.dll"] }""");
        }

        var result = await _manager.ValidateAsync(bogus);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("id", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Validate_NonSwpkgExtension_WarnsButNotFailing()
    {
        var manifestPath = WriteManifest("warn.plugin", "1.0.0", ["warn.dll"]);
        WriteStubDll("warn.dll");
        var pkg = await _manager.PackAsync(_buildDir, manifestPath, _outputDir);

        // Rename to .zip to trigger the warning path
        var renamed = pkg.Replace(".swpkg", ".zip", StringComparison.Ordinal);
        File.Move(pkg, renamed);

        var result = await _manager.ValidateAsync(renamed);

        Assert.True(result.IsValid);
        Assert.NotEmpty(result.Warnings);
    }

    [Fact]
    public async Task Uninstall_RemovesPluginDirectory()
    {
        var manifestPath = WriteManifest("uninst.plugin", "1.0.0", ["uninst.dll"]);
        WriteStubDll("uninst.dll");
        var pkg = await _manager.PackAsync(_buildDir, manifestPath, _outputDir);
        var install = await _manager.InstallAsync(pkg, _pluginsDir);
        Assert.True(install.Success);

        var removed = await _manager.UninstallAsync("uninst.plugin", _pluginsDir);

        Assert.True(removed);
        Assert.False(Directory.Exists(install.InstallPath!));
    }

    [Fact]
    public async Task Uninstall_NotInstalled_ReturnsFalse()
    {
        var removed = await _manager.UninstallAsync("never.installed", _pluginsDir);

        Assert.False(removed);
    }

    [Fact]
    public async Task GetInstalledPlugins_ListsInstalledOnly()
    {
        var m1 = WriteManifest("p1.plugin", "1.0.0", ["p1.dll"]);
        WriteStubDll("p1.dll");
        var pkg1 = await _manager.PackAsync(_buildDir, m1, _outputDir);
        await _manager.InstallAsync(pkg1, _pluginsDir);

        var m2 = WriteManifest("p2.plugin", "2.0.0", ["p2.dll"]);
        WriteStubDll("p2.dll");
        var pkg2 = await _manager.PackAsync(_buildDir, m2, _outputDir);
        await _manager.InstallAsync(pkg2, _pluginsDir);

        // A stray non-plugin directory should be silently ignored
        Directory.CreateDirectory(Path.Combine(_pluginsDir, "stray"));

        var installed = new List<InstalledPlugin>();
        await foreach (var p in _manager.GetInstalledPluginsAsync(_pluginsDir))
            installed.Add(p);

        Assert.Equal(2, installed.Count);
        Assert.Contains(installed, p => p.Id == "p1.plugin" && p.Version == "1.0.0");
        Assert.Contains(installed, p => p.Id == "p2.plugin" && p.Version == "2.0.0");
    }

    [Fact]
    public async Task GetInstalledPlugins_EmptyDir_YieldsNothing()
    {
        var installed = new List<InstalledPlugin>();
        await foreach (var p in _manager.GetInstalledPluginsAsync(_pluginsDir))
            installed.Add(p);

        Assert.Empty(installed);
    }

    [Fact]
    public async Task GetInstalledPlugins_NonExistentDir_YieldsNothing()
    {
        var installed = new List<InstalledPlugin>();
        await foreach (var p in _manager.GetInstalledPluginsAsync(Path.Combine(_root, "no-such-dir")))
            installed.Add(p);

        Assert.Empty(installed);
    }

    [Fact]
    public async Task Pack_NoManifest_Throws()
    {
        WriteStubDll("orphan.dll");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _manager.PackAsync(_buildDir, manifestPath: null, _outputDir));

        Assert.Contains("plugin.json", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Pack_AssemblyNotFound_Throws()
    {
        var manifestPath = WriteManifest("missing.plugin", "1.0.0", ["does-not-exist.dll"]);
        // No stub-DLL on disk

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _manager.PackAsync(_buildDir, manifestPath, _outputDir));

        Assert.Contains("assembl", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private string WriteManifest(string id, string version, string[] assemblies)
    {
        var manifest = new
        {
            id,
            name = id,
            version,
            assemblies,
        };
        var path = Path.Combine(_buildDir, "plugin.json");
        File.WriteAllText(path, JsonSerializer.Serialize(manifest));
        return path;
    }

    private void WriteStubDll(string fileName)
    {
        // PluginPackageManager.PackAsync only checks File.Exists for assemblies — empty
        // contents are fine because the test never loads them.
        File.WriteAllBytes(Path.Combine(_buildDir, fileName), [0x4D, 0x5A]);
    }

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            try { Directory.Delete(_root, recursive: true); }
            catch { /* best effort */ }
        }
    }
}
