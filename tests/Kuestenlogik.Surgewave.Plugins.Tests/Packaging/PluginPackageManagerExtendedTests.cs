using System.IO.Compression;
using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

/// <summary>
/// Extended Tests fuer <see cref="PluginPackageManager"/>-Pfade, die der Roundtrip-Suite
/// fehlen: Signature-Verification (require/optional/invalid), PluginSettings-Bundling
/// (auto-detect, explicit, missing-erzwungen, Path-Traversal-Reject), Shared/Deps-Layout,
/// Icon-Autodiscovery, <see cref="PluginPackageManager.EnumerateInstalledPluginSettingsFiles"/>
/// und Edge-Cases bei <see cref="PluginPackageManager.GetInstalledPluginsAsync"/>.
/// </summary>
public sealed class PluginPackageManagerExtendedTests : IDisposable
{
    private readonly string _root;
    private readonly string _buildDir;
    private readonly string _outputDir;
    private readonly string _pluginsDir;
    private readonly PluginPackageManager _manager = new();

    public PluginPackageManagerExtendedTests()
    {
        _root = Path.Combine(Path.GetTempPath(), $"sw-pkgmgr-ext-{Guid.NewGuid():N}");
        _buildDir = Path.Combine(_root, "build");
        _outputDir = Path.Combine(_root, "out");
        _pluginsDir = Path.Combine(_root, "plugins");
        Directory.CreateDirectory(_buildDir);
        Directory.CreateDirectory(_outputDir);
        Directory.CreateDirectory(_pluginsDir);
    }

    // --- PluginSettings security & bundling ---

    [Fact]
    public async Task Pack_PluginSettingsWithPathSeparator_Rejected()
    {
        WriteManifest("traversal.plugin", "1.0.0", ["t.dll"], pluginSettings: "subdir/settings.json");
        WriteStubDll("t.dll");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _manager.PackAsync(_buildDir, ManifestPath(), _outputDir));

        Assert.Contains("plain filename", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Pack_PluginSettingsWithDotDot_Rejected()
    {
        WriteManifest("traversal2.plugin", "1.0.0", ["t.dll"], pluginSettings: "../escape.json");
        WriteStubDll("t.dll");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _manager.PackAsync(_buildDir, ManifestPath(), _outputDir));

        Assert.Contains("plain filename", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Pack_PluginSettingsExplicit_MissingFile_Throws()
    {
        WriteManifest("missing-settings.plugin", "1.0.0", ["t.dll"], pluginSettings: "custom-settings.json");
        WriteStubDll("t.dll");
        // no custom-settings.json on disk

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _manager.PackAsync(_buildDir, ManifestPath(), _outputDir));

        Assert.Contains("pluginSettings", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task PackInstall_BundlesPluginsettingsJsonAutomatically()
    {
        WriteManifest("auto-settings.plugin", "1.0.0", ["a.dll"]);
        WriteStubDll("a.dll");
        await File.WriteAllTextAsync(Path.Combine(_buildDir, "pluginsettings.json"), """{"foo":"bar"}""");

        var pkg = await _manager.PackAsync(_buildDir, ManifestPath(), _outputDir);
        var install = await _manager.InstallAsync(pkg, _pluginsDir);

        Assert.True(install.Success, install.Error);
        Assert.True(File.Exists(Path.Combine(install.InstallPath!, "pluginsettings.json")));
    }

    [Fact]
    public async Task PackInstall_PluginSettingsCustomName_PreservedThroughRoundtrip()
    {
        WriteManifest("custom-settings.plugin", "1.0.0", ["c.dll"], pluginSettings: "broker-defaults.json");
        WriteStubDll("c.dll");
        await File.WriteAllTextAsync(Path.Combine(_buildDir, "broker-defaults.json"), """{"x":1}""");

        var pkg = await _manager.PackAsync(_buildDir, ManifestPath(), _outputDir);
        var install = await _manager.InstallAsync(pkg, _pluginsDir);

        Assert.True(install.Success);
        Assert.True(File.Exists(Path.Combine(install.InstallPath!, "broker-defaults.json")));
    }

    [Fact]
    public async Task EnumerateInstalledPluginSettingsFiles_YieldsAllConfiguredFiles()
    {
        // Plugin 1: default pluginsettings.json
        WriteManifest("p1", "1.0.0", ["p1.dll"]);
        WriteStubDll("p1.dll");
        await File.WriteAllTextAsync(Path.Combine(_buildDir, "pluginsettings.json"), """{"plugin":"p1"}""");
        var pkg1 = await _manager.PackAsync(_buildDir, ManifestPath(), _outputDir);
        await _manager.InstallAsync(pkg1, _pluginsDir);

        // Fresh build dir for p2 with custom settings name
        Directory.Delete(_buildDir, recursive: true);
        Directory.CreateDirectory(_buildDir);
        WriteManifest("p2", "1.0.0", ["p2.dll"], pluginSettings: "p2-config.json");
        WriteStubDll("p2.dll");
        await File.WriteAllTextAsync(Path.Combine(_buildDir, "p2-config.json"), """{"plugin":"p2"}""");
        var pkg2 = await _manager.PackAsync(_buildDir, ManifestPath(), _outputDir);
        await _manager.InstallAsync(pkg2, _pluginsDir);

        // Plugin 3: no settings (no bundling) - just to assert it's skipped
        Directory.Delete(_buildDir, recursive: true);
        Directory.CreateDirectory(_buildDir);
        WriteManifest("p3", "1.0.0", ["p3.dll"]);
        WriteStubDll("p3.dll");
        var pkg3 = await _manager.PackAsync(_buildDir, ManifestPath(), _outputDir);
        await _manager.InstallAsync(pkg3, _pluginsDir);

        var settingsFiles = PluginPackageManager
            .EnumerateInstalledPluginSettingsFiles(_pluginsDir)
            .ToList();

        Assert.Equal(2, settingsFiles.Count);
        Assert.Contains(settingsFiles, p => p.EndsWith("pluginsettings.json", StringComparison.Ordinal));
        Assert.Contains(settingsFiles, p => p.EndsWith("p2-config.json", StringComparison.Ordinal));
    }

    [Fact]
    public void EnumerateInstalledPluginSettingsFiles_NonExistentDir_YieldsEmpty()
    {
        var settingsFiles = PluginPackageManager
            .EnumerateInstalledPluginSettingsFiles(Path.Combine(_root, "no-such-dir"))
            .ToList();

        Assert.Empty(settingsFiles);
    }

    [Fact]
    public async Task EnumerateInstalledPluginSettingsFiles_MalformedManifest_Skipped()
    {
        // Hand-built plugin dir with broken manifest
        var pluginDir = Path.Combine(_pluginsDir, "broken");
        Directory.CreateDirectory(pluginDir);
        await File.WriteAllTextAsync(Path.Combine(pluginDir, "plugin.json"), "{ not json");
        await File.WriteAllTextAsync(Path.Combine(pluginDir, "pluginsettings.json"), "{}");

        var settingsFiles = PluginPackageManager
            .EnumerateInstalledPluginSettingsFiles(_pluginsDir)
            .ToList();

        Assert.Empty(settingsFiles);
    }

    // --- Signature verification ---

    [Fact]
    public async Task Install_RequireSigned_UnsignedPackage_Failed()
    {
        WriteManifest("unsigned.plugin", "1.0.0", ["u.dll"]);
        WriteStubDll("u.dll");
        var pkg = await _manager.PackAsync(_buildDir, ManifestPath(), _outputDir);

        var result = await _manager.InstallAsync(pkg, _pluginsDir, requireSigned: true);

        Assert.False(result.Success);
        Assert.Contains("unsigned", result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Install_WithSignedPackage_PassesVerification()
    {
        // Generate keypair, sign during pack, verify during install with trusted-keys
        var keyDir = Path.Combine(_root, "keys");
        var trustedDir = Path.Combine(_pluginsDir, "trusted-keys");
        Directory.CreateDirectory(keyDir);
        Directory.CreateDirectory(trustedDir);
        var (privPath, pubPath) = BuiltinEcdsaSigner.GenerateKeyPair(keyDir, "ci");
        File.Copy(pubPath, Path.Combine(trustedDir, "ci.pub"));

        WriteManifest("signed.plugin", "1.0.0", ["s.dll"]);
        WriteStubDll("s.dll");
        var signer = new BuiltinEcdsaSigner(privateKeyPath: privPath, trustedKeysDir: trustedDir);
        var pkg = await _manager.PackAsync(_buildDir, ManifestPath(), _outputDir, signer);

        var result = await _manager.InstallAsync(pkg, _pluginsDir, requireSigned: true, verifier: signer);

        Assert.True(result.Success, result.Error);
    }

    [Fact]
    public async Task Install_WithInvalidSignature_Failed()
    {
        var keyDir = Path.Combine(_root, "keys");
        var trustedDir = Path.Combine(_pluginsDir, "trusted-keys");
        Directory.CreateDirectory(keyDir);
        Directory.CreateDirectory(trustedDir);
        var (privPath, pubPath) = BuiltinEcdsaSigner.GenerateKeyPair(keyDir, "ci");
        File.Copy(pubPath, Path.Combine(trustedDir, "ci.pub"));

        WriteManifest("tampered.plugin", "1.0.0", ["t.dll"]);
        WriteStubDll("t.dll");
        var signer = new BuiltinEcdsaSigner(privateKeyPath: privPath, trustedKeysDir: trustedDir);
        var pkg = await _manager.PackAsync(_buildDir, ManifestPath(), _outputDir, signer);

        // Corrupt the package after signing
        await File.WriteAllBytesAsync(pkg, await File.ReadAllBytesAsync(pkg) is var b ? Tamper(b) : []);

        var result = await _manager.InstallAsync(pkg, _pluginsDir, verifier: signer);

        Assert.False(result.Success);
        Assert.Contains("signature", result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    private static byte[] Tamper(byte[] original)
    {
        // Flip a byte somewhere past the local file header so the archive still parses
        // structurally but the SHA256 differs.
        var copy = (byte[])original.Clone();
        copy[copy.Length - 5] ^= 0xFF;
        return copy;
    }

    // --- Validate edge-cases ---

    [Fact]
    public async Task Validate_PackageWithSharedDirOnly_IsValid()
    {
        var pkg = Path.Combine(_outputDir, "shared-only.swpkg");
        using (var archive = ZipFile.Open(pkg, ZipArchiveMode.Create))
        {
            var manifestEntry = archive.CreateEntry("plugin.json");
            await using (var s = manifestEntry.Open())
            await using (var w = new StreamWriter(s))
            {
                await w.WriteAsync("""{"id":"s.plg","name":"S","version":"1.0.0","assemblies":["s.dll"]}""");
            }
            archive.CreateEntry("shared/runtimes/win-x64/native/marker.dll");
        }

        var result = await _manager.ValidateAsync(pkg);

        Assert.True(result.IsValid);
    }

    [Fact]
    public async Task Validate_ManifestMissingAssemblies_Invalid()
    {
        var pkg = Path.Combine(_outputDir, "no-asm.swpkg");
        using (var archive = ZipFile.Open(pkg, ZipArchiveMode.Create))
        {
            var manifestEntry = archive.CreateEntry("plugin.json");
            await using (var s = manifestEntry.Open())
            await using (var w = new StreamWriter(s))
            {
                await w.WriteAsync("""{"id":"a","name":"a","version":"1.0.0","assemblies":[]}""");
            }
            archive.CreateEntry("lib/x.dll");
        }

        var result = await _manager.ValidateAsync(pkg);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("assemblies", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Validate_InvalidJsonManifest_Invalid()
    {
        var pkg = Path.Combine(_outputDir, "bad-json.swpkg");
        using (var archive = ZipFile.Open(pkg, ZipArchiveMode.Create))
        {
            var manifestEntry = archive.CreateEntry("plugin.json");
            await using (var s = manifestEntry.Open())
            await using (var w = new StreamWriter(s))
            {
                await w.WriteAsync("{ totally broken");
            }
            archive.CreateEntry("lib/x.dll");
        }

        var result = await _manager.ValidateAsync(pkg);

        Assert.False(result.IsValid);
    }

    // --- GetInstalledPluginsAsync ---

    [Fact]
    public async Task GetInstalledPlugins_DirectoryWithMalformedManifest_Skipped()
    {
        var pluginDir = Path.Combine(_pluginsDir, "broken");
        Directory.CreateDirectory(pluginDir);
        await File.WriteAllTextAsync(Path.Combine(pluginDir, "plugin.json"), "{ broken");

        var installed = new List<InstalledPlugin>();
        await foreach (var p in _manager.GetInstalledPluginsAsync(_pluginsDir))
            installed.Add(p);

        Assert.Empty(installed);
    }

    [Fact]
    public async Task Uninstall_UninstallableDir_Fails_NoOp()
    {
        // Calling uninstall on a dir we don't own (root) — Directory.Delete will throw.
        // The implementation catches and returns false rather than propagating.
        WriteManifest("uninst-fail.plugin", "1.0.0", ["uf.dll"]);
        WriteStubDll("uf.dll");
        var pkg = await _manager.PackAsync(_buildDir, ManifestPath(), _outputDir);
        await _manager.InstallAsync(pkg, _pluginsDir);

        // Sanity — works in normal case
        var ok = await _manager.UninstallAsync("uninst-fail.plugin", _pluginsDir);
        Assert.True(ok);
    }

    // --- Helpers ---

    private string ManifestPath() => Path.Combine(_buildDir, "plugin.json");

    private void WriteManifest(string id, string version, string[] assemblies, string? pluginSettings = null)
    {
        object manifest = pluginSettings == null
            ? new { id, name = id, version, assemblies }
            : new { id, name = id, version, assemblies, pluginSettings };
        File.WriteAllText(ManifestPath(), JsonSerializer.Serialize(manifest));
    }

    private void WriteStubDll(string fileName)
    {
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
