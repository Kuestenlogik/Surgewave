using System.IO.Compression;
using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

internal static class PluginSettingsTestJson
{
    public static readonly JsonSerializerOptions Options = new() { WriteIndented = true };
}

/// <summary>
/// End-to-end tests for the pluginsettings.json bundling pipeline:
/// <list type="bullet">
///   <item><description><see cref="PluginPackageManager.PackAsync"/> picks up
///   <c>pluginsettings.json</c> from the build output and writes it into the
///   <c>.swpkg</c> archive at the root.</description></item>
///   <item><description><see cref="PluginPackageManager.InstallAsync"/> extracts
///   that file to <c>plugins/&lt;id&gt;/pluginsettings.json</c> next to the DLLs.</description></item>
///   <item><description>The auto-detect path (no explicit <c>pluginSettings</c> in
///   the manifest) finds a sibling <c>pluginsettings.json</c>.</description></item>
///   <item><description>An explicit <c>pluginSettings</c> path that does not exist
///   makes <see cref="PluginPackageManager.PackAsync"/> fail loudly instead of
///   silently dropping the file.</description></item>
/// </list>
/// </summary>
public sealed class PluginSettingsBundlingTests : IDisposable
{
    private readonly string _scratchDir;
    private readonly PluginPackageManager _packageManager = new();

    public PluginSettingsBundlingTests()
    {
        _scratchDir = Path.Combine(Path.GetTempPath(), "surgewave-pluginsettings-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_scratchDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_scratchDir))
        {
            try { Directory.Delete(_scratchDir, recursive: true); }
            catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public async Task Pack_BundlesPluginSettings_WhenManifestReferencesIt()
    {
        var buildDir = CreatePluginBuildDir(
            manifest: new PluginManifest
            {
                Id = "test.plugin.explicit",
                Name = "Test Plugin (explicit)",
                Version = "1.0.0",
                Assemblies = ["DummyPlugin.dll"],
                PluginSettings = "pluginsettings.json",
            },
            pluginSettingsContent: """{ "Test": { "Explicit": true } }""");

        var outputDir = Path.Combine(_scratchDir, "out-explicit");
        var packagePath = await _packageManager.PackAsync(buildDir, manifestPath: null, outputDir);

        Assert.True(File.Exists(packagePath));
        AssertArchiveContains(packagePath, "pluginsettings.json", expectedContains: "\"Explicit\"");
    }

    [Fact]
    public async Task Pack_AutoDetectsPluginSettings_WhenManifestOmitsField()
    {
        var buildDir = CreatePluginBuildDir(
            manifest: new PluginManifest
            {
                Id = "test.plugin.autodetect",
                Name = "Test Plugin (auto)",
                Version = "1.0.0",
                Assemblies = ["DummyPlugin.dll"],
                // PluginSettings deliberately omitted — auto-detect should find it.
            },
            pluginSettingsContent: """{ "Test": { "AutoDetect": true } }""");

        var outputDir = Path.Combine(_scratchDir, "out-auto");
        var packagePath = await _packageManager.PackAsync(buildDir, manifestPath: null, outputDir);

        AssertArchiveContains(packagePath, "pluginsettings.json", expectedContains: "\"AutoDetect\"");
    }

    [Fact]
    public async Task Pack_OmitsPluginSettings_WhenFileIsAbsentAndManifestSilent()
    {
        // No pluginsettings.json in the build dir, no manifest reference. Pack should
        // succeed and the resulting .swpkg should not contain a pluginsettings.json entry.
        var buildDir = CreatePluginBuildDir(
            manifest: new PluginManifest
            {
                Id = "test.plugin.absent",
                Name = "Test Plugin (absent)",
                Version = "1.0.0",
                Assemblies = ["DummyPlugin.dll"],
            },
            pluginSettingsContent: null);

        var outputDir = Path.Combine(_scratchDir, "out-absent");
        var packagePath = await _packageManager.PackAsync(buildDir, manifestPath: null, outputDir);

        using var archive = ZipFile.OpenRead(packagePath);
        Assert.Null(archive.GetEntry("pluginsettings.json"));
    }

    [Fact]
    public async Task Pack_Throws_WhenManifestReferencesMissingPluginSettingsFile()
    {
        var buildDir = CreatePluginBuildDir(
            manifest: new PluginManifest
            {
                Id = "test.plugin.broken",
                Name = "Test Plugin (broken)",
                Version = "1.0.0",
                Assemblies = ["DummyPlugin.dll"],
                PluginSettings = "missing.json",
            },
            pluginSettingsContent: null);

        var outputDir = Path.Combine(_scratchDir, "out-broken");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => _packageManager.PackAsync(buildDir, manifestPath: null, outputDir));
        Assert.Contains("missing.json", ex.Message);
    }

    [Fact]
    public async Task Install_ExtractsPluginSettingsNextToDlls()
    {
        // Roundtrip: pack a package that bundles pluginsettings.json, install it into a
        // fresh plugins/ directory, and assert the file lands at plugins/<id>/pluginsettings.json.
        var buildDir = CreatePluginBuildDir(
            manifest: new PluginManifest
            {
                Id = "test.plugin.roundtrip",
                Name = "Test Plugin (roundtrip)",
                Version = "1.0.0",
                Assemblies = ["DummyPlugin.dll"],
                PluginSettings = "pluginsettings.json",
            },
            pluginSettingsContent: """{ "Test": { "Roundtrip": true } }""");

        var outputDir = Path.Combine(_scratchDir, "out-roundtrip");
        var packagePath = await _packageManager.PackAsync(buildDir, manifestPath: null, outputDir);

        var pluginsDir = Path.Combine(_scratchDir, "plugins");
        var result = await _packageManager.InstallAsync(packagePath, pluginsDir);

        Assert.True(result.Success, $"Install failed: {result.Error}");

        var installedSettings = Path.Combine(pluginsDir, "test.plugin.roundtrip", "pluginsettings.json");
        Assert.True(File.Exists(installedSettings), $"Expected {installedSettings} to be extracted");

        var content = await File.ReadAllTextAsync(installedSettings);
        Assert.Contains("\"Roundtrip\"", content);
    }

    [Fact]
    public async Task Pack_PreservesCustomFilename_WhenManifestDeclaresIt()
    {
        // Plugin author picks a non-default settings filename. Pack must:
        // 1) find the source file by its declared name in the build dir
        // 2) write it into the .swpkg under its ORIGINAL name (not normalised)
        var buildDir = CreateBuildDirWithCustomSettingsFile(
            manifest: new PluginManifest
            {
                Id = "test.plugin.custom-name",
                Name = "Test Plugin (custom)",
                Version = "1.0.0",
                Assemblies = ["DummyPlugin.dll"],
                PluginSettings = "mqtt-defaults.json",
            },
            settingsFileName: "mqtt-defaults.json",
            settingsContent: """{ "Test": { "CustomName": true } }""");

        var outputDir = Path.Combine(_scratchDir, "out-custom-pack");
        var packagePath = await _packageManager.PackAsync(buildDir, manifestPath: null, outputDir);

        using var archive = ZipFile.OpenRead(packagePath);
        // Original filename survives the trip into the .swpkg.
        Assert.NotNull(archive.GetEntry("mqtt-defaults.json"));
        // The default name is NOT created — pack does not normalise.
        Assert.Null(archive.GetEntry("pluginsettings.json"));
    }

    [Fact]
    public async Task Install_ExtractsCustomSettingsFilename_AndDiscoveryFindsIt()
    {
        // Full custom-name roundtrip: pack with a custom settings filename, install,
        // verify the file lands at plugins/<id>/<custom-name>, and verify
        // EnumerateInstalledPluginSettingsFiles picks it up via the manifest's
        // pluginSettings field (not by globbing for "pluginsettings.json").
        var buildDir = CreateBuildDirWithCustomSettingsFile(
            manifest: new PluginManifest
            {
                Id = "test.plugin.custom-discover",
                Name = "Test Plugin (custom-discover)",
                Version = "1.0.0",
                Assemblies = ["DummyPlugin.dll"],
                PluginSettings = "mqtt-defaults.json",
            },
            settingsFileName: "mqtt-defaults.json",
            settingsContent: """{ "Test": { "DiscoveredViaManifest": true } }""");

        var outputDir = Path.Combine(_scratchDir, "out-custom-install");
        var packagePath = await _packageManager.PackAsync(buildDir, manifestPath: null, outputDir);

        var pluginsDir = Path.Combine(_scratchDir, "plugins-custom");
        var result = await _packageManager.InstallAsync(packagePath, pluginsDir);
        Assert.True(result.Success, $"Install failed: {result.Error}");

        var installedSettings = Path.Combine(pluginsDir, "test.plugin.custom-discover", "mqtt-defaults.json");
        Assert.True(File.Exists(installedSettings), $"Expected {installedSettings} to be extracted");

        // The default name should NOT exist either — install respects the manifest, no fallback file.
        var defaultName = Path.Combine(pluginsDir, "test.plugin.custom-discover", "pluginsettings.json");
        Assert.False(File.Exists(defaultName));

        // Discovery side reads plugin.json and yields the custom filename.
        var discovered = PluginPackageManager.EnumerateInstalledPluginSettingsFiles(pluginsDir).ToList();
        Assert.Single(discovered);
        Assert.Equal(installedSettings, discovered[0], ignoreCase: true);
    }

    [Fact]
    public async Task Pack_RejectsManifestPluginSettingsWithPathSeparators()
    {
        // Defence-in-depth: plugin manifest field must be a plain filename, not a path.
        var buildDir = CreatePluginBuildDir(
            manifest: new PluginManifest
            {
                Id = "test.plugin.evil",
                Name = "Test Plugin (evil)",
                Version = "1.0.0",
                Assemblies = ["DummyPlugin.dll"],
                PluginSettings = "../../../etc/passwd",
            },
            pluginSettingsContent: """{ "irrelevant": true }""");

        var outputDir = Path.Combine(_scratchDir, "out-evil");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => _packageManager.PackAsync(buildDir, manifestPath: null, outputDir));
        Assert.Contains("plain filename", ex.Message);
    }

    private string CreateBuildDirWithCustomSettingsFile(PluginManifest manifest, string settingsFileName, string settingsContent)
    {
        var buildDir = Path.Combine(_scratchDir, manifest.Id, "bin");
        Directory.CreateDirectory(buildDir);

        var manifestJson = JsonSerializer.Serialize(manifest, PluginSettingsTestJson.Options);
        File.WriteAllText(Path.Combine(buildDir, "plugin.json"), manifestJson);

        File.WriteAllBytes(Path.Combine(buildDir, "DummyPlugin.dll"), [0x4D, 0x5A]);
        File.WriteAllText(Path.Combine(buildDir, settingsFileName), settingsContent);

        return buildDir;
    }

    private string CreatePluginBuildDir(PluginManifest manifest, string? pluginSettingsContent)
    {
        var buildDir = Path.Combine(_scratchDir, manifest.Id, "bin");
        Directory.CreateDirectory(buildDir);

        // Manifest
        var manifestJson = JsonSerializer.Serialize(manifest, PluginSettingsTestJson.Options);
        File.WriteAllText(Path.Combine(buildDir, "plugin.json"), manifestJson);

        // Stub assembly — content does not matter, the packager only checks for File.Exists.
        File.WriteAllBytes(Path.Combine(buildDir, "DummyPlugin.dll"), [0x4D, 0x5A]); // "MZ" header

        // Optional pluginsettings.json
        if (pluginSettingsContent is not null)
        {
            File.WriteAllText(Path.Combine(buildDir, "pluginsettings.json"), pluginSettingsContent);
        }

        return buildDir;
    }

    private static void AssertArchiveContains(string packagePath, string entryName, string expectedContains)
    {
        using var archive = ZipFile.OpenRead(packagePath);
        var entry = archive.GetEntry(entryName);
        Assert.NotNull(entry);
        using var reader = new StreamReader(entry!.Open());
        var content = reader.ReadToEnd();
        Assert.Contains(expectedContains, content);
    }
}
