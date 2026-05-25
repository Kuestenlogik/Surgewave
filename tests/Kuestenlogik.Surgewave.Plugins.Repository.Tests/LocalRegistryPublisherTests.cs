using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Repository.Tests;

/// <summary>
/// End-to-end fuer <see cref="LocalRegistryPublisher"/>. Erzeugt echte .swpkg-Files via
/// <see cref="PluginPackageManager"/> und publiziert sie in ein temp-Registry-Root.
/// Coverage zielt auf Publish-Erfolg, Force-Overwrite, force-required-Fehler, packages.json
/// merge fuer existing-und-new-Entries.
/// </summary>
public sealed class LocalRegistryPublisherTests : IDisposable
{
    private readonly string _root;
    private readonly string _buildDir;
    private readonly string _packageOutDir;
    private readonly string _registryRoot;
    private readonly PluginPackageManager _manager = new();

    public LocalRegistryPublisherTests()
    {
        _root = Path.Combine(Path.GetTempPath(), $"sw-localreg-tests-{Guid.NewGuid():N}");
        _buildDir = Path.Combine(_root, "build");
        _packageOutDir = Path.Combine(_root, "packed");
        _registryRoot = Path.Combine(_root, "registry");
        Directory.CreateDirectory(_buildDir);
        Directory.CreateDirectory(_packageOutDir);
        Directory.CreateDirectory(_registryRoot);
    }

    [Fact]
    public void CanPublish_IsAlwaysTrue()
    {
        var publisher = new LocalRegistryPublisher(_registryRoot);
        Assert.True(publisher.CanPublish);
    }

    [Fact]
    public async Task PublishAsync_InvalidPackage_Failed()
    {
        var publisher = new LocalRegistryPublisher(_registryRoot);
        var bogus = Path.Combine(_root, "not-a-package.swpkg");
        await File.WriteAllTextAsync(bogus, "garbage");

        var result = await publisher.PublishAsync(bogus);

        Assert.False(result.Success);
        Assert.NotNull(result.Error);
        Assert.Contains("Invalid package", result.Error!);
    }

    [Fact]
    public async Task PublishAsync_NewPackage_CopiesFileAndWritesSidecarAndManifest()
    {
        var pkg = await Pack("first.plugin", "1.0.0");
        var publisher = new LocalRegistryPublisher(_registryRoot);

        var result = await publisher.PublishAsync(pkg);

        Assert.True(result.Success, result.Error);
        Assert.Equal("first.plugin", result.PackageId);
        Assert.Equal("1.0.0", result.Version);

        var targetFile = Path.Combine(_registryRoot, "packages", "first.plugin", "1.0.0", "first.plugin.1.0.0.swpkg");
        Assert.True(File.Exists(targetFile));
        Assert.True(File.Exists(targetFile + ".sha256"));

        var packagesJsonPath = Path.Combine(_registryRoot, "packages.json");
        Assert.True(File.Exists(packagesJsonPath));
        var entries = await ReadPackagesJsonAsync(packagesJsonPath);
        Assert.Single(entries);
    }

    [Fact]
    public async Task PublishAsync_SamePackageTwiceWithoutForce_Fails()
    {
        var pkg = await Pack("dup.plugin", "1.0.0");
        var publisher = new LocalRegistryPublisher(_registryRoot);

        var first = await publisher.PublishAsync(pkg);
        Assert.True(first.Success, first.Error);

        var second = await publisher.PublishAsync(pkg);

        Assert.False(second.Success);
        Assert.Contains("--force", second.Error!);
    }

    [Fact]
    public async Task PublishAsync_SamePackageTwiceWithForce_OverwritesAndWarns()
    {
        var pkg = await Pack("force.plugin", "1.0.0");
        var publisher = new LocalRegistryPublisher(_registryRoot);
        await publisher.PublishAsync(pkg);

        var second = await publisher.PublishAsync(pkg, force: true);

        Assert.True(second.Success);
        Assert.Single(second.Warnings, w => w.Contains("Overwriting", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task PublishAsync_TwoVersionsOfSamePackage_UpdatesAvailableVersions()
    {
        var v1 = await Pack("multi.plugin", "1.0.0");
        var v2 = await Pack("multi.plugin", "1.1.0");
        var publisher = new LocalRegistryPublisher(_registryRoot);

        await publisher.PublishAsync(v1);
        var second = await publisher.PublishAsync(v2);

        Assert.True(second.Success);
        var entries = await ReadPackagesJsonAsync(Path.Combine(_registryRoot, "packages.json"));
        Assert.Single(entries);

        var entry = entries[0];
        Assert.Equal("1.1.0", entry.GetProperty("version").GetString());
        var versions = entry.GetProperty("availableVersions").EnumerateArray()
            .Select(v => v.GetString()).ToList();
        Assert.Equal("1.1.0", versions[0]);
        Assert.Contains("1.0.0", versions);
    }

    [Fact]
    public async Task PublishAsync_DifferentPackages_AppendToPackagesJson()
    {
        var a = await Pack("a.plugin", "1.0.0");
        var b = await Pack("b.plugin", "1.0.0");
        var publisher = new LocalRegistryPublisher(_registryRoot);

        await publisher.PublishAsync(a);
        await publisher.PublishAsync(b);

        var entries = await ReadPackagesJsonAsync(Path.Combine(_registryRoot, "packages.json"));
        Assert.Equal(2, entries.Count);
        Assert.Contains(entries, e => e.GetProperty("packageId").GetString() == "a.plugin");
        Assert.Contains(entries, e => e.GetProperty("packageId").GetString() == "b.plugin");
    }

    private async Task<string> Pack(string id, string version)
    {
        // Clean build dir between packs so consecutive WriteManifest calls don't collide.
        if (Directory.Exists(_buildDir)) Directory.Delete(_buildDir, true);
        Directory.CreateDirectory(_buildDir);

        var dllName = id.Replace(".", "-") + ".dll";
        var manifest = new
        {
            id,
            name = id,
            version,
            assemblies = new[] { dllName },
        };
        var manifestPath = Path.Combine(_buildDir, "plugin.json");
        await File.WriteAllTextAsync(manifestPath, JsonSerializer.Serialize(manifest));
        await File.WriteAllBytesAsync(Path.Combine(_buildDir, dllName), [0x4D, 0x5A]);

        return await _manager.PackAsync(_buildDir, manifestPath, _packageOutDir);
    }

    private static async Task<List<JsonElement>> ReadPackagesJsonAsync(string path)
    {
        var json = await File.ReadAllTextAsync(path);
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.EnumerateArray().Select(e => e.Clone()).ToList();
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
