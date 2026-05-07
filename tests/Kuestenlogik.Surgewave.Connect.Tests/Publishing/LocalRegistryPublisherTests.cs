namespace Kuestenlogik.Surgewave.Connect.Tests.Publishing;

using System.IO.Compression;
using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Repository;

public class LocalRegistryPublisherTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _registryDir;

    public LocalRegistryPublisherTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid()}");
        _registryDir = Path.Combine(_tempDir, "registry");
        Directory.CreateDirectory(_tempDir);
        Directory.CreateDirectory(_registryDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, true); }
            catch { /* Ignore cleanup errors */ }
        }
    }

    private string CreateTestPackage(string id = "Kuestenlogik.Surgewave.Connector.Test", string version = "1.0.0")
    {
        var packagePath = Path.Combine(_tempDir, $"{id}-{version}.swpkg");

        using var archive = ZipFile.Open(packagePath, ZipArchiveMode.Create);

        // Add manifest
        var manifestEntry = archive.CreateEntry("plugin.json");
        using (var stream = manifestEntry.Open())
        {
            var manifest = new
            {
                id,
                name = "Test Connector",
                version,
                assemblies = new[] { "lib/Test.dll" }
            };
            JsonSerializer.Serialize(stream, manifest);
        }

        // Add a dummy DLL in lib/
        var libEntry = archive.CreateEntry("lib/Test.dll");
        using (var stream = libEntry.Open())
        {
            stream.Write(new byte[] { 0x4D, 0x5A }); // PE header
        }

        return packagePath;
    }

    [Fact]
    public async Task PublishToEmptyRegistry_Succeeds()
    {
        var publisher = new LocalRegistryPublisher(_registryDir);
        var packagePath = CreateTestPackage();

        var result = await publisher.PublishAsync(packagePath);

        Assert.True(result.Success);
        Assert.Equal("Kuestenlogik.Surgewave.Connector.Test", result.PackageId);
        Assert.Equal("1.0.0", result.Version);

        // Verify packages.json was created
        var packagesJson = Path.Combine(_registryDir, "packages.json");
        Assert.True(File.Exists(packagesJson));

        var json = await File.ReadAllTextAsync(packagesJson);
        Assert.Contains("Kuestenlogik.Surgewave.Connector.Test", json);
    }

    [Fact]
    public async Task PublishToExistingRegistry_AppendsEntry()
    {
        var publisher = new LocalRegistryPublisher(_registryDir);

        // Publish first package
        var pkg1 = CreateTestPackage("Kuestenlogik.Surgewave.Connector.First", "1.0.0");
        await publisher.PublishAsync(pkg1);

        // Publish second package
        var pkg2 = CreateTestPackage("Kuestenlogik.Surgewave.Connector.Second", "1.0.0");
        var result = await publisher.PublishAsync(pkg2);

        Assert.True(result.Success);

        var packagesJson = Path.Combine(_registryDir, "packages.json");
        var json = await File.ReadAllTextAsync(packagesJson);
        Assert.Contains("Kuestenlogik.Surgewave.Connector.First", json);
        Assert.Contains("Kuestenlogik.Surgewave.Connector.Second", json);
    }

    [Fact]
    public async Task PublishDuplicate_WithoutForce_Fails()
    {
        var publisher = new LocalRegistryPublisher(_registryDir);
        var packagePath = CreateTestPackage();

        await publisher.PublishAsync(packagePath);
        var result = await publisher.PublishAsync(packagePath);

        Assert.False(result.Success);
        Assert.Contains("already exists", result.Error);
    }

    [Fact]
    public async Task PublishDuplicate_WithForce_Succeeds()
    {
        var publisher = new LocalRegistryPublisher(_registryDir);
        var packagePath = CreateTestPackage();

        await publisher.PublishAsync(packagePath);
        var result = await publisher.PublishAsync(packagePath, force: true);

        Assert.True(result.Success);
        Assert.Single(result.Warnings);
    }

    [Fact]
    public async Task PublishCreatesCorrectDirectoryStructure()
    {
        var publisher = new LocalRegistryPublisher(_registryDir);
        var packagePath = CreateTestPackage("Kuestenlogik.Surgewave.Connector.Foo", "2.0.0");

        await publisher.PublishAsync(packagePath);

        var expectedFile = Path.Combine(_registryDir, "packages", "Kuestenlogik.Surgewave.Connector.Foo", "2.0.0", "Kuestenlogik.Surgewave.Connector.Foo.2.0.0.swpkg");
        Assert.True(File.Exists(expectedFile));

        // SHA256 sidecar should exist
        Assert.True(File.Exists(expectedFile + ".sha256"));
    }

    [Fact]
    public async Task PublishInvalidPackage_Fails()
    {
        var publisher = new LocalRegistryPublisher(_registryDir);

        // Create an invalid package (not a valid ZIP)
        var invalidPath = Path.Combine(_tempDir, "invalid.swpkg");
        await File.WriteAllTextAsync(invalidPath, "not a zip file");

        var result = await publisher.PublishAsync(invalidPath);

        Assert.False(result.Success);
        Assert.Contains("Invalid", result.Error, StringComparison.OrdinalIgnoreCase);
    }
}
