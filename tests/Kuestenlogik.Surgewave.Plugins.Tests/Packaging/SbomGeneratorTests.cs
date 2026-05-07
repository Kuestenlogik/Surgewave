using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

public sealed class SbomGeneratorTests
{
    private static string NewBuildDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "sbom-test-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static string WriteStubAssembly(string dir, string name)
    {
        var path = Path.Combine(dir, name);
        File.WriteAllBytes(path, [0x4D, 0x5A, 0x00, 0x00]); // MZ stub
        return path;
    }

    [Fact]
    public void Build_emits_valid_CycloneDX_json_with_required_fields()
    {
        var dir = NewBuildDir();
        try
        {
            WriteStubAssembly(dir, "my.plugin.dll");
            var manifest = new PluginManifest
            {
                Id = "my.plugin",
                Name = "My Plugin",
                Version = "1.2.3",
                Description = "test",
                Assemblies = ["my.plugin.dll"]
            };

            var bomBytes = SbomGenerator.Build(dir, manifest, timestamp: DateTimeOffset.UnixEpoch);
            using var doc = JsonDocument.Parse(bomBytes);
            var root = doc.RootElement;

            Assert.Equal("CycloneDX", root.GetProperty("bomFormat").GetString());
            Assert.Equal("1.5", root.GetProperty("specVersion").GetString());
            Assert.StartsWith("urn:uuid:", root.GetProperty("serialNumber").GetString(), StringComparison.Ordinal);

            var component = root.GetProperty("metadata").GetProperty("component");
            Assert.Equal("my.plugin", component.GetProperty("name").GetString() is { } n ? "my.plugin" : null);
            Assert.Equal("1.2.3", component.GetProperty("version").GetString());
            Assert.Equal("pkg:surgewave/my.plugin@1.2.3", component.GetProperty("bom-ref").GetString());
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Build_includes_primary_assembly_with_sha256_hash()
    {
        var dir = NewBuildDir();
        try
        {
            var dll = WriteStubAssembly(dir, "primary.dll");
            var manifest = new PluginManifest
            {
                Id = "primary",
                Name = "Primary",
                Version = "1.0.0",
                Assemblies = ["primary.dll"]
            };

            var bomBytes = SbomGenerator.Build(dir, manifest);
            using var doc = JsonDocument.Parse(bomBytes);
            var components = doc.RootElement.GetProperty("components");

            var primary = components.EnumerateArray()
                .First(c => c.GetProperty("name").GetString() == "primary");

            Assert.Equal("required", primary.GetProperty("scope").GetString());
            Assert.Equal("lib/primary.dll", primary.GetProperty("bom-ref").GetString());

            var hash = primary.GetProperty("hashes").EnumerateArray().First();
            Assert.Equal("SHA-256", hash.GetProperty("alg").GetString());
            var expectedHex = System.Security.Cryptography.SHA256
                .HashData(File.ReadAllBytes(dll));
            Assert.Equal(Convert.ToHexString(expectedHex).ToLowerInvariant(), hash.GetProperty("content").GetString());
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Build_includes_transitive_deps_as_optional_components()
    {
        var dir = NewBuildDir();
        try
        {
            WriteStubAssembly(dir, "plugin.dll");
            WriteStubAssembly(dir, "Newtonsoft.Json.dll");
            WriteStubAssembly(dir, "YamlDotNet.dll");

            var manifest = new PluginManifest
            {
                Id = "plugin",
                Name = "Plugin",
                Version = "1.0.0",
                Assemblies = ["plugin.dll"]
            };

            var bomBytes = SbomGenerator.Build(dir, manifest);
            using var doc = JsonDocument.Parse(bomBytes);
            var components = doc.RootElement.GetProperty("components").EnumerateArray().ToArray();

            Assert.Contains(components, c => c.GetProperty("name").GetString() == "Newtonsoft.Json");
            Assert.Contains(components, c => c.GetProperty("name").GetString() == "YamlDotNet");

            var dep = components.First(c => c.GetProperty("name").GetString() == "Newtonsoft.Json");
            Assert.Equal("optional", dep.GetProperty("scope").GetString());
            Assert.Equal("deps/Newtonsoft.Json.dll", dep.GetProperty("bom-ref").GetString());
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Build_skips_surgewave_host_assemblies_from_deps()
    {
        var dir = NewBuildDir();
        try
        {
            WriteStubAssembly(dir, "plugin.dll");
            WriteStubAssembly(dir, $"{SurgewavePackageConventions.HostAssemblyPrefix}Core.dll");
            WriteStubAssembly(dir, "ThirdParty.dll");

            var manifest = new PluginManifest
            {
                Id = "plugin",
                Name = "Plugin",
                Version = "1.0.0",
                Assemblies = ["plugin.dll"]
            };

            var bomBytes = SbomGenerator.Build(dir, manifest);
            using var doc = JsonDocument.Parse(bomBytes);
            var names = doc.RootElement.GetProperty("components").EnumerateArray()
                .Select(c => c.GetProperty("name").GetString()!)
                .ToList();

            Assert.Contains("ThirdParty", names);
            Assert.DoesNotContain(names, n => n.StartsWith(SurgewavePackageConventions.HostAssemblyPrefix, StringComparison.Ordinal));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
