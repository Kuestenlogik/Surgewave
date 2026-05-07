using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

public sealed class PluginPackageSignerRegistryTests
{
    [Fact]
    public void Builtin_ecdsa_is_always_registered()
    {
        using var registry = PluginPackageSignerRegistry.LoadFrom();

        Assert.Contains("builtin-ecdsa", registry.Providers.Keys);
    }

    [Fact]
    public void GetProvider_is_case_insensitive()
    {
        using var registry = PluginPackageSignerRegistry.LoadFrom();

        Assert.NotNull(registry.GetProvider("BUILTIN-ECDSA"));
        Assert.NotNull(registry.GetProvider("Builtin-Ecdsa"));
    }

    [Fact]
    public void GetProvider_unknown_name_throws()
    {
        using var registry = PluginPackageSignerRegistry.LoadFrom();

        var ex = Assert.Throws<KeyNotFoundException>(() => registry.GetProvider("nonexistent-provider"));
        Assert.Contains("builtin-ecdsa", ex.Message);
    }

    [Fact]
    public void Missing_plugin_dir_is_silently_skipped()
    {
        using var registry = PluginPackageSignerRegistry.LoadFrom(Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N")));

        // builtin still present
        Assert.Contains("builtin-ecdsa", registry.Providers.Keys);
    }

    [Fact]
    public void Builtin_provider_builds_signer_from_options()
    {
        using var registry = PluginPackageSignerRegistry.LoadFrom();
        var provider = registry.GetProvider("builtin-ecdsa");

        var tempDir = Path.Combine(Path.GetTempPath(), "pluginPackage-registry-test-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(tempDir);
        try
        {
            var (keyPath, _) = BuiltinEcdsaSigner.GenerateKeyPair(tempDir, "reg-test");
            var signer = provider.Create(new Dictionary<string, string>
            {
                ["private-key"] = keyPath
            });

            Assert.Equal("builtin-ecdsa", signer.Name);
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public void Builtin_provider_rejects_empty_options()
    {
        using var registry = PluginPackageSignerRegistry.LoadFrom();
        var provider = registry.GetProvider("builtin-ecdsa");

        // BuiltinEcdsaSigner requires at least one of private-key / trusted-keys-dir
        Assert.Throws<ArgumentException>(() => provider.Create(new Dictionary<string, string>()));
    }
}
