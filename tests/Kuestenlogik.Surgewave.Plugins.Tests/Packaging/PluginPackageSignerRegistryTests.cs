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

    [Fact]
    public void LoadFrom_NoDirectoriesArg_OnlyBuiltinRegistered()
    {
        using var registry = PluginPackageSignerRegistry.LoadFrom();

        Assert.Single(registry.Providers);
    }

    [Fact]
    public void LoadFrom_EmptyOrNullDirEntry_SilentlySkipped()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"sw-signer-empty-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);
        try
        {
            using var registry = PluginPackageSignerRegistry.LoadFrom("", tempDir);

            Assert.Contains("builtin-ecdsa", registry.Providers.Keys);
        }
        finally { Directory.Delete(tempDir, true); }
    }

    [Fact]
    public void LoadFrom_PluginDirWithMalformedDll_SkipsAndKeepsBuiltin()
    {
        var rootDir = Path.Combine(Path.GetTempPath(), $"sw-signer-bad-{Guid.NewGuid():N}");
        var pluginDir = Path.Combine(rootDir, "broken-plugin");
        Directory.CreateDirectory(pluginDir);
        File.WriteAllBytes(Path.Combine(pluginDir, "not-an-assembly.dll"), [0x4D, 0x5A, 0x90, 0x00]);

        try
        {
            using var registry = PluginPackageSignerRegistry.LoadFrom(rootDir);

            Assert.Single(registry.Providers);
        }
        finally { Directory.Delete(rootDir, true); }
    }

    [Fact]
    public void LoadFrom_PluginDirWithLibSubdir_ScansLibInstead()
    {
        var rootDir = Path.Combine(Path.GetTempPath(), $"sw-signer-lib-{Guid.NewGuid():N}");
        var libDir = Path.Combine(rootDir, "lib-plugin", "lib");
        Directory.CreateDirectory(libDir);
        // garbage bytes -> BadImageFormatException on load, swallowed by the registry
        File.WriteAllBytes(Path.Combine(libDir, "garbage.dll"), [0x00, 0x00, 0x00, 0x00]);

        try
        {
            using var registry = PluginPackageSignerRegistry.LoadFrom(rootDir);

            Assert.Single(registry.Providers);
        }
        finally { Directory.Delete(rootDir, true); }
    }

    [Fact]
    public void GetProvider_NullOrWhitespace_Throws()
    {
        using var registry = PluginPackageSignerRegistry.LoadFrom();

        Assert.Throws<ArgumentNullException>(() => registry.GetProvider(null!));
        Assert.Throws<ArgumentException>(() => registry.GetProvider(""));
        Assert.Throws<ArgumentException>(() => registry.GetProvider("   "));
    }

    [Fact]
    public void Dispose_Twice_DoesNotThrow_AndClearsProviders()
    {
        var registry = PluginPackageSignerRegistry.LoadFrom();
        Assert.NotEmpty(registry.Providers);

        registry.Dispose();
        registry.Dispose();

        Assert.Empty(registry.Providers);
    }
}
