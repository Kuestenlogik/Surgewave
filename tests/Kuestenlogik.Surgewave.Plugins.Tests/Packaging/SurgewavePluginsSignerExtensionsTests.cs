using Kuestenlogik.Surgewave.Plugins.Packaging;
using Kuestenlogik.Surgewave.Plugins.Packaging.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

/// <summary>
/// Regression tests for #32. The Broker (and any other host that calls
/// <c>AddSurgewavePluginSigner</c>) used to crash at DI resolution when
/// no <c>Surgewave:Plugins:Signer</c> section was configured — the eager
/// singleton factory called <c>BuiltinEcdsaSignerProvider.Create</c> with
/// an empty options dict and that provider rejects it. Per the documented
/// intent ("a fresh install can run without a configured trust store"),
/// the empty-options path now resolves to the no-op <c>BuiltinEcdsaSigner</c>
/// without going through the provider.
/// </summary>
public sealed class SurgewavePluginsSignerExtensionsTests
{
    [Fact]
    public void ResolveSigner_NoConfigSection_ReturnsNoOpBuiltinSigner()
    {
        var pluginsDir = Directory.CreateTempSubdirectory("surgewave-test-").FullName;
        try
        {
            var services = new ServiceCollection();
            var config = new ConfigurationBuilder().Build(); // empty
            services.AddSurgewavePluginSigner(config, pluginsDir);
            using var provider = services.BuildServiceProvider();

            var signer = provider.GetRequiredService<ISppSigner>();

            Assert.IsType<BuiltinEcdsaSigner>(signer);
            Assert.Equal("builtin-ecdsa", signer.Name);
            // No-op signer has neither key nor trust dir → HasSignature on a
            // non-existent path just returns false, never throws.
            Assert.False(signer.HasSignature(Path.Combine(pluginsDir, "does-not-exist.swpkg")));
        }
        finally
        {
            Directory.Delete(pluginsDir, recursive: true);
        }
    }

    [Fact]
    public void ResolveSigner_ConfiguredTrustDir_StillGoesThroughProvider()
    {
        var pluginsDir = Directory.CreateTempSubdirectory("surgewave-test-").FullName;
        var trustDir = Directory.CreateTempSubdirectory("surgewave-trust-").FullName;
        try
        {
            var services = new ServiceCollection();
            var config = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["Surgewave:Plugins:Signer:Name"] = "builtin-ecdsa",
                    ["Surgewave:Plugins:Signer:Options:trusted-keys-dir"] = trustDir,
                })
                .Build();
            services.AddSurgewavePluginSigner(config, pluginsDir);
            using var provider = services.BuildServiceProvider();

            var signer = provider.GetRequiredService<ISppSigner>();

            Assert.IsType<BuiltinEcdsaSigner>(signer);
        }
        finally
        {
            Directory.Delete(pluginsDir, recursive: true);
            Directory.Delete(trustDir, recursive: true);
        }
    }
}
