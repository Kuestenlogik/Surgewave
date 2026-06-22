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
///
/// After 56769ca the DI returns an <see cref="OptionsTrackingSigner"/>
/// wrapper that holds the concrete signer and swaps it on options reload;
/// the asserts here target the externally visible contract
/// (Name + HasSignature behaviour) rather than the wrapper type so the
/// tests pass through either implementation shape.
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

            // Goes through BuiltinEcdsaSignerProvider successfully → provider
            // path is wired. Reaching this line without a throw is the
            // assertion; Name == "builtin-ecdsa" double-checks the registry
            // resolved the provider with the configured Name.
            Assert.Equal("builtin-ecdsa", signer.Name);
        }
        finally
        {
            Directory.Delete(pluginsDir, recursive: true);
            Directory.Delete(trustDir, recursive: true);
        }
    }
}
