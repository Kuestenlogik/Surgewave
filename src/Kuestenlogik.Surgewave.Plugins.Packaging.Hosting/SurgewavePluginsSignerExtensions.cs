using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Surgewave.Plugins.Packaging.Hosting;

/// <summary>
/// DI wiring for the signer verifier used by plugin installation. The registry is a singleton
/// that owns the loaded <see cref="System.Runtime.Loader.AssemblyLoadContext"/> instances for
/// the life of the host; the verifier itself is resolved lazily from config so appsettings
/// changes under <c>IOptionsMonitor</c> don't leak stale verifier instances.
/// </summary>
public static class SurgewavePluginsSignerExtensions
{
    /// <summary>
    /// Registers <see cref="PluginPackageSignerRegistry"/> (scanning <paramref name="pluginsDir"/>) and
    /// <see cref="ISppSigner"/> (built from <see cref="SignerOptions"/> at resolve time) in DI.
    /// Call once during host setup. The registry is disposed when the root
    /// <see cref="IServiceProvider"/> is disposed.
    /// </summary>
    public static IServiceCollection AddSurgewavePluginSigner(
        this IServiceCollection services,
        IConfiguration configuration,
        string pluginsDir)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(pluginsDir);

        services.Configure<SignerOptions>(configuration.GetSection(SignerOptions.ConfigSection));

        services.AddSingleton(sp => PluginPackageSignerRegistry.LoadFrom(pluginsDir));
        services.AddSingleton<ISppSigner>(sp =>
        {
            var registry = sp.GetRequiredService<PluginPackageSignerRegistry>();
            var options = sp.GetRequiredService<IOptions<SignerOptions>>().Value;
            // Operator hasn't configured a signer (no Surgewave:Plugins:Signer:Options
            // section) — fall back to the no-op BuiltinEcdsaSigner so a fresh checkout
            // boots without a trust store, as SignerOptions.RequireSignedPackages's
            // doc comment promises. Once the operator fills in `private-key` /
            // `trusted-keys-dir`, the provider path takes over.
            if (options.Options.Count == 0)
            {
                return new BuiltinEcdsaSigner();
            }
            var provider = registry.GetProvider(options.Name);
            return provider.Create(options.Options);
        });

        return services;
    }
}
