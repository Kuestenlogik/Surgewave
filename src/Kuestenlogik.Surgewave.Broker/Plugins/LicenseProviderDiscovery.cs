using Kuestenlogik.Surgewave.Plugins.Licensing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Plugins;

/// <summary>
/// Discovers and initializes an <see cref="ILicenseProvider"/> before plugin activation.
/// If Surgewave.Licensing is present as an assembly, its provider is instantiated and registered.
/// If not, the broker runs in Community Edition mode (no enterprise features).
/// </summary>
public static class LicenseProviderDiscovery
{
    /// <summary>
    /// Scans loaded assemblies for an <see cref="ILicenseProvider"/> implementation,
    /// initializes it, and registers it in DI.
    /// Returns the provider (or null for Community Edition).
    /// </summary>
    public static ILicenseProvider? Discover(
        IServiceCollection services,
        IConfiguration configuration,
        ILogger? logger = null)
    {
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            var name = assembly.GetName().Name;
            if (name is null || !name.StartsWith("Kuestenlogik.Surgewave.", StringComparison.OrdinalIgnoreCase))
                continue;

            try
            {
                foreach (var type in assembly.GetExportedTypes())
                {
                    if (type.IsAbstract || type.IsInterface || !typeof(ILicenseProvider).IsAssignableFrom(type))
                        continue;

                    if (Activator.CreateInstance(type) is ILicenseProvider provider)
                    {
                        services.AddSingleton(provider);

                        logger?.LogInformation(
                            "License provider loaded: {Type} — Edition: {Edition}, Licensed to: {LicensedTo}",
                            type.Name, provider.Edition, provider.LicensedTo ?? "Community");

                        return provider;
                    }
                }
            }
            catch
            {
                // Skip assemblies that fail type enumeration
            }
        }

        logger?.LogInformation("No license provider found — running as Community Edition");
        return null;
    }
}
