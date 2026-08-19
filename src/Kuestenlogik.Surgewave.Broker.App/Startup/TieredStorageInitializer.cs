using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Storage.Tiering;
using Microsoft.Extensions.Logging;
using CoreTieredStorageConfig = Kuestenlogik.Surgewave.Storage.Tiering.TieredStorageConfig;

namespace Kuestenlogik.Surgewave.Broker.Startup;

/// <summary>
/// Initializes tiered storage (cloud object storage) if configured.
/// Discovers tiered storage provider plugins from the plugins directory.
/// </summary>
public static class TieredStorageInitializer
{
    /// <summary>
    /// Registers built-in providers and discovers additional providers from plugins.
    /// </summary>
    public static void RegisterProviders(ILogger? logger = null)
    {
        // Built-in: local filesystem provider is always available (registered by TieredStorageManager itself)

        // Discover and register plugin-provided providers (Azure, S3, GCP, etc.)
        var plugins = DiscoverTieredStoragePlugins();
        foreach (var plugin in plugins)
        {
            plugin.Register();
            logger?.LogInformation("Registered tiered storage provider: {Provider}", plugin.ProviderName);
        }
    }

    /// <summary>
    /// Creates a tiered storage manager if tiered storage is enabled in config.
    /// </summary>
    public static TieredStorageManager? Create(BrokerConfig config, ILogger logger)
    {
        if (!config.TieredStorage.Enabled)
        {
            return null;
        }

        var coreConfig = new CoreTieredStorageConfig
        {
            Enabled = config.TieredStorage.Enabled,
            Provider = config.TieredStorage.Provider,
            LocalPath = config.TieredStorage.LocalPath,
            AzureConnectionString = config.TieredStorage.AzureConnectionString,
            AzureContainerName = config.TieredStorage.AzureContainerName,
            S3BucketName = config.TieredStorage.S3BucketName,
            S3Region = config.TieredStorage.S3Region,
            GcpBucketName = config.TieredStorage.GcpBucketName,
            Prefix = config.TieredStorage.Prefix,
            LocalRetentionHours = config.TieredStorage.LocalRetentionHours,
            RemoteRetentionHours = config.TieredStorage.RemoteRetentionHours,
            TieringLagHours = config.TieredStorage.TieringLagHours,
            MinSegmentSizeBytes = config.TieredStorage.MinSegmentSizeBytes,
            LocalCacheSizeBytes = config.TieredStorage.LocalCacheSizeBytes,
            LocalCachePath = config.TieredStorage.LocalCachePath,
            DeleteAfterUpload = config.TieredStorage.DeleteAfterUpload,
            TieringIntervalSeconds = config.TieredStorage.TieringIntervalSeconds
        };

        logger.LogInformation("Tiered storage enabled (provider: {Provider})", config.TieredStorage.Provider);
        return new TieredStorageManager(coreConfig, config.DataDirectory);
    }

    private static List<ITieredStoragePlugin> DiscoverTieredStoragePlugins()
    {
        var plugins = new List<ITieredStoragePlugin>();

        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            var name = assembly.GetName().Name;
            if (name == null || !name.StartsWith("Kuestenlogik.Surgewave.", StringComparison.OrdinalIgnoreCase))
                continue;

            try
            {
                foreach (var type in assembly.GetExportedTypes())
                {
                    if (type.IsAbstract || type.IsInterface || !typeof(ITieredStoragePlugin).IsAssignableFrom(type))
                        continue;

                    if (Activator.CreateInstance(type) is ITieredStoragePlugin instance)
                        plugins.Add(instance);
                }
            }
            catch
            {
                // Skip assemblies that fail type enumeration
            }
        }

        return plugins;
    }
}
