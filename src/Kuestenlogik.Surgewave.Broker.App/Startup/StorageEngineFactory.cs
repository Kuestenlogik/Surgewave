using Kuestenlogik.Surgewave.Broker.Plugins;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;

namespace Kuestenlogik.Surgewave.Broker.Startup;

/// <summary>
/// Factory for creating log segment factories based on storage engine configuration.
/// All engines — built-in and enterprise — are discovered via IStorageEnginePlugin.
/// </summary>
public static class StorageEngineFactory
{
    /// <summary>
    /// Creates a log segment factory by discovering the matching IStorageEnginePlugin.
    /// </summary>
    public static ILogSegmentFactory Create(string storageEngine, Microsoft.Extensions.Configuration.IConfiguration? configuration = null)
    {
        // ObjectStore has special handling (provider-based)
        if (string.Equals(storageEngine, StorageEngines.ObjectStore, StringComparison.OrdinalIgnoreCase))
            return CreateObjectStoreFactory();

        // Discover all storage engine plugins (built-in + enterprise)
        var plugins = BrokerPluginActivator.Discover<IStorageEnginePlugin>();
        var match = plugins.FirstOrDefault(p =>
            p.SupportedModes.Any(m => m.Equals(storageEngine, StringComparison.OrdinalIgnoreCase)));

        if (match is not null)
            return match.CreateFactory(storageEngine, configuration!);

        throw new InvalidOperationException(
            $"Unknown storage engine '{storageEngine}'. Available: {string.Join(", ", plugins.SelectMany(p => p.SupportedModes))}. " +
            $"Install a storage engine plugin or use a built-in engine (file, memory, zerocopy-wal, zerocopy-memory, objectstore).");
    }

    /// <summary>
    /// Creates a log segment factory, using custom factory if provided, otherwise by engine name.
    /// </summary>
    public static ILogSegmentFactory Create(string storageEngine, Func<ILogSegmentFactory>? customFactory)
    {
        if (customFactory != null)
            return customFactory();
        return Create(storageEngine);
    }

    /// <summary>
    /// Creates an ObjectStore log segment factory with the given provider.
    /// </summary>
    public static ILogSegmentFactory CreateObjectStore(
        IObjectStoreProvider storeProvider,
        ObjectStoreConfig? config = null)
    {
        return ObjectStoreLogSegmentFactory.Create(storeProvider, config);
    }

    /// <summary>
    /// Creates an ObjectStore log segment factory from a provider configuration.
    /// </summary>
    public static ILogSegmentFactory CreateObjectStore(
        ObjectStoreProviderConfig providerConfig,
        ObjectStoreConfig? config = null)
    {
        var provider = ObjectStoreProviderFactory.Create(providerConfig);
        return ObjectStoreLogSegmentFactory.Create(provider, config);
    }

    private static ObjectStoreLogSegmentFactory CreateObjectStoreFactory()
    {
        var provider = new LocalFileObjectStoreProvider("./zero-disk-storage");
        return ObjectStoreLogSegmentFactory.Create(provider);
    }
}
