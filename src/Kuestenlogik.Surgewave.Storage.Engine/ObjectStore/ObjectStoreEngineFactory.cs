using Kuestenlogik.Surgewave.Storage;

namespace Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;

/// <summary>
/// Factory for creating ObjectStoreEngine instances.
/// Each engine maps to a topic-partition segment backed by object storage.
/// </summary>
public sealed class ObjectStoreEngineFactory : ISurgewaveStorageEngineFactory
{
    private readonly IObjectStoreProvider _storeProvider;
    private readonly ObjectStoreConfig _config;
    private readonly ISurgewaveBufferPool? _bufferPool;

    public ObjectStoreEngineFactory(
        IObjectStoreProvider storeProvider,
        ObjectStoreConfig config,
        ISurgewaveBufferPool? bufferPool = null)
    {
        _storeProvider = storeProvider ?? throw new ArgumentNullException(nameof(storeProvider));
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _bufferPool = bufferPool;
    }

    public ISurgewaveStorageEngine Create(string directory, long baseOffset, long maxSize)
    {
        // For object store, the directory is used to derive topic/partition path
        var (topic, partition) = ParseDirectoryPath(directory);

        return new ObjectStoreEngine(
            _storeProvider,
            _config,
            topic,
            partition,
            baseOffset,
            maxSize,
            _bufferPool);
    }

    public ISurgewaveStorageEngine Open(string directory, long baseOffset)
    {
        var (topic, partition) = ParseDirectoryPath(directory);

        return new ObjectStoreEngine(
            _storeProvider,
            _config,
            topic,
            partition,
            baseOffset,
            _config.DefaultMaxSegmentSize,
            _bufferPool);
    }

    /// <summary>
    /// Parse directory path to extract topic and partition.
    /// Expected format: ".../{topic}/{partition}" or ".../{topic}-{partition}"
    /// Falls back to using the directory name as topic with partition 0.
    /// </summary>
    private static (string topic, int partition) ParseDirectoryPath(string directory)
    {
        if (string.IsNullOrEmpty(directory))
            return ("default", 0);

        var normalized = directory.Replace('\\', '/').TrimEnd('/');
        var parts = normalized.Split('/');

        if (parts.Length >= 2 && int.TryParse(parts[^1], out var partitionNum))
        {
            return (parts[^2], partitionNum);
        }

        // Try topic-partition format
        var lastPart = parts[^1];
        var dashIdx = lastPart.LastIndexOf('-');
        if (dashIdx > 0 && int.TryParse(lastPart[(dashIdx + 1)..], out var partNum))
        {
            return (lastPart[..dashIdx], partNum);
        }

        return (lastPart, 0);
    }
}
