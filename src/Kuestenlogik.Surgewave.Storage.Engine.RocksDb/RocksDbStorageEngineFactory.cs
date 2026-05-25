using Kuestenlogik.Surgewave.Storage.Engine;

namespace Kuestenlogik.Surgewave.Storage.Engine.RocksDb;

/// <summary>
/// Factory for creating RocksDB storage engines.
/// </summary>
public sealed class RocksDbStorageEngineFactory : ISurgewaveStorageEngineFactory
{
    private readonly ISurgewaveBufferPool? _bufferPool;
    private readonly long _defaultMaxSize;

    public RocksDbStorageEngineFactory(
        ISurgewaveBufferPool? bufferPool = null,
        long defaultMaxSize = 1024L * 1024 * 1024)
    {
        _bufferPool = bufferPool;
        _defaultMaxSize = defaultMaxSize;
    }

    public ISurgewaveStorageEngine Create(string directory, long baseOffset, long maxSize)
    {
        var dbPath = Path.Combine(directory, $"{baseOffset:D20}.rocksdb");
        return new RocksDbStorageEngine(dbPath, baseOffset, maxSize, createNew: true, _bufferPool);
    }

    public ISurgewaveStorageEngine Open(string directory, long baseOffset)
    {
        var dbPath = Path.Combine(directory, $"{baseOffset:D20}.rocksdb");
        return new RocksDbStorageEngine(dbPath, baseOffset, _defaultMaxSize, createNew: false, _bufferPool);
    }
}

/// <summary>
/// Factory methods for creating RocksDB-backed log segment factories.
/// </summary>
public static class RocksDbLogSegmentFactory
{
    /// <summary>
    /// Create a RocksDB storage factory.
    /// </summary>
    public static Kuestenlogik.Surgewave.Core.Storage.ILogSegmentFactory Create(ISurgewaveBufferPool? bufferPool = null)
    {
        var engineFactory = new RocksDbStorageEngineFactory(bufferPool);
        return new StorageEngineSegmentFactory(engineFactory, isPersistent: true);
    }
}
