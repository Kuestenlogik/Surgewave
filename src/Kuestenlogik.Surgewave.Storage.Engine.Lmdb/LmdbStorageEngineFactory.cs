using Kuestenlogik.Surgewave.Storage.Engine;

namespace Kuestenlogik.Surgewave.Storage.Engine.Lmdb;

/// <summary>
/// Factory for creating LMDB storage engines.
/// </summary>
public sealed class LmdbStorageEngineFactory : ISurgewaveStorageEngineFactory
{
    private readonly ISurgewaveBufferPool? _bufferPool;
    private readonly long _defaultMaxSize;

    public LmdbStorageEngineFactory(
        ISurgewaveBufferPool? bufferPool = null,
        long defaultMaxSize = 1024L * 1024 * 1024)
    {
        _bufferPool = bufferPool;
        _defaultMaxSize = defaultMaxSize;
    }

    public ISurgewaveStorageEngine Create(string directory, long baseOffset, long maxSize)
    {
        var dbPath = Path.Combine(directory, $"{baseOffset:D20}.lmdb");
        return new LmdbStorageEngine(dbPath, baseOffset, maxSize, createNew: true, _bufferPool);
    }

    public ISurgewaveStorageEngine Open(string directory, long baseOffset)
    {
        var dbPath = Path.Combine(directory, $"{baseOffset:D20}.lmdb");
        return new LmdbStorageEngine(dbPath, baseOffset, _defaultMaxSize, createNew: false, _bufferPool);
    }
}

/// <summary>
/// Factory methods for creating LMDB-backed log segment factories.
/// </summary>
public static class LmdbLogSegmentFactory
{
    /// <summary>
    /// Create an LMDB storage factory.
    /// </summary>
    public static Kuestenlogik.Surgewave.Core.Storage.ILogSegmentFactory Create(ISurgewaveBufferPool? bufferPool = null)
    {
        var engineFactory = new LmdbStorageEngineFactory(bufferPool);
        return new StorageEngineSegmentFactory(engineFactory, isPersistent: true);
    }
}
