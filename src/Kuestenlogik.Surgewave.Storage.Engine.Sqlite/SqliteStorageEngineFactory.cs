using Kuestenlogik.Surgewave.Storage.Engine;

namespace Kuestenlogik.Surgewave.Storage.Engine.Sqlite;

/// <summary>
/// Factory for creating SQLite storage engines.
/// </summary>
public sealed class SqliteStorageEngineFactory : ISurgewaveStorageEngineFactory
{
    private readonly ISurgewaveBufferPool? _bufferPool;
    private readonly long _defaultMaxSize;

    public SqliteStorageEngineFactory(
        ISurgewaveBufferPool? bufferPool = null,
        long defaultMaxSize = 1024L * 1024 * 1024)
    {
        _bufferPool = bufferPool;
        _defaultMaxSize = defaultMaxSize;
    }

    public ISurgewaveStorageEngine Create(string directory, long baseOffset, long maxSize)
    {
        Directory.CreateDirectory(directory);
        var dbPath = Path.Combine(directory, $"{baseOffset:D20}.db");
        return new SqliteStorageEngine(dbPath, baseOffset, maxSize, createNew: true, _bufferPool);
    }

    public ISurgewaveStorageEngine Open(string directory, long baseOffset)
    {
        var dbPath = Path.Combine(directory, $"{baseOffset:D20}.db");
        return new SqliteStorageEngine(dbPath, baseOffset, _defaultMaxSize, createNew: false, _bufferPool);
    }
}

/// <summary>
/// Factory methods for creating SQLite-backed log segment factories.
/// </summary>
public static class SqliteLogSegmentFactory
{
    /// <summary>
    /// Create a SQLite storage factory.
    /// </summary>
    public static Kuestenlogik.Surgewave.Core.Storage.ILogSegmentFactory Create(ISurgewaveBufferPool? bufferPool = null)
    {
        var engineFactory = new SqliteStorageEngineFactory(bufferPool);
        return new StorageEngineSegmentFactory(engineFactory, isPersistent: true);
    }
}
