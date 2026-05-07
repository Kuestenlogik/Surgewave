using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Storage.Engine;

namespace Kuestenlogik.Surgewave.Storage.Engine.Sqlite;

/// <summary>
/// Extension methods for configuring SQLite storage on SurgewaveRuntimeBuilder.
/// </summary>
public static class SqliteStorageExtensions
{
    /// <summary>
    /// Configure SQLite storage with default settings.
    /// Single-file database with WAL mode, ACID transactions.
    /// Good for moderate workloads and easy backup.
    /// </summary>
    public static SurgewaveRuntimeBuilder WithSqliteStorage(this SurgewaveRuntimeBuilder builder)
    {
        return builder.WithStorage(() => SqliteLogSegmentFactory.Create());
    }

    /// <summary>
    /// Configure SQLite storage with a custom buffer pool.
    /// </summary>
    public static SurgewaveRuntimeBuilder WithSqliteStorage(this SurgewaveRuntimeBuilder builder, ISurgewaveBufferPool bufferPool)
    {
        return builder.WithStorage(() => SqliteLogSegmentFactory.Create(bufferPool));
    }
}
