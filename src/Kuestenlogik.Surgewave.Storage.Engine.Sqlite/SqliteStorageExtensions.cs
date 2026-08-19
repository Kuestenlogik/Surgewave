using Kuestenlogik.Surgewave.Storage.Engine;

namespace Kuestenlogik.Surgewave.Storage.Engine.Sqlite;

/// <summary>
/// Extension methods for configuring SQLite storage on any storage-configurable runtime builder.
/// </summary>
public static class SqliteStorageExtensions
{
    /// <summary>
    /// Configure SQLite storage with default settings.
    /// Single-file database with WAL mode, ACID transactions.
    /// Good for moderate workloads and easy backup.
    /// </summary>
    public static TBuilder WithSqliteStorage<TBuilder>(
        this TBuilder builder)
        where TBuilder : IStorageConfigurableBuilder
    {
        builder.UseStorage(() => SqliteLogSegmentFactory.Create());
        return builder;
    }

    /// <summary>
    /// Configure SQLite storage with a custom buffer pool.
    /// </summary>
    public static TBuilder WithSqliteStorage<TBuilder>(
        this TBuilder builder, ISurgewaveBufferPool bufferPool)
        where TBuilder : IStorageConfigurableBuilder
    {
        builder.UseStorage(() => SqliteLogSegmentFactory.Create(bufferPool));
        return builder;
    }
}
