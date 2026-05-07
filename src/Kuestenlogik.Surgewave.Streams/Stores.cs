namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Factory for creating state store suppliers. Use these methods to configure state stores
/// for tables, aggregations, and stateful transformations.
/// </summary>
public static class Stores
{
    /// <summary>Creates an in-memory key-value store supplier.</summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="name">The store name (used for interactive queries).</param>
    /// <param name="comparer">Optional key comparer for ordered range queries.</param>
    /// <returns>A key-value store supplier.</returns>
    public static KeyValueStoreSupplier<TKey, TValue> KeyValueStore<TKey, TValue>(string name, IComparer<TKey>? comparer = null)
        where TKey : notnull
        => new(name, comparer);

    /// <summary>Creates an in-memory window store supplier for time-windowed aggregations.</summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="name">The store name.</param>
    /// <param name="windowSize">The window duration.</param>
    /// <param name="retentionPeriod">How long to retain closed windows.</param>
    /// <returns>A window store supplier.</returns>
    public static WindowStoreSupplier<TKey, TValue> WindowStore<TKey, TValue>(
        string name,
        TimeSpan windowSize,
        TimeSpan retentionPeriod)
        where TKey : notnull
        => new(name, windowSize, retentionPeriod);

    /// <summary>Creates an in-memory session store supplier for session-windowed aggregations.</summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="name">The store name.</param>
    /// <param name="retentionPeriod">How long to retain expired sessions.</param>
    /// <returns>A session store supplier.</returns>
    public static SessionStoreSupplier<TKey, TValue> SessionStore<TKey, TValue>(
        string name,
        TimeSpan retentionPeriod)
        where TKey : notnull
        => new(name, retentionPeriod);

    /// <summary>
    /// Create a persistent key-value store that survives restarts.
    /// </summary>
    public static PersistentKeyValueStoreSupplier<TKey, TValue> PersistentKeyValueStore<TKey, TValue>(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        IComparer<TKey>? comparer = null)
        where TKey : notnull
        => new(name, keySerde, valueSerde, comparer);

    /// <summary>
    /// Create a RocksDB-backed key-value store for large state (>RAM).
    /// Uses LSM-trees on disk with block cache for hot data.
    /// </summary>
    public static RocksDbKeyValueStoreSupplier<TKey, TValue> RocksDbKeyValueStore<TKey, TValue>(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        IComparer<TKey>? comparer = null,
        RocksDbStoreConfig? config = null)
        where TKey : notnull
        => new(name, keySerde, valueSerde, comparer, config);

    /// <summary>
    /// Create a SQLite-backed key-value store.
    /// Zero native dependencies, ACID transactions, WAL mode.
    /// Good for medium-to-large state with strong durability guarantees.
    /// </summary>
    public static SqliteKeyValueStoreSupplier<TKey, TValue> SqliteKeyValueStore<TKey, TValue>(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        SqliteStoreConfig? config = null)
        where TKey : notnull
        => new(name, keySerde, valueSerde, config);

    /// <summary>
    /// Create a memory-mapped file key-value store.
    /// Zero external dependencies — custom LSM-tree with MemTable, WAL, and sorted segments.
    /// Uses OS page cache via MemoryMappedFile for efficient disk access.
    /// </summary>
    public static MappedFileKeyValueStoreSupplier<TKey, TValue> MappedFileKeyValueStore<TKey, TValue>(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        MappedFileStoreConfig? config = null)
        where TKey : notnull
        => new(name, keySerde, valueSerde, config);

    /// <summary>
    /// Create a persistent window store that survives restarts.
    /// </summary>
    public static PersistentWindowStoreSupplier<TKey, TValue> PersistentWindowStore<TKey, TValue>(
        string name,
        TimeSpan windowSize,
        TimeSpan retentionPeriod,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde)
        where TKey : notnull
        => new(name, windowSize, retentionPeriod, keySerde, valueSerde);
}
