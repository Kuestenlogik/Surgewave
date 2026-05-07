using Microsoft.Data.Sqlite;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// SQLite-backed key-value store for Streams state management.
/// Uses WAL mode, WITHOUT ROWID clustered index, and prepared statements.
/// Zero native dependencies — SQLite is embedded in Microsoft.Data.Sqlite.
/// Good for medium-to-large state with ACID guarantees.
/// </summary>
public sealed class SqliteKeyValueStore<TKey, TValue> : ByteBackedKeyValueStore<TKey, TValue>
    where TKey : notnull
{
    private readonly SqliteStoreConfig _config;

    // CA2213 false positive across the prepared-command fields below: all disposables are
    // released in CloseBackend which the base class invokes via Dispose(bool).
    #pragma warning disable CA2213
    private SqliteConnection? _connection;
    private SqliteCommand? _putCmd;
    private SqliteCommand? _getCmd;
    private SqliteCommand? _deleteCmd;
    private SqliteCommand? _countCmd;
    private SqliteCommand? _allCmd;
    private SqliteCommand? _rangeCmd;
    #pragma warning restore CA2213

    private bool _disposed;

    public override bool Persistent => true;

    public SqliteKeyValueStore(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        SqliteStoreConfig? config = null)
        : base(name, keySerde, valueSerde)
    {
        _config = config ?? new SqliteStoreConfig();
    }

    protected override void InitBackend(ProcessorContext context)
    {
        var stateDir = context.Config.StateDir ?? Path.Combine(Path.GetTempPath(), "surgewave-streams");
        var dbDir = Path.Combine(stateDir, context.ApplicationId, context.TaskId ?? "default", Name);
        Directory.CreateDirectory(dbDir);

        var dbPath = Path.Combine(dbDir, "state.db");
        var connStr = new SqliteConnectionStringBuilder
        {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared
        }.ToString();

        _connection = new SqliteConnection(connStr);
        _connection.Open();

        // Performance pragmas
        Exec("PRAGMA journal_mode=WAL");
        Exec($"PRAGMA synchronous={_config.SynchronousMode}");
        Exec($"PRAGMA cache_size={_config.CacheSizePages}");
        Exec($"PRAGMA page_size={_config.PageSizeBytes}");
        Exec($"PRAGMA mmap_size={_config.MmapSizeBytes}");

        // Clustered B-tree on key (WITHOUT ROWID)
        Exec("CREATE TABLE IF NOT EXISTS kv (key BLOB PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID");

        // Prepare reusable commands
        _putCmd = Prepare("INSERT OR REPLACE INTO kv (key, value) VALUES (@k, @v)");
        _putCmd.Parameters.Add("@k", SqliteType.Blob);
        _putCmd.Parameters.Add("@v", SqliteType.Blob);

        _getCmd = Prepare("SELECT value FROM kv WHERE key = @k");
        _getCmd.Parameters.Add("@k", SqliteType.Blob);

        _deleteCmd = Prepare("DELETE FROM kv WHERE key = @k RETURNING value");
        _deleteCmd.Parameters.Add("@k", SqliteType.Blob);

        _countCmd = Prepare("SELECT COUNT(*) FROM kv");

        _allCmd = Prepare("SELECT key, value FROM kv ORDER BY key");

        _rangeCmd = Prepare("SELECT key, value FROM kv WHERE key >= @from AND key <= @to ORDER BY key");
        _rangeCmd.Parameters.Add("@from", SqliteType.Blob);
        _rangeCmd.Parameters.Add("@to", SqliteType.Blob);

        RefreshApproximateEntriesFromBackend();
    }

    protected override byte[]? GetBytes(byte[] keyBytes)
    {
        if (_connection == null) return null;
        _getCmd!.Parameters[0].Value = keyBytes;
        return _getCmd.ExecuteScalar() as byte[];
    }

    protected override bool PutBytes(byte[] keyBytes, byte[] valueBytes)
    {
        if (_connection == null) return false;
        _putCmd!.Parameters[0].Value = keyBytes;
        _putCmd.Parameters[1].Value = valueBytes;
        _putCmd.ExecuteNonQuery();
        // INSERT OR REPLACE — we can't cheaply tell whether this was a new row or an
        // overwrite. Return true and let the base class increment; Flush reconciles via
        // RefreshApproximateEntriesFromBackend().
        return true;
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100",
        Justification = "SQL is hardcoded")]
    public override void PutAll(IEnumerable<KeyValue<TKey, TValue>> entries)
    {
        // SQLite bulk-insert optimisation: a single explicit transaction is ~100x faster
        // than per-key Puts because each Put would otherwise be an implicit transaction.
        if (_connection == null) return;

        using var transaction = _connection.BeginTransaction();
        using var batchCmd = _connection.CreateCommand();
        batchCmd.CommandText = "INSERT OR REPLACE INTO kv (key, value) VALUES (@k, @v)";
        batchCmd.Transaction = transaction;
        var kParam = batchCmd.Parameters.Add("@k", SqliteType.Blob);
        var vParam = batchCmd.Parameters.Add("@v", SqliteType.Blob);

        var count = 0;
        foreach (var entry in entries)
        {
            kParam.Value = KeySerde.Serialize(entry.Key);
            vParam.Value = ValueSerde.Serialize(entry.Value);
            batchCmd.ExecuteNonQuery();
            count++;
        }

        transaction.Commit();
        AddEntries(count);
        if (count > 0)
            Metrics?.RecordPut(count);
    }

    protected override byte[]? DeleteBytes(byte[] keyBytes)
    {
        if (_connection == null) return null;
        _deleteCmd!.Parameters[0].Value = keyBytes;
        return _deleteCmd.ExecuteScalar() as byte[];
    }

    protected override IEnumerable<(byte[] key, byte[] value)> RangeBytes(byte[] fromBytes, byte[] toBytes)
    {
        if (_connection == null) yield break;

        _rangeCmd!.Parameters[0].Value = fromBytes;
        _rangeCmd.Parameters[1].Value = toBytes;

        using var reader = _rangeCmd.ExecuteReader();
        while (reader.Read())
        {
            yield return ((byte[])reader[0], (byte[])reader[1]);
        }
    }

    protected override IEnumerable<(byte[] key, byte[] value)> AllBytes()
    {
        if (_connection == null) yield break;

        using var reader = _allCmd!.ExecuteReader();
        while (reader.Read())
        {
            yield return ((byte[])reader[0], (byte[])reader[1]);
        }
    }

    protected override void FlushBackend()
    {
        // WAL checkpoint — forces WAL content into the main database
        Exec("PRAGMA wal_checkpoint(PASSIVE)");
        RefreshApproximateEntriesFromBackend();
    }

    protected override void CloseBackend()
    {
        if (_disposed) return;

        _putCmd?.Dispose();
        _getCmd?.Dispose();
        _deleteCmd?.Dispose();
        _countCmd?.Dispose();
        _allCmd?.Dispose();
        _rangeCmd?.Dispose();

        _connection?.Close();
        _connection?.Dispose();
        _connection = null;
        _disposed = true;
    }

    private void RefreshApproximateEntriesFromBackend()
    {
        if (_connection == null) return;
        try
        {
            var result = _countCmd!.ExecuteScalar();
            if (result is long count)
                SetApproximateEntries(count);
        }
        catch { /* best-effort */ }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100",
        Justification = "SQL is hardcoded, not user input")]
    private void Exec(string sql)
    {
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100",
        Justification = "SQL is hardcoded, not user input")]
    private SqliteCommand Prepare(string sql)
    {
        var cmd = _connection!.CreateCommand();
        cmd.CommandText = sql;
        return cmd;
    }
}

/// <summary>
/// Configuration for SQLite state store.
/// </summary>
public sealed class SqliteStoreConfig
{
    /// <summary>
    /// SQLite synchronous mode. NORMAL is safe with WAL mode (default).
    /// FULL for maximum durability, OFF for maximum speed.
    /// </summary>
    public SqliteSynchronousMode SynchronousMode { get; init; } = SqliteSynchronousMode.Normal;

    /// <summary>
    /// Page cache size in pages (negative = KB). Default: -8000 (~8MB).
    /// </summary>
    public int CacheSizePages { get; init; } = -8000;

    /// <summary>
    /// Database page size in bytes (default: 4096).
    /// </summary>
    public int PageSizeBytes { get; init; } = 4096;

    /// <summary>
    /// Memory-mapped I/O size in bytes (default: 256MB).
    /// Set to 0 to disable mmap.
    /// </summary>
    public long MmapSizeBytes { get; init; } = 256 * 1024 * 1024;
}

public enum SqliteSynchronousMode
{
    Off = 0,
    Normal = 1,
    Full = 2,
    Extra = 3
}
