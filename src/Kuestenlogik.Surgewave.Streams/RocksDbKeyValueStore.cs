using RocksDbSharp;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// RocksDB-backed key-value store for Streams state management.
/// Unlike PersistentKeyValueStore (which loads all data into RAM),
/// this store keeps data on disk in sorted LSM-trees and only caches
/// hot blocks via RocksDB's internal block cache.
///
/// Scales to datasets much larger than available memory.
/// Production default in Kafka Streams.
/// </summary>
public sealed class RocksDbKeyValueStore<TKey, TValue> : ByteBackedKeyValueStore<TKey, TValue>
    where TKey : notnull
{
    private readonly RocksDbStoreConfig _storeConfig;

    // CA2213 false positive: _db is disposed via CloseBackend → Dispose(true) chain
    // inherited from ByteBackedKeyValueStore, but the analyzer can't trace the template
    // method call.
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage", "CA2213:Disposable fields should be disposed",
        Justification = "Disposed in CloseBackend which is called via the base class Dispose pattern.")]
    private RocksDbSharp.RocksDb? _db;

    private string _dbPath = "";
    private bool _disposed;

    public override bool Persistent => true;

    public RocksDbKeyValueStore(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        IComparer<TKey>? comparer = null,
        RocksDbStoreConfig? config = null)
        : base(name, keySerde, valueSerde)
    {
        // comparer is accepted for API compatibility but unused: RocksDB iterators
        // are byte-lexicographic on the serialized key bytes, which is the only
        // ordering the base class needs for Range queries.
        _ = comparer;
        _storeConfig = config ?? new RocksDbStoreConfig();
    }

    protected override void InitBackend(ProcessorContext context)
    {
        var stateDir = context.Config.StateDir ?? Path.Combine(Path.GetTempPath(), "surgewave-streams");
        _dbPath = Path.Combine(stateDir, context.ApplicationId, context.TaskId ?? "default", Name);
        Directory.CreateDirectory(_dbPath);

        var options = new DbOptions()
            .SetCreateIfMissing(true)
            .SetMaxBackgroundCompactions(_storeConfig.MaxBackgroundCompactions)
            .SetMaxBackgroundFlushes(_storeConfig.MaxBackgroundFlushes);

        // Column family options for the default CF (performance tuning)
        var cfOptions = new ColumnFamilyOptions()
            .SetWriteBufferSize((ulong)_storeConfig.WriteBufferSizeBytes)
            .SetMaxWriteBufferNumber(_storeConfig.MaxWriteBufferNumber)
            .SetCompression(Compression.Lz4);

        // Block cache for hot data + bloom filter
        var tableOptions = new BlockBasedTableOptions();
        tableOptions.SetBlockCache(Cache.CreateLru((ulong)_storeConfig.BlockCacheSizeBytes));
        tableOptions.SetBlockSize((ulong)_storeConfig.BlockSizeBytes);
        tableOptions.SetFilterPolicy(BloomFilterPolicy.Create(10));
        cfOptions.SetBlockBasedTableFactory(tableOptions);

        var columnFamilies = new ColumnFamilies
        {
            { "default", cfOptions }
        };

        _db = RocksDbSharp.RocksDb.Open(options, _dbPath, columnFamilies);

        // Reconcile the approximate entry counter with the backend's estimate.
        RefreshApproximateEntriesFromBackend();
    }

    protected override byte[]? GetBytes(byte[] keyBytes)
        => _db?.Get(keyBytes);

    protected override bool PutBytes(byte[] keyBytes, byte[] valueBytes)
    {
        if (_db == null) return false;

        // RocksDB Put is blind (overwrite), so we don't know whether the key existed. We
        // return true unconditionally and let the base class increment the counter, then
        // rely on RefreshApproximateEntriesFromBackend() during Flush to reconcile any
        // overcount on overwrites.
        _db.Put(keyBytes, valueBytes);
        return true;
    }

    public override void PutAll(IEnumerable<KeyValue<TKey, TValue>> entries)
    {
        // RocksDB ships a native WriteBatch — dramatically faster than per-key Puts because
        // it fsyncs once at the end. Override the base class helper to take advantage.
        if (_db == null) return;

        using var batch = new WriteBatch();
        var count = 0;

        foreach (var entry in entries)
        {
            var keyBytes = KeySerde.Serialize(entry.Key);
            var valueBytes = ValueSerde.Serialize(entry.Value);
            batch.Put(keyBytes, valueBytes);
            count++;
        }

        _db.Write(batch);
        AddEntries(count);
        Metrics?.RecordPut(count);
    }

    protected override byte[]? DeleteBytes(byte[] keyBytes)
    {
        if (_db == null) return null;

        var existing = _db.Get(keyBytes);
        if (existing == null) return null;

        _db.Remove(keyBytes);
        return existing;
    }

    protected override IEnumerable<(byte[] key, byte[] value)> RangeBytes(byte[] fromBytes, byte[] toBytes)
    {
        if (_db == null) yield break;

        using var iterator = _db.NewIterator();
        iterator.Seek(fromBytes);

        while (iterator.Valid())
        {
            var keyBytes = iterator.Key();

            // Compare with upper bound
            if (CompareBytes(keyBytes, toBytes) > 0)
                break;

            yield return (keyBytes, iterator.Value());
            iterator.Next();
        }
    }

    protected override IEnumerable<(byte[] key, byte[] value)> AllBytes()
    {
        if (_db == null) yield break;

        using var iterator = _db.NewIterator();
        iterator.SeekToFirst();

        while (iterator.Valid())
        {
            yield return (iterator.Key(), iterator.Value());
            iterator.Next();
        }
    }

    protected override void FlushBackend()
    {
        _db?.Flush(new FlushOptions());
        RefreshApproximateEntriesFromBackend();
    }

    protected override void CloseBackend()
    {
        if (_disposed) return;
        _db?.Dispose();
        _db = null;
        _disposed = true;
    }

    private void RefreshApproximateEntriesFromBackend()
    {
        if (_db == null) return;
        try
        {
            var estimate = _db.GetProperty("rocksdb.estimate-num-keys");
            if (long.TryParse(estimate, out var count))
                SetApproximateEntries(count);
        }
        catch
        {
            // Ignore — estimate is best-effort
        }
    }

    private static int CompareBytes(byte[] a, byte[] b)
    {
        var len = Math.Min(a.Length, b.Length);
        for (var i = 0; i < len; i++)
        {
            if (a[i] != b[i])
                return a[i].CompareTo(b[i]);
        }
        return a.Length.CompareTo(b.Length);
    }
}

/// <summary>
/// Configuration for RocksDB state store.
/// </summary>
public sealed class RocksDbStoreConfig
{
    /// <summary>
    /// Block cache size for hot data (default: 64MB).
    /// </summary>
    public long BlockCacheSizeBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>
    /// Write buffer (memtable) size before flush to SST (default: 16MB).
    /// </summary>
    public long WriteBufferSizeBytes { get; init; } = 16 * 1024 * 1024;

    /// <summary>
    /// Max number of memtables before stalling writes (default: 3).
    /// </summary>
    public int MaxWriteBufferNumber { get; init; } = 3;

    /// <summary>
    /// SST block size (default: 16KB).
    /// </summary>
    public long BlockSizeBytes { get; init; } = 16 * 1024;

    /// <summary>
    /// Max background compaction threads (default: 2).
    /// </summary>
    public int MaxBackgroundCompactions { get; init; } = 2;

    /// <summary>
    /// Max background flush threads (default: 1).
    /// </summary>
    public int MaxBackgroundFlushes { get; init; } = 1;
}
