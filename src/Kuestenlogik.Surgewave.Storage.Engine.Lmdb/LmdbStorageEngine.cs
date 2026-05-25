using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Storage.Engine;
using LightningDB;

namespace Kuestenlogik.Surgewave.Storage.Engine.Lmdb;

/// <summary>
/// LMDB-based storage engine with memory-mapped B+Tree.
/// Extremely fast reads via memory-mapping, ACID transactions, zero-copy reads.
/// Ideal for read-heavy workloads with moderate write rates.
/// </summary>
public sealed class LmdbStorageEngine : ISurgewaveStorageEngine
{
    private readonly LightningEnvironment _env;
    private readonly string _dbPath;
    private readonly long _baseOffset;
    private readonly long _maxSize;
    private readonly ISurgewaveBufferPool _bufferPool;

    private const string DataDbName = "data";
    private const string MetaDbName = "meta";
    private const string TimestampDbName = "timestamp";

    private long _currentOffset;
    private long _size;
    private long _maxTimestamp;
    private long? _firstOffset;
    private bool _disposed;

    private readonly object _writeLock = new();

    public long BaseOffset => _baseOffset;
    public long CurrentOffset => Volatile.Read(ref _currentOffset);
    public long Size => Volatile.Read(ref _size);
    public bool IsFull => Size >= _maxSize;
    public DateTime CreatedAt { get; }
    public long MaxTimestamp => Volatile.Read(ref _maxTimestamp);
    public long? FirstOffset => _firstOffset;

    public LmdbStorageEngine(
        string dbPath,
        long baseOffset,
        long maxSize,
        bool createNew,
        ISurgewaveBufferPool? bufferPool = null)
    {
        _dbPath = dbPath;
        _baseOffset = baseOffset;
        _currentOffset = baseOffset;
        _maxSize = maxSize;
        _bufferPool = bufferPool ?? DefaultSurgewaveBufferPool.Shared;
        CreatedAt = DateTime.UtcNow;

        Directory.CreateDirectory(dbPath);

        // Configure LMDB environment
        _env = new LightningEnvironment(dbPath)
        {
            MaxDatabases = 3,
            MapSize = maxSize * 2 // Allow some headroom
        };
        _env.Open(EnvironmentOpenFlags.WriteMap | EnvironmentOpenFlags.MapAsync);

        // Create databases
        using (var txn = _env.BeginTransaction())
        {
            txn.OpenDatabase(DataDbName, new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create | DatabaseOpenFlags.IntegerKey });
            txn.OpenDatabase(MetaDbName, new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create });
            txn.OpenDatabase(TimestampDbName, new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create | DatabaseOpenFlags.IntegerKey });
            txn.Commit();
        }

        if (!createNew)
        {
            LoadMetadata();
        }
        else
        {
            SaveMetadata();
        }
    }

    public ValueTask<(long baseOffset, int recordCount)> AppendAsync(
        ReadOnlySpan<byte> recordBatch,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var (batchBaseOffset, recordCount, maxTimestamp) = ParseBatchHeader(recordBatch);

        lock (_writeLock)
        {
            using var txn = _env.BeginTransaction();
            using var dataDb = txn.OpenDatabase(DataDbName, new DatabaseConfiguration { Flags = DatabaseOpenFlags.IntegerKey });
            using var tsDb = txn.OpenDatabase(TimestampDbName, new DatabaseConfiguration { Flags = DatabaseOpenFlags.IntegerKey });

            // Write batch data (key = offset as big-endian for proper ordering)
            var keyBuffer = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(keyBuffer, batchBaseOffset);
            txn.Put(dataDb, keyBuffer, recordBatch.ToArray());

            // Update timestamp index
            if (maxTimestamp > 0)
            {
                var tsKey = new byte[8];
                BinaryPrimitives.WriteInt64BigEndian(tsKey, maxTimestamp);
                txn.Put(tsDb, tsKey, keyBuffer);

                if (maxTimestamp > _maxTimestamp)
                {
                    Volatile.Write(ref _maxTimestamp, maxTimestamp);
                }
            }

            txn.Commit();

            _firstOffset ??= batchBaseOffset;
            Volatile.Write(ref _currentOffset, batchBaseOffset + recordCount);
            Volatile.Write(ref _size, _size + recordBatch.Length);
        }

        return ValueTask.FromResult((batchBaseOffset, recordCount));
    }

    public ValueTask<(long baseOffset, int recordCount)> AppendAsync(
        ISurgewaveBuffer recordBatch,
        CancellationToken cancellationToken = default)
    {
        return AppendAsync(recordBatch.Span, cancellationToken);
    }

    public ValueTask<IStorageReadLease> ReadAsync(
        long startOffset,
        int maxBytes,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (startOffset < _baseOffset || startOffset >= CurrentOffset)
        {
            return ValueTask.FromResult<IStorageReadLease>(EmptyStorageReadLease.Instance);
        }

        using var txn = _env.BeginTransaction(TransactionBeginFlags.ReadOnly);
        using var dataDb = txn.OpenDatabase(DataDbName, new DatabaseConfiguration { Flags = DatabaseOpenFlags.IntegerKey });

        // Find starting position
        var batchOffset = FindBatchOffsetForRead(txn, dataDb, startOffset);
        if (batchOffset == null)
        {
            return ValueTask.FromResult<IStorageReadLease>(EmptyStorageReadLease.Instance);
        }

        var batchOffsets = new List<int>();
        var batches = new List<byte[]>();
        var totalBytes = 0;

        using var cursor = txn.CreateCursor(dataDb);
        var keyBuffer = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(keyBuffer, batchOffset.Value);

        var resultCode = cursor.SetRange(keyBuffer);
        while (resultCode == MDBResultCode.Success)
        {
            var current = cursor.GetCurrent();
            var data = current.value.CopyToNewArray();

            if (totalBytes > 0 && totalBytes + data.Length > maxBytes)
                break;

            batchOffsets.Add(totalBytes);
            batches.Add(data);
            totalBytes += data.Length;

            resultCode = cursor.Next().resultCode;
        }

        if (totalBytes == 0)
        {
            return ValueTask.FromResult<IStorageReadLease>(EmptyStorageReadLease.Instance);
        }

        var combined = new byte[totalBytes];
        var position = 0;
        foreach (var batch in batches)
        {
            batch.CopyTo(combined, position);
            position += batch.Length;
        }

        var buffer = _bufferPool.Wrap(combined);
#pragma warning disable CA2000
        var lease = new StorageReadLease(buffer, batchOffsets);
#pragma warning restore CA2000

        return ValueTask.FromResult<IStorageReadLease>(lease);
    }

    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        lock (_writeLock)
        {
            SaveMetadata();
            _env.Flush(true);
        }

        return ValueTask.CompletedTask;
    }

    public long? FindOffsetByTimestamp(long targetTimestamp)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var txn = _env.BeginTransaction(TransactionBeginFlags.ReadOnly);
        using var tsDb = txn.OpenDatabase(TimestampDbName, new DatabaseConfiguration { Flags = DatabaseOpenFlags.IntegerKey });
        using var cursor = txn.CreateCursor(tsDb);

        var tsKey = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(tsKey, targetTimestamp);

        var resultCode = cursor.SetRange(tsKey);
        if (resultCode == MDBResultCode.Success)
        {
            var current = cursor.GetCurrent();
            var offsetBytes = current.value.CopyToNewArray();
            if (offsetBytes.Length == 8)
            {
                return BinaryPrimitives.ReadInt64BigEndian(offsetBytes);
            }
        }

        return null;
    }

    public void DeleteStorage()
    {
        if (Directory.Exists(_dbPath))
        {
            Directory.Delete(_dbPath, recursive: true);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        SaveMetadata();
        _env.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    private long? FindBatchOffsetForRead(LightningTransaction txn, LightningDatabase dataDb, long requestedOffset)
    {
        using var cursor = txn.CreateCursor(dataDb);
        var keyBuffer = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(keyBuffer, requestedOffset);

        // Try to find the entry at or before the requested offset
        var resultCode = cursor.SetRange(keyBuffer);
        if (resultCode == MDBResultCode.Success)
        {
            var current = cursor.GetCurrent();
            var foundOffset = BinaryPrimitives.ReadInt64BigEndian(current.key.CopyToNewArray());

            // If we found an exact match or a later entry, check if there's a previous entry
            if (foundOffset > requestedOffset)
            {
                var prevResult = cursor.Previous();
                if (prevResult.resultCode == MDBResultCode.Success)
                {
                    var prevCurrent = cursor.GetCurrent();
                    return BinaryPrimitives.ReadInt64BigEndian(prevCurrent.key.CopyToNewArray());
                }
            }
            return foundOffset;
        }

        // If no entry at or after requested offset, try the last entry
        var lastResult = cursor.Last();
        if (lastResult.resultCode == MDBResultCode.Success)
        {
            var lastCurrent = cursor.GetCurrent();
            var lastOffset = BinaryPrimitives.ReadInt64BigEndian(lastCurrent.key.CopyToNewArray());
            if (lastOffset <= requestedOffset)
            {
                return lastOffset;
            }
        }

        // Otherwise try first entry
        var firstResult = cursor.First();
        if (firstResult.resultCode == MDBResultCode.Success)
        {
            var firstCurrent = cursor.GetCurrent();
            return BinaryPrimitives.ReadInt64BigEndian(firstCurrent.key.CopyToNewArray());
        }

        return null;
    }

    private void LoadMetadata()
    {
        using var txn = _env.BeginTransaction(TransactionBeginFlags.ReadOnly);
        using var metaDb = txn.OpenDatabase(MetaDbName, new DatabaseConfiguration { Flags = DatabaseOpenFlags.None });

        if (txn.TryGet(metaDb, "currentOffset"u8.ToArray(), out var value))
        {
            _currentOffset = BinaryPrimitives.ReadInt64LittleEndian(value.AsSpan());
        }
        if (txn.TryGet(metaDb, "size"u8.ToArray(), out value))
        {
            _size = BinaryPrimitives.ReadInt64LittleEndian(value.AsSpan());
        }
        if (txn.TryGet(metaDb, "maxTimestamp"u8.ToArray(), out value))
        {
            _maxTimestamp = BinaryPrimitives.ReadInt64LittleEndian(value.AsSpan());
        }
        if (txn.TryGet(metaDb, "firstOffset"u8.ToArray(), out value))
        {
            _firstOffset = BinaryPrimitives.ReadInt64LittleEndian(value.AsSpan());
        }
    }

    private void SaveMetadata()
    {
        using var txn = _env.BeginTransaction();
        using var metaDb = txn.OpenDatabase(MetaDbName, new DatabaseConfiguration { Flags = DatabaseOpenFlags.None });
        var buffer = new byte[8];

        BinaryPrimitives.WriteInt64LittleEndian(buffer, _currentOffset);
        txn.Put(metaDb, "currentOffset"u8.ToArray(), buffer);

        BinaryPrimitives.WriteInt64LittleEndian(buffer, _size);
        txn.Put(metaDb, "size"u8.ToArray(), buffer);

        BinaryPrimitives.WriteInt64LittleEndian(buffer, _maxTimestamp);
        txn.Put(metaDb, "maxTimestamp"u8.ToArray(), buffer);

        if (_firstOffset.HasValue)
        {
            BinaryPrimitives.WriteInt64LittleEndian(buffer, _firstOffset.Value);
            txn.Put(metaDb, "firstOffset"u8.ToArray(), buffer);
        }

        txn.Commit();
    }

    private static (long baseOffset, int recordCount, long maxTimestamp) ParseBatchHeader(ReadOnlySpan<byte> recordBatch)
    {
        var baseOffset = BinaryPrimitives.ReadInt64BigEndian(recordBatch);
        var maxTimestamp = BinaryPrimitives.ReadInt64BigEndian(recordBatch.Slice(35));
        var recordCount = BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(57));
        return (baseOffset, recordCount, maxTimestamp);
    }
}
