using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Storage.Engine;
using RocksDbSharp;

namespace Kuestenlogik.Surgewave.Storage.Engine.RocksDb;

/// <summary>
/// RocksDB-based storage engine with LSM-tree optimizations.
/// Ideal for write-heavy workloads with automatic compaction.
/// </summary>
public sealed class RocksDbStorageEngine : ISurgewaveStorageEngine
{
    private readonly RocksDbSharp.RocksDb _db;
    private readonly string _dbPath;
    private readonly long _baseOffset;
    private readonly long _maxSize;
    private readonly ISurgewaveBufferPool _bufferPool;

    private long _currentOffset;
    private long _size;
    private long _maxTimestamp;
    private long? _firstOffset;
    private bool _disposed;

    // Column family names
    private const string DataCfName = "data";
    private const string MetaCfName = "meta";
    private const string TimestampIdxCfName = "ts_idx";

    private readonly ColumnFamilyHandle _dataCf;
    private readonly ColumnFamilyHandle _metaCf;
    private readonly ColumnFamilyHandle _timestampIdxCf;

    // Reusable key buffers
    private readonly byte[] _keyBuffer = new byte[8];

    private readonly object _writeLock = new();

    public long BaseOffset => _baseOffset;
    public long CurrentOffset => Volatile.Read(ref _currentOffset);
    public long Size => Volatile.Read(ref _size);
    public bool IsFull => Size >= _maxSize;
    public DateTime CreatedAt { get; }
    public long MaxTimestamp => Volatile.Read(ref _maxTimestamp);
    public long? FirstOffset => _firstOffset;

    public RocksDbStorageEngine(
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

        var options = new DbOptions()
            .SetCreateIfMissing(true)
            .SetCreateMissingColumnFamilies(true)
            .SetMaxBackgroundCompactions(4)
            .SetMaxBackgroundFlushes(2)
            .EnableStatistics();

        var cfOptions = new ColumnFamilyOptions()
            .SetCompression(Compression.Lz4)
            .SetWriteBufferSize(64 * 1024 * 1024)
            .SetMaxWriteBufferNumber(3)
            .SetTargetFileSizeBase(64 * 1024 * 1024);

        var columnFamilies = new ColumnFamilies
        {
            { "default", cfOptions },
            { DataCfName, cfOptions },
            { MetaCfName, new ColumnFamilyOptions() },
            { TimestampIdxCfName, cfOptions }
        };

        _db = RocksDbSharp.RocksDb.Open(options, dbPath, columnFamilies);

        _dataCf = _db.GetColumnFamily(DataCfName);
        _metaCf = _db.GetColumnFamily(MetaCfName);
        _timestampIdxCf = _db.GetColumnFamily(TimestampIdxCfName);

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
            // Write batch data
            BinaryPrimitives.WriteInt64BigEndian(_keyBuffer, batchBaseOffset);
            _db.Put(_keyBuffer, recordBatch.ToArray(), _dataCf);

            // Update timestamp index
            if (maxTimestamp > 0)
            {
                Span<byte> tsKey = stackalloc byte[8];
                BinaryPrimitives.WriteInt64BigEndian(tsKey, maxTimestamp);
                _db.Put(tsKey.ToArray(), _keyBuffer, _timestampIdxCf);

                if (maxTimestamp > _maxTimestamp)
                {
                    Volatile.Write(ref _maxTimestamp, maxTimestamp);
                }
            }

            // Update state
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

        // Find the batch containing or immediately before startOffset
        var batchOffset = FindBatchOffsetForRead(startOffset);
        if (batchOffset == null)
        {
            return ValueTask.FromResult<IStorageReadLease>(EmptyStorageReadLease.Instance);
        }

        var batchOffsets = new List<int>();
        var batches = new List<byte[]>();
        var totalBytes = 0;
        var currentOffset = batchOffset.Value;

        // Read batches until we hit maxBytes
        using var iterator = _db.NewIterator(_dataCf);
        BinaryPrimitives.WriteInt64BigEndian(_keyBuffer, currentOffset);
        iterator.Seek(_keyBuffer);

        while (iterator.Valid())
        {
            var value = iterator.Value();
            if (value == null || value.Length < 12)
                break;

            var batchSize = value.Length;

            if (totalBytes > 0 && totalBytes + batchSize > maxBytes)
                break;

            batchOffsets.Add(totalBytes);
            batches.Add(value);
            totalBytes += batchSize;

            iterator.Next();
        }

        if (totalBytes == 0)
        {
            return ValueTask.FromResult<IStorageReadLease>(EmptyStorageReadLease.Instance);
        }

        // Combine batches into single buffer
        var combined = new byte[totalBytes];
        var position = 0;
        foreach (var batch in batches)
        {
            batch.CopyTo(combined, position);
            position += batch.Length;
        }

        var buffer = _bufferPool.Wrap(combined);
#pragma warning disable CA2000 // Ownership transferred to caller via IStorageReadLease
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
            var writeOptions = new WriteOptions().SetSync(true);
            // Force a sync by writing metadata with sync enabled
            _db.Put("flush_marker"u8.ToArray(), BitConverter.GetBytes(DateTime.UtcNow.Ticks), _metaCf, writeOptions);
        }

        return ValueTask.CompletedTask;
    }

    public long? FindOffsetByTimestamp(long targetTimestamp)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var iterator = _db.NewIterator(_timestampIdxCf);
        Span<byte> tsKey = stackalloc byte[8];
        BinaryPrimitives.WriteInt64BigEndian(tsKey, targetTimestamp);
        iterator.Seek(tsKey.ToArray());

        if (iterator.Valid())
        {
            var offsetBytes = iterator.Value();
            if (offsetBytes != null && offsetBytes.Length == 8)
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
        _db.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    private long? FindBatchOffsetForRead(long requestedOffset)
    {
        using var iterator = _db.NewIterator(_dataCf);
        BinaryPrimitives.WriteInt64BigEndian(_keyBuffer, requestedOffset);
        iterator.SeekForPrev(_keyBuffer);

        if (iterator.Valid())
        {
            var key = iterator.Key();
            if (key != null && key.Length == 8)
            {
                return BinaryPrimitives.ReadInt64BigEndian(key);
            }
        }

        // If SeekForPrev didn't find anything, try from the beginning
        iterator.SeekToFirst();
        if (iterator.Valid())
        {
            var key = iterator.Key();
            if (key != null && key.Length == 8)
            {
                var firstBatchOffset = BinaryPrimitives.ReadInt64BigEndian(key);
                if (firstBatchOffset <= requestedOffset)
                {
                    return firstBatchOffset;
                }
            }
        }

        return null;
    }

    private void LoadMetadata()
    {
        var currentOffsetBytes = _db.Get("currentOffset"u8.ToArray(), _metaCf);
        if (currentOffsetBytes != null && currentOffsetBytes.Length == 8)
        {
            _currentOffset = BinaryPrimitives.ReadInt64LittleEndian(currentOffsetBytes);
        }

        var sizeBytes = _db.Get("size"u8.ToArray(), _metaCf);
        if (sizeBytes != null && sizeBytes.Length == 8)
        {
            _size = BinaryPrimitives.ReadInt64LittleEndian(sizeBytes);
        }

        var maxTsBytes = _db.Get("maxTimestamp"u8.ToArray(), _metaCf);
        if (maxTsBytes != null && maxTsBytes.Length == 8)
        {
            _maxTimestamp = BinaryPrimitives.ReadInt64LittleEndian(maxTsBytes);
        }

        var firstOffsetBytes = _db.Get("firstOffset"u8.ToArray(), _metaCf);
        if (firstOffsetBytes != null && firstOffsetBytes.Length == 8)
        {
            _firstOffset = BinaryPrimitives.ReadInt64LittleEndian(firstOffsetBytes);
        }
    }

    private void SaveMetadata()
    {
        Span<byte> buffer = stackalloc byte[8];

        BinaryPrimitives.WriteInt64LittleEndian(buffer, _currentOffset);
        _db.Put("currentOffset"u8.ToArray(), buffer.ToArray(), _metaCf);

        BinaryPrimitives.WriteInt64LittleEndian(buffer, _size);
        _db.Put("size"u8.ToArray(), buffer.ToArray(), _metaCf);

        BinaryPrimitives.WriteInt64LittleEndian(buffer, _maxTimestamp);
        _db.Put("maxTimestamp"u8.ToArray(), buffer.ToArray(), _metaCf);

        if (_firstOffset.HasValue)
        {
            BinaryPrimitives.WriteInt64LittleEndian(buffer, _firstOffset.Value);
            _db.Put("firstOffset"u8.ToArray(), buffer.ToArray(), _metaCf);
        }
    }

    private static (long baseOffset, int recordCount, long maxTimestamp) ParseBatchHeader(ReadOnlySpan<byte> recordBatch)
    {
        var baseOffset = BinaryPrimitives.ReadInt64BigEndian(recordBatch);
        var maxTimestamp = BinaryPrimitives.ReadInt64BigEndian(recordBatch.Slice(35));
        var recordCount = BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(57));
        return (baseOffset, recordCount, maxTimestamp);
    }
}
