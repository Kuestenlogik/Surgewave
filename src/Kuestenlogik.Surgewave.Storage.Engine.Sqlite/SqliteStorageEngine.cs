using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Storage.Engine;
using Microsoft.Data.Sqlite;

namespace Kuestenlogik.Surgewave.Storage.Engine.Sqlite;

/// <summary>
/// SQLite-based storage engine for simple, portable storage.
/// Ideal for development, testing, edge devices, and IoT scenarios.
/// Single-file database with ACID guarantees.
/// </summary>
public sealed class SqliteStorageEngine : ISurgewaveStorageEngine
{
    private readonly SqliteConnection _connection;
    private readonly string _dbPath;
    private readonly long _baseOffset;
    private readonly long _maxSize;
    private readonly ISurgewaveBufferPool _bufferPool;

    private long _currentOffset;
    private long _size;
    private long _maxTimestamp;
    private long? _firstOffset;
    private bool _disposed;

    private readonly object _writeLock = new();

    // Prepared statements for performance
    private SqliteCommand? _insertBatchCmd;
    private SqliteCommand? _insertTimestampIdxCmd;
    private SqliteCommand? _selectBatchCmd;
    private SqliteCommand? _selectBatchRangeCmd;

    public long BaseOffset => _baseOffset;
    public long CurrentOffset => Volatile.Read(ref _currentOffset);
    public long Size => Volatile.Read(ref _size);
    public bool IsFull => Size >= _maxSize;
    public DateTime CreatedAt { get; }
    public long MaxTimestamp => Volatile.Read(ref _maxTimestamp);
    public long? FirstOffset => _firstOffset;

    public SqliteStorageEngine(
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

        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared,
            Pooling = true
        }.ToString();

        _connection = new SqliteConnection(connectionString);
        _connection.Open();

        // Enable WAL mode for better concurrent read/write performance
        using (var cmd = _connection.CreateCommand())
        {
            cmd.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=10000;";
            cmd.ExecuteNonQuery();
        }

        if (createNew)
        {
            CreateSchema();
            SaveMetadata();
        }
        else
        {
            LoadMetadata();
        }

        PrepareStatements();
    }

    private void CreateSchema()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS batches (
                offset INTEGER PRIMARY KEY,
                data BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS timestamp_index (
                timestamp INTEGER PRIMARY KEY,
                offset INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ts ON timestamp_index(timestamp);
            """;
        cmd.ExecuteNonQuery();
    }

    private void PrepareStatements()
    {
        _insertBatchCmd = _connection.CreateCommand();
        _insertBatchCmd.CommandText = "INSERT OR REPLACE INTO batches (offset, data) VALUES ($offset, $data)";
        _insertBatchCmd.Parameters.Add("$offset", SqliteType.Integer);
        _insertBatchCmd.Parameters.Add("$data", SqliteType.Blob);

        _insertTimestampIdxCmd = _connection.CreateCommand();
        _insertTimestampIdxCmd.CommandText = "INSERT OR REPLACE INTO timestamp_index (timestamp, offset) VALUES ($ts, $offset)";
        _insertTimestampIdxCmd.Parameters.Add("$ts", SqliteType.Integer);
        _insertTimestampIdxCmd.Parameters.Add("$offset", SqliteType.Integer);

        _selectBatchCmd = _connection.CreateCommand();
        _selectBatchCmd.CommandText = "SELECT data FROM batches WHERE offset = $offset";
        _selectBatchCmd.Parameters.Add("$offset", SqliteType.Integer);

        _selectBatchRangeCmd = _connection.CreateCommand();
        _selectBatchRangeCmd.CommandText = "SELECT offset, data FROM batches WHERE offset >= $startOffset ORDER BY offset";
        _selectBatchRangeCmd.Parameters.Add("$startOffset", SqliteType.Integer);
    }

    public ValueTask<(long baseOffset, int recordCount)> AppendAsync(
        ReadOnlySpan<byte> recordBatch,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var (batchBaseOffset, recordCount, maxTimestamp) = ParseBatchHeader(recordBatch);

        lock (_writeLock)
        {
            // Insert batch data
            _insertBatchCmd!.Parameters["$offset"].Value = batchBaseOffset;
            _insertBatchCmd.Parameters["$data"].Value = recordBatch.ToArray();
            _insertBatchCmd.ExecuteNonQuery();

            // Update timestamp index
            if (maxTimestamp > 0)
            {
                _insertTimestampIdxCmd!.Parameters["$ts"].Value = maxTimestamp;
                _insertTimestampIdxCmd.Parameters["$offset"].Value = batchBaseOffset;
                _insertTimestampIdxCmd.ExecuteNonQuery();

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

        _selectBatchRangeCmd!.Parameters["$startOffset"].Value = batchOffset.Value;

        using (var reader = _selectBatchRangeCmd.ExecuteReader())
        {
            while (reader.Read())
            {
                var data = (byte[])reader["data"];
                var batchSize = data.Length;

                if (totalBytes > 0 && totalBytes + batchSize > maxBytes)
                    break;

                batchOffsets.Add(totalBytes);
                batches.Add(data);
                totalBytes += batchSize;
            }
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

            // Force WAL checkpoint
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = "PRAGMA wal_checkpoint(TRUNCATE)";
            cmd.ExecuteNonQuery();
        }

        return ValueTask.CompletedTask;
    }

    public long? FindOffsetByTimestamp(long targetTimestamp)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT offset FROM timestamp_index WHERE timestamp >= $ts ORDER BY timestamp LIMIT 1";
        cmd.Parameters.AddWithValue("$ts", targetTimestamp);

        var result = cmd.ExecuteScalar();
        return result != null ? (long)result : null;
    }

    public void DeleteStorage()
    {
        if (File.Exists(_dbPath))
        {
            File.Delete(_dbPath);
        }
        // Also delete WAL and SHM files
        var walPath = _dbPath + "-wal";
        var shmPath = _dbPath + "-shm";
        if (File.Exists(walPath)) File.Delete(walPath);
        if (File.Exists(shmPath)) File.Delete(shmPath);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        SaveMetadata();

        _insertBatchCmd?.Dispose();
        _insertTimestampIdxCmd?.Dispose();
        _selectBatchCmd?.Dispose();
        _selectBatchRangeCmd?.Dispose();

        _connection.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    private long? FindBatchOffsetForRead(long requestedOffset)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT MAX(offset) FROM batches WHERE offset <= $offset";
        cmd.Parameters.AddWithValue("$offset", requestedOffset);

        var result = cmd.ExecuteScalar();
        return result != DBNull.Value && result != null ? (long)result : null;
    }

    private void LoadMetadata()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT key, value FROM metadata";

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var key = reader.GetString(0);
            var value = reader.GetInt64(1);

            switch (key)
            {
                case "currentOffset":
                    _currentOffset = value;
                    break;
                case "size":
                    _size = value;
                    break;
                case "maxTimestamp":
                    _maxTimestamp = value;
                    break;
                case "firstOffset":
                    _firstOffset = value;
                    break;
            }
        }
    }

    private void SaveMetadata()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            INSERT OR REPLACE INTO metadata (key, value) VALUES
            ('currentOffset', $currentOffset),
            ('size', $size),
            ('maxTimestamp', $maxTimestamp),
            ('firstOffset', $firstOffset)
            """;
        cmd.Parameters.AddWithValue("$currentOffset", _currentOffset);
        cmd.Parameters.AddWithValue("$size", _size);
        cmd.Parameters.AddWithValue("$maxTimestamp", _maxTimestamp);
        cmd.Parameters.AddWithValue("$firstOffset", _firstOffset ?? (object)DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    private static (long baseOffset, int recordCount, long maxTimestamp) ParseBatchHeader(ReadOnlySpan<byte> recordBatch)
    {
        var baseOffset = BinaryPrimitives.ReadInt64BigEndian(recordBatch);
        var maxTimestamp = BinaryPrimitives.ReadInt64BigEndian(recordBatch.Slice(35));
        var recordCount = BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(57));
        return (baseOffset, recordCount, maxTimestamp);
    }
}
