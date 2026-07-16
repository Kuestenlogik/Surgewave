using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage;

namespace Kuestenlogik.Surgewave.Storage.Engine.FileSystem;

/// <summary>
/// File-based storage engine implementing ISurgewaveStorageEngine.
/// Provides sequential writes with indexed reads for Kafka RecordBatch data.
/// </summary>
public sealed class FileStorageEngine : ISurgewaveStorageEngine
{
    private readonly string _baseDirectory;
    private readonly long _baseOffset;
    private readonly long _maxSize;
    private readonly ISurgewaveBufferPool _bufferPool;

    private readonly FileStream _logFile;
    private readonly FileStream _indexFile;
    private readonly FileStream _timeIndexFile;

    // Memory-mapped file for zero-copy reads
    private readonly FileMmapManager? _mmapManager;

    // Thread-safe offset index (ConcurrentDictionary for lock-free reads)
    private readonly ConcurrentDictionary<long, long> _offsetIndex = new();
    private readonly List<long> _offsetsInOrder = new();

    // Thread-safe timestamp index
    private readonly ConcurrentDictionary<long, long> _timestampIndex = new();
    private readonly List<long> _timestampsInOrder = new();

    // Lock for ordered list access (lists don't have concurrent equivalents with binary search)
    private readonly ReaderWriterLockSlim _orderedIndexLock = new();

    // Background index write
    private readonly object _indexWriteLock = new();
    private readonly List<(long offset, long position, long timestamp)> _pendingIndexEntries = new();
    private Task? _pendingIndexWrite;
    private int _pendingIndexCount;
    private const int IndexFlushInterval = 100;
    private const int IndexBatchSize = 32;

    private long _writePosition;
    private long _currentOffset;
    private bool _disposed;

    private const int BufferSize = 64 * 1024;
    private const FileOptions AsyncWriteOptions = FileOptions.Asynchronous | FileOptions.SequentialScan;

    public long BaseOffset => _baseOffset;
    public long CurrentOffset => Volatile.Read(ref _currentOffset);
    public long Size => Volatile.Read(ref _writePosition);
    public bool IsFull => Size >= _maxSize;
    public DateTime CreatedAt { get; }
    public long MaxTimestamp { get; private set; }

    public long? FirstOffset
    {
        get
        {
            _orderedIndexLock.EnterReadLock();
            try
            {
                return _offsetsInOrder.Count > 0 ? _offsetsInOrder[0] : null;
            }
            finally
            {
                _orderedIndexLock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// Path to the log file for this segment.
    /// </summary>
    public string LogFilePath { get; }

    public FileStorageEngine(
        string baseDirectory,
        long baseOffset,
        bool createNew,
        long maxSize = 1024L * 1024 * 1024,
        ISurgewaveBufferPool? bufferPool = null,
        bool useMmap = true)
    {
        _baseDirectory = baseDirectory;
        _baseOffset = baseOffset;
        _currentOffset = baseOffset;
        _maxSize = maxSize;
        _bufferPool = bufferPool ?? DefaultSurgewaveBufferPool.Shared;

        Directory.CreateDirectory(baseDirectory);

        LogFilePath = Path.Combine(baseDirectory, $"{baseOffset:D20}.log");
        var indexPath = Path.Combine(baseDirectory, $"{baseOffset:D20}.index");
        var timeIndexPath = Path.Combine(baseDirectory, $"{baseOffset:D20}.timeindex");

        if (createNew)
        {
            _logFile = new FileStream(LogFilePath, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, BufferSize, AsyncWriteOptions);
            _indexFile = new FileStream(indexPath, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.Asynchronous);
            _timeIndexFile = new FileStream(timeIndexPath, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.Asynchronous);
            CreatedAt = DateTime.UtcNow;
            _writePosition = 0;
        }
        else
        {
            _logFile = new FileStream(LogFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, BufferSize, AsyncWriteOptions);
            _indexFile = new FileStream(indexPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.Asynchronous);
            _timeIndexFile = new FileStream(timeIndexPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.Asynchronous);
            CreatedAt = File.GetCreationTimeUtc(LogFilePath);
            _writePosition = _logFile.Length;
            LoadIndex();
            LoadTimeIndex();
        }

        // Initialize mmap manager for zero-copy reads (after file is created/opened)
        if (useMmap && !createNew && _logFile.Length > 0)
        {
            _mmapManager = new FileMmapManager(LogFilePath);
        }
    }

    public ValueTask<(long baseOffset, int recordCount)> AppendAsync(
        ReadOnlySpan<byte> recordBatch,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var (batchBaseOffset, recordCount, maxTimestamp) = ParseBatchHeader(recordBatch);
        var filePosition = Volatile.Read(ref _writePosition);

        // Write synchronously using RandomAccess (span-friendly)
        RandomAccess.Write(_logFile.SafeFileHandle, recordBatch, filePosition);
        Volatile.Write(ref _writePosition, filePosition + recordBatch.Length);

        UpdateIndexes(batchBaseOffset, filePosition, maxTimestamp, recordCount);
        QueueIndexWrite(batchBaseOffset, filePosition, maxTimestamp);

        return ValueTask.FromResult((batchBaseOffset, recordCount));
    }

    public async ValueTask<(long baseOffset, int recordCount)> AppendAsync(
        ISurgewaveBuffer recordBatch,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var length = recordBatch.Length;
        var (batchBaseOffset, recordCount, maxTimestamp) = ParseBatchHeader(recordBatch.Span);
        var filePosition = Volatile.Read(ref _writePosition);

        if (recordBatch.TryGetMemory(out var memory))
        {
            await RandomAccess.WriteAsync(_logFile.SafeFileHandle, memory, filePosition, cancellationToken);
        }
        else
        {
            var temp = recordBatch.ToArray();
            await RandomAccess.WriteAsync(_logFile.SafeFileHandle, temp, filePosition, cancellationToken);
        }
        Volatile.Write(ref _writePosition, filePosition + length);

        UpdateIndexes(batchBaseOffset, filePosition, maxTimestamp, recordCount);
        QueueIndexWrite(batchBaseOffset, filePosition, maxTimestamp);

        return (batchBaseOffset, recordCount);
    }

    public async ValueTask<IStorageReadLease> ReadAsync(
        long startOffset,
        int maxBytes,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Use Volatile.Read for thread-safe access to _currentOffset
        if (startOffset < _baseOffset || startOffset >= Volatile.Read(ref _currentOffset))
        {
            return EmptyStorageReadLease.Instance;
        }

        var batchOffset = FindBatchOffsetForRead(startOffset);
        if (batchOffset == null || !_offsetIndex.TryGetValue(batchOffset.Value, out var filePosition))
        {
            return EmptyStorageReadLease.Instance;
        }

        var availableBytes = Volatile.Read(ref _writePosition) - filePosition;
        if (availableBytes < 12)
        {
            return EmptyStorageReadLease.Instance;
        }

        // Try zero-copy mmap read first
        if (_mmapManager != null && filePosition + maxBytes <= _logFile.Length)
        {
            return await ReadWithMmapAsync(filePosition, maxBytes, cancellationToken);
        }

        // Fallback to pooled buffer read
        return await ReadWithPooledBufferAsync(filePosition, (int)Math.Min(maxBytes, availableBytes), cancellationToken);
    }

    private async ValueTask<IStorageReadLease> ReadWithMmapAsync(long filePosition, int maxBytes, CancellationToken cancellationToken)
    {
        // First pass: scan to find valid batch boundaries
        var bytesToRead = (int)Math.Min(maxBytes, _logFile.Length - filePosition);

        // Get mmap buffer for reading
        FileMmapBuffer? mmapBuffer = null;
        ISurgewaveBuffer? finalBuffer = null;
        try
        {
            mmapBuffer = _mmapManager!.GetBuffer(filePosition, bytesToRead);
            var span = mmapBuffer.Span;

            var batchOffsets = new List<int>();
            var position = 0;
            var validBytes = 0;

            while (position + 12 <= span.Length)
            {
                var batchLength = BinaryPrimitives.ReadInt32BigEndian(span.Slice(position + 8, 4));
                var totalBatchSize = 12 + batchLength;

                if (position + totalBatchSize > span.Length)
                    break;

                batchOffsets.Add(position);
                validBytes = position + totalBatchSize;
                position += totalBatchSize;
            }

            if (validBytes == 0)
            {
                return EmptyStorageReadLease.Instance;
            }

            // Slice to valid bytes only - finalBuffer takes ownership
            finalBuffer = validBytes < bytesToRead
                ? mmapBuffer.Slice(0, validBytes)
                : mmapBuffer;

            // If we sliced, the original buffer is no longer needed
            if (finalBuffer != mmapBuffer)
            {
                mmapBuffer.Dispose();
            }

            // Transfer ownership to the lease
            mmapBuffer = null; // Prevent dispose in finally
            var result = new StorageReadLease(finalBuffer, batchOffsets);
            finalBuffer = null; // Ownership transferred
            return result;
        }
        finally
        {
            // Clean up only if ownership wasn't transferred
            finalBuffer?.Dispose();
            mmapBuffer?.Dispose();
        }
    }

    private async ValueTask<IStorageReadLease> ReadWithPooledBufferAsync(long filePosition, int maxBytes, CancellationToken cancellationToken)
    {
        var buffer = _bufferPool.Rent(maxBytes);
        var writableBuffer = (ISurgewaveWritableBuffer)buffer;

        var bytesRead = await RandomAccess.ReadAsync(
            _logFile.SafeFileHandle,
            writableBuffer.Memory,
            filePosition,
            cancellationToken);

        if (bytesRead < 12)
        {
            buffer.Dispose();
            return EmptyStorageReadLease.Instance;
        }

        var span = buffer.Span.Slice(0, bytesRead);
        var batchOffsets = new List<int>();
        var position = 0;
        var validBytes = 0;

        while (position + 12 <= bytesRead)
        {
            var batchLength = BinaryPrimitives.ReadInt32BigEndian(span.Slice(position + 8, 4));
            var totalBatchSize = 12 + batchLength;

            if (position + totalBatchSize > bytesRead)
                break;

            batchOffsets.Add(position);
            validBytes = position + totalBatchSize;
            position += totalBatchSize;
        }

        if (validBytes == 0)
        {
            buffer.Dispose();
            return EmptyStorageReadLease.Instance;
        }

        // Trim to the valid prefix, TRANSFERRING pool ownership to the trimmed view — a plain
        // Slice() is non-owning and the parent rent (maxBytes, typically LOH-sized) would leak
        // because the lease only disposes the buffer it is handed (#75). Non-default pool
        // implementations keep their own parent-lifetime semantics via the non-owning fallback.
        var finalBuffer = validBytes < bytesRead
            ? buffer switch
            {
                PooledSurgewaveBuffer pooled => pooled.SliceTransferringOwnership(0, validBytes),
                _ => buffer.Slice(0, validBytes),
            }
            : buffer;

        return new StorageReadLease(finalBuffer, batchOffsets);
    }

    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        (long offset, long position, long timestamp)[]? remainingEntries = null;
        lock (_indexWriteLock)
        {
            if (_pendingIndexEntries.Count > 0)
            {
                remainingEntries = [.. _pendingIndexEntries];
                _pendingIndexEntries.Clear();
            }
        }

        if (remainingEntries is { Length: > 0 })
        {
            await WriteBatchedIndexEntriesAsync(remainingEntries, flush: false);
        }

        Task? pendingWrite;
        lock (_indexWriteLock)
        {
            pendingWrite = _pendingIndexWrite;
        }
        if (pendingWrite != null)
        {
            await pendingWrite;
        }

        // Real durability flush (#76): batch writes go through RandomAccess on the SafeFileHandle
        // and bypass the FileStream buffer, so FlushAsync() flushed an empty user-space buffer and
        // never issued an fsync — the "flush" was page-cache-only. Flush(flushToDisk: true) drains
        // the FileStream buffer (the index files DO write through it) AND calls
        // FlushFileBuffers/fsync on the handle, which covers the RandomAccess-written log bytes.
        _logFile.Flush(flushToDisk: true);
        _indexFile.Flush(flushToDisk: true);
        _timeIndexFile.Flush(flushToDisk: true);
        _pendingIndexCount = 0;
    }

    public long? FindOffsetByTimestamp(long targetTimestamp)
    {
        _orderedIndexLock.EnterReadLock();
        try
        {
            if (_timestampsInOrder.Count == 0)
                return null;

            int left = 0, right = _timestampsInOrder.Count - 1, result = -1;

            while (left <= right)
            {
                int mid = left + (right - left) / 2;
                if (_timestampsInOrder[mid] >= targetTimestamp)
                {
                    result = mid;
                    right = mid - 1;
                }
                else
                {
                    left = mid + 1;
                }
            }

            if (result >= 0 && _timestampIndex.TryGetValue(_timestampsInOrder[result], out var batchOffset))
            {
                return batchOffset;
            }
            return null;
        }
        finally
        {
            _orderedIndexLock.ExitReadLock();
        }
    }

    public void DeleteStorage()
    {
        if (!_disposed)
            throw new InvalidOperationException("Must be disposed before deleting storage");

        var indexPath = Path.Combine(_baseDirectory, $"{_baseOffset:D20}.index");
        var timeIndexPath = Path.Combine(_baseDirectory, $"{_baseOffset:D20}.timeindex");

        try { File.Delete(LogFilePath); } catch { }
        try { File.Delete(indexPath); } catch { }
        try { File.Delete(timeIndexPath); } catch { }
    }

    private void UpdateIndexes(long batchBaseOffset, long filePosition, long maxTimestamp, int recordCount)
    {
        // ConcurrentDictionary is thread-safe for writes
        _offsetIndex[batchBaseOffset] = filePosition;

        // Lists require explicit locking
        _orderedIndexLock.EnterWriteLock();
        try
        {
            _offsetsInOrder.Add(batchBaseOffset);

            if (maxTimestamp > 0)
            {
                _timestampIndex[maxTimestamp] = batchBaseOffset;
                _timestampsInOrder.Add(maxTimestamp);
                if (maxTimestamp > MaxTimestamp)
                {
                    MaxTimestamp = maxTimestamp;
                }
            }

            // Update current offset inside lock to ensure memory visibility
            Volatile.Write(ref _currentOffset, batchBaseOffset + recordCount);
        }
        finally
        {
            _orderedIndexLock.ExitWriteLock();
        }
    }

    private void QueueIndexWrite(long batchBaseOffset, long filePosition, long maxTimestamp)
    {
        lock (_indexWriteLock)
        {
            _pendingIndexEntries.Add((batchBaseOffset, filePosition, maxTimestamp));
            _pendingIndexCount++;

            var shouldFlush = _pendingIndexCount >= IndexFlushInterval;
            var shouldWrite = shouldFlush || _pendingIndexEntries.Count >= IndexBatchSize;

            if (shouldWrite)
            {
                var entriesToWrite = _pendingIndexEntries.ToArray();
                _pendingIndexEntries.Clear();

                var previous = _pendingIndexWrite ?? Task.CompletedTask;
                _pendingIndexWrite = previous.ContinueWith(
                    async _ => await WriteBatchedIndexEntriesAsync(entriesToWrite, shouldFlush),
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default).Unwrap();
            }

            if (shouldFlush)
            {
                _pendingIndexCount = 0;
            }
        }
    }

    private async Task WriteBatchedIndexEntriesAsync((long offset, long position, long timestamp)[] entries, bool flush)
    {
        if (entries.Length == 0) return;

        var offsetIndexBufferSize = entries.Length * 16;
        var offsetIndexBuffer = ArrayPool<byte>.Shared.Rent(offsetIndexBufferSize);
        try
        {
            var timeIndexCount = 0;

            for (int i = 0; i < entries.Length; i++)
            {
                var (offset, position, timestamp) = entries[i];
                BinaryPrimitives.WriteInt64LittleEndian(offsetIndexBuffer.AsSpan(i * 16), offset);
                BinaryPrimitives.WriteInt64LittleEndian(offsetIndexBuffer.AsSpan(i * 16 + 8), position);

                if (timestamp > 0)
                {
                    timeIndexCount++;
                }
            }

            await _indexFile.WriteAsync(offsetIndexBuffer.AsMemory(0, offsetIndexBufferSize));

            if (timeIndexCount > 0)
            {
                var timeIndexBufferSize = timeIndexCount * 16;
                var timeIndexBuffer = ArrayPool<byte>.Shared.Rent(timeIndexBufferSize);
                try
                {
                    var timeIdx = 0;
                    for (int i = 0; i < entries.Length; i++)
                    {
                        var (offset, _, timestamp) = entries[i];
                        if (timestamp > 0)
                        {
                            BinaryPrimitives.WriteInt64LittleEndian(timeIndexBuffer.AsSpan(timeIdx * 16), timestamp);
                            BinaryPrimitives.WriteInt64LittleEndian(timeIndexBuffer.AsSpan(timeIdx * 16 + 8), offset);
                            timeIdx++;
                        }
                    }
                    await _timeIndexFile.WriteAsync(timeIndexBuffer.AsMemory(0, timeIndexBufferSize));
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(timeIndexBuffer);
                }
            }

            if (flush)
            {
                // Real disk flush — see FlushAsync for why FlushAsync() alone was a no-op (#76).
                _logFile.Flush(flushToDisk: true);
                _indexFile.Flush(flushToDisk: true);
                _timeIndexFile.Flush(flushToDisk: true);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(offsetIndexBuffer);
        }
    }

    private long? FindBatchOffsetForRead(long requestedOffset)
    {
        _orderedIndexLock.EnterReadLock();
        try
        {
            if (_offsetsInOrder.Count == 0)
                return null;

            int left = 0, right = _offsetsInOrder.Count - 1, result = -1;

            while (left <= right)
            {
                int mid = left + (right - left) / 2;
                if (_offsetsInOrder[mid] <= requestedOffset)
                {
                    result = mid;
                    left = mid + 1;
                }
                else
                {
                    right = mid - 1;
                }
            }

            return result >= 0 ? _offsetsInOrder[result] : null;
        }
        finally
        {
            _orderedIndexLock.ExitReadLock();
        }
    }

    private void LoadIndex()
    {
        if (_indexFile.Length == 0)
        {
            if (_logFile.Length > 0)
            {
                RebuildIndexFromLog();
            }
            return;
        }

        // Batch read entire index file (16 bytes per entry)
        var indexLength = (int)_indexFile.Length;
        var entryCount = indexLength / 16;
        if (entryCount == 0) return;

        var buffer = ArrayPool<byte>.Shared.Rent(indexLength);
        try
        {
            _indexFile.Seek(0, SeekOrigin.Begin);
            var bytesRead = _indexFile.Read(buffer, 0, indexLength);

            // Pre-allocate lists
            _offsetsInOrder.Capacity = Math.Max(_offsetsInOrder.Capacity, entryCount);

            for (int i = 0; i < bytesRead / 16; i++)
            {
                var span = buffer.AsSpan(i * 16, 16);
                var batchBaseOffset = BinaryPrimitives.ReadInt64LittleEndian(span);
                var filePosition = BinaryPrimitives.ReadInt64LittleEndian(span.Slice(8));
                _offsetIndex[batchBaseOffset] = filePosition;
                _offsetsInOrder.Add(batchBaseOffset);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        if (_offsetsInOrder.Count > 0 && _logFile.Length > 0)
        {
            var lastBatchOffset = _offsetsInOrder[^1];
            var lastBatchPosition = _offsetIndex[lastBatchOffset];

            // Read record count from last batch header (only need bytes 57-60)
            Span<byte> headerBuffer = stackalloc byte[61];
            _logFile.Seek(lastBatchPosition, SeekOrigin.Begin);
            _logFile.ReadExactly(headerBuffer);

            var recordCount = BinaryPrimitives.ReadInt32BigEndian(headerBuffer.Slice(57, 4));
            _currentOffset = lastBatchOffset + recordCount;
            _logFile.Seek(0, SeekOrigin.End);
        }
    }

    private void RebuildIndexFromLog()
    {
        _logFile.Seek(0, SeekOrigin.Begin);
        Span<byte> headerBuffer = stackalloc byte[12]; // baseOffset(8) + batchLength(4)

        while (_logFile.Position < _logFile.Length)
        {
            var batchStartPos = _logFile.Position;

            // Try to read header
            var bytesRead = _logFile.Read(headerBuffer);
            if (bytesRead < 12)
                break;

            var baseOffset = BinaryPrimitives.ReadInt64BigEndian(headerBuffer);
            var batchLength = BinaryPrimitives.ReadInt32BigEndian(headerBuffer.Slice(8));

            _offsetIndex[baseOffset] = batchStartPos;
            _offsetsInOrder.Add(baseOffset);

            // Seek past batch data
            _logFile.Seek(batchStartPos + 12 + batchLength, SeekOrigin.Begin);
        }

        _logFile.Seek(0, SeekOrigin.End);
    }

    private void LoadTimeIndex()
    {
        if (_timeIndexFile.Length == 0)
            return;

        // Batch read entire time index file (16 bytes per entry)
        var indexLength = (int)_timeIndexFile.Length;
        var entryCount = indexLength / 16;
        if (entryCount == 0) return;

        var buffer = ArrayPool<byte>.Shared.Rent(indexLength);
        try
        {
            _timeIndexFile.Seek(0, SeekOrigin.Begin);
            var bytesRead = _timeIndexFile.Read(buffer, 0, indexLength);

            // Pre-allocate list
            _timestampsInOrder.Capacity = Math.Max(_timestampsInOrder.Capacity, entryCount);

            for (int i = 0; i < bytesRead / 16; i++)
            {
                var span = buffer.AsSpan(i * 16, 16);
                var timestamp = BinaryPrimitives.ReadInt64LittleEndian(span);
                var batchBaseOffset = BinaryPrimitives.ReadInt64LittleEndian(span.Slice(8));
                _timestampIndex[timestamp] = batchBaseOffset;
                _timestampsInOrder.Add(timestamp);

                if (timestamp > MaxTimestamp)
                    MaxTimestamp = timestamp;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static (long baseOffset, int recordCount, long maxTimestamp) ParseBatchHeader(ReadOnlySpan<byte> recordBatch)
    {
        var baseOffset = BinaryPrimitives.ReadInt64BigEndian(recordBatch);
        var maxTimestamp = BinaryPrimitives.ReadInt64BigEndian(recordBatch.Slice(35));
        var recordCount = BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(57));
        return (baseOffset, recordCount, maxTimestamp);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        lock (_indexWriteLock)
        {
            if (_pendingIndexEntries.Count > 0)
            {
                try
                {
                    WriteBatchedIndexEntriesAsync([.. _pendingIndexEntries], flush: false).Wait(TimeSpan.FromSeconds(5));
                }
                catch { }
                _pendingIndexEntries.Clear();
            }
        }

        try { _pendingIndexWrite?.Wait(TimeSpan.FromSeconds(5)); } catch { }
        // Clean shutdown flushes to DISK (#76): closing the handles only hands the pages to the OS;
        // Flush() without flushToDisk never fsyncs, so a post-shutdown power loss could drop
        // acknowledged data.
        try { _logFile?.Flush(flushToDisk: true); _indexFile?.Flush(flushToDisk: true); _timeIndexFile?.Flush(flushToDisk: true); } catch { }

        _mmapManager?.Dispose();
        _logFile?.Dispose();
        _indexFile?.Dispose();
        _timeIndexFile?.Dispose();
        _orderedIndexLock?.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// Factory methods for creating file-backed log segment factories.
/// </summary>
public static class FileLogSegmentFactory
{
    /// <summary>
    /// Create a zero-copy file storage factory with optional mmap support.
    /// </summary>
    public static ILogSegmentFactory Create(bool useMmap = true)
    {
        var engineFactory = new FileStorageEngineFactory(useMmap: useMmap);
        return new StorageEngineSegmentFactory(engineFactory, isPersistent: true);
    }
}

/// <summary>
/// Factory for creating file-based storage engines.
/// </summary>
public sealed class FileStorageEngineFactory : ISurgewaveStorageEngineFactory
{
    private readonly ISurgewaveBufferPool _bufferPool;
    private readonly long _defaultMaxSize;
    private readonly bool _useMmap;

    public FileStorageEngineFactory(
        ISurgewaveBufferPool? bufferPool = null,
        long defaultMaxSize = 1024L * 1024 * 1024,
        bool useMmap = true)
    {
        _bufferPool = bufferPool ?? DefaultSurgewaveBufferPool.Shared;
        _defaultMaxSize = defaultMaxSize;
        _useMmap = useMmap;
    }

    public ISurgewaveStorageEngine Create(string directory, long baseOffset, long maxSize)
    {
        return new FileStorageEngine(directory, baseOffset, createNew: true, maxSize, _bufferPool, _useMmap);
    }

    public ISurgewaveStorageEngine Open(string directory, long baseOffset)
    {
        return new FileStorageEngine(directory, baseOffset, createNew: false, _defaultMaxSize, _bufferPool, _useMmap);
    }
}
