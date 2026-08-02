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

    // Memory-mapped file for zero-copy reads. Created lazily on the first read that can use it,
    // not in the constructor: a freshly created segment has an empty file, and mapping it would
    // throw. Reads of a segment written in this process used to miss the mmap path entirely
    // because of that (#78).
    private FileMmapManager? _mmapManager;
    private readonly Lock _mmapInitLock = new();
    private readonly bool _useMmap;

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

        // Initialize mmap manager for zero-copy reads (after file is created/opened).
        // A reopened, non-empty file can be mapped right away; a new one is mapped lazily once
        // it actually holds data — see GetOrCreateMmapManager.
        _useMmap = useMmap;
        if (useMmap && !createNew && _logFile.Length > 0)
        {
            _mmapManager = new FileMmapManager(LogFilePath);
        }
    }

    /// <summary>
    /// Returns the mmap manager, creating it on first use once the file holds data (#78).
    /// <para>
    /// Mapping a segment that is still being appended to is safe here because the log is
    /// append-only: bytes below the current write position never change again, and callers only
    /// map regions below it. Appends go through <see cref="RandomAccess"/> straight into the page
    /// cache — the same pages the mapping exposes — so a mapped read cannot miss a completed
    /// write. Growth is handled by the manager, which re-maps on demand and hands out a fresh
    /// view per buffer.
    /// </para>
    /// </summary>
    private FileMmapManager? GetOrCreateMmapManager()
    {
        if (!_useMmap)
        {
            return null;
        }

        var existing = Volatile.Read(ref _mmapManager);
        if (existing != null)
        {
            return existing;
        }

        lock (_mmapInitLock)
        {
            if (_mmapManager != null)
            {
                return _mmapManager;
            }

            // Disposal takes this same lock, so checking here orders the two: a reader that gets in
            // first creates a manager that Dispose then disposes; one that arrives afterwards sees
            // the flag and falls back to the pooled read. Without it, a read that raced disposal
            // could map the file after Dispose had already released its manager — leaving a mapping
            // nobody owns, which on Windows blocks the segment file from ever being deleted.
            if (_disposed)
            {
                return null;
            }

            // Nothing written yet: mapping an empty file throws, so stay on the pooled path
            // until there is data. The next read retries.
            if (Volatile.Read(ref _writePosition) <= 0)
            {
                return null;
            }

            var created = new FileMmapManager(LogFilePath);
            Volatile.Write(ref _mmapManager, created);
            return created;
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

        var bytesToRead = (int)Math.Min(maxBytes, availableBytes);

        // Try zero-copy mmap read first. The region must lie fully below the write position:
        // the log is append-only, so those bytes are final, whereas anything at or beyond it may
        // still be in flight.
#pragma warning disable CA2000 // The manager is cached in _mmapManager and disposed in Dispose()
        if (GetOrCreateMmapManager() is { } mmapManager
            && filePosition + bytesToRead <= Volatile.Read(ref _writePosition))
        {
            return await ReadWithMmapAsync(mmapManager, filePosition, bytesToRead, cancellationToken);
        }
#pragma warning restore CA2000

        // Fallback to pooled buffer read
        return await ReadWithPooledBufferAsync(filePosition, bytesToRead, cancellationToken);
    }

    private async ValueTask<IStorageReadLease> ReadWithMmapAsync(
        FileMmapManager mmapManager, long filePosition, int maxBytes, CancellationToken cancellationToken)
    {
        // The caller already bounded this to the written region; re-clamping against
        // _logFile.Length would be wrong while the segment is still being appended to, because
        // the stream's cached length can lag the write position.
        var bytesToRead = maxBytes;

        // Get mmap buffer for reading
        FileMmapBuffer? mmapBuffer = null;
        ISurgewaveBuffer? finalBuffer = null;
        try
        {
            mmapBuffer = mmapManager.GetBuffer(filePosition, bytesToRead);
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

    /// <summary>
    /// Deletes the segment's files.
    ///
    /// <para><b>Failure is reported, not swallowed.</b> A memory-mapped view held by an in-flight
    /// read pins its file on Windows: <c>File.Delete</c> then fails with
    /// <c>ERROR_USER_MAPPED_FILE</c>. Swallowing that leaves a log file on disk that the partition
    /// has already dropped from its segment list — and <c>LoadExistingSegments</c> picks it up
    /// again on the next start, serving records that retention had deleted. The caller retries
    /// instead (#78 follow-up).</para>
    /// </summary>
    /// <exception cref="IOException">The log file could not be deleted; retry once readers are done.</exception>
    public void DeleteStorage()
    {
        if (!_disposed)
            throw new InvalidOperationException("Must be disposed before deleting storage");

        var indexPath = Path.Combine(_baseDirectory, $"{_baseOffset:D20}.index");
        var timeIndexPath = Path.Combine(_baseDirectory, $"{_baseOffset:D20}.timeindex");

        // The indexes are derived data — a leftover is harmless because the log file's absence
        // already removes the records, and a stale index without its log is ignored on load.
        try { File.Delete(indexPath); } catch { }
        try { File.Delete(timeIndexPath); } catch { }

        // The log file is the record. Let the exception out so the partition can keep the segment
        // on its retry list rather than believing the data is gone.
        File.Delete(LogFilePath);
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

        // Under the same lock GetOrCreateMmapManager uses, so a read racing disposal cannot install
        // a manager after this line has run.
        lock (_mmapInitLock)
        {
            _mmapManager?.Dispose();
            _mmapManager = null;
        }

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
