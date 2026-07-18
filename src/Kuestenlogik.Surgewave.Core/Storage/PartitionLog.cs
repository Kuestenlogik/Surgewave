using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Kuestenlogik.Surgewave.Core.Exceptions;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Manages the log for a single partition across multiple segments.
/// Uses lock-free atomic offset claim for high-throughput concurrent writes.
/// </summary>
public sealed class PartitionLog : IPartitionLog
{
    private readonly string _baseDirectory;
    private readonly TopicPartition _topicPartition;
    private readonly long _maxSegmentBytes;
    private readonly ILogSegmentFactory _segmentFactory;
    private readonly List<ILogSegment> _segments = new();
    private readonly PartitionLogReader _reader = new();
#pragma warning disable CA2213 // _activeSegment is always a member of _segments and gets disposed with the collection
    private ILogSegment? _activeSegment;
#pragma warning restore CA2213
    private long _nextOffset;
    private long _highWatermark;
    private long _dirtyBytes;  // Use Interlocked for thread-safe updates
    private readonly ReaderWriterLockSlim _appendLock = new();  // Read=append, Write=segment roll
    private readonly SemaphoreSlim _segmentWriteLock = new(1, 1);  // Serialize actual segment writes
    private readonly Lock _segmentLock = new();  // For read-only segment lookup
    private readonly Lock _waiterLock = new();  // For data waiters
    private readonly List<DataWaiter> _dataWaiters = [];  // Long-polling waiters
    private bool _disposed;

    // Data integrity settings
    private bool _validateCrcOnRead;
    private CorruptionRecoveryMode _corruptionRecoveryMode = CorruptionRecoveryMode.SkipAndContinue;
    private ICorruptionHandler? _corruptionHandler;

    public TopicPartition TopicPartition => _topicPartition;
    public long NextOffset => Volatile.Read(ref _nextOffset);
    public long HighWatermark => Volatile.Read(ref _highWatermark);
    public long LogStartOffset { get; private set; }

    /// <summary>
    /// Bytes written since last compaction (used for dirty ratio calculation)
    /// </summary>
    public long DirtyBytes => Volatile.Read(ref _dirtyBytes);

    /// <summary>
    /// Size of log after last compaction (used for dirty ratio calculation)
    /// </summary>
    public long CleanBytes { get; private set; }

    /// <summary>
    /// Whether CRC validation is enabled on read operations.
    /// </summary>
    public bool ValidateCrcOnRead => _validateCrcOnRead;

    /// <summary>
    /// The current corruption recovery mode.
    /// </summary>
    public CorruptionRecoveryMode CorruptionRecoveryMode => _corruptionRecoveryMode;

    /// <summary>
    /// Configure data integrity validation settings.
    /// </summary>
    /// <param name="validateCrc">Enable CRC validation on reads</param>
    /// <param name="recoveryMode">How to handle corrupted batches</param>
    /// <param name="handler">Optional handler for corruption events</param>
    public void ConfigureDataIntegrity(
        bool validateCrc,
        CorruptionRecoveryMode recoveryMode = CorruptionRecoveryMode.SkipAndContinue,
        ICorruptionHandler? handler = null)
    {
        _validateCrcOnRead = validateCrc;
        _corruptionRecoveryMode = recoveryMode;
        _corruptionHandler = handler;
    }

    public PartitionLog(string dataDirectory, TopicPartition topicPartition, ILogSegmentFactory segmentFactory, long maxSegmentBytes = ILogSegment.DefaultMaxSegmentSize)
    {
        _topicPartition = topicPartition;
        _maxSegmentBytes = maxSegmentBytes;
        _segmentFactory = segmentFactory;
        _baseDirectory = Path.Combine(dataDirectory, topicPartition.Topic, $"partition-{topicPartition.Partition}");

        // Only create directory for persistent storage
        if (_segmentFactory.IsPersistent)
        {
            Directory.CreateDirectory(_baseDirectory);
        }

        LoadExistingSegments();

        if (_activeSegment == null)
        {
            _activeSegment = _segmentFactory.CreateSegment(_baseDirectory, 0, createNew: true, _maxSegmentBytes);
            _segments.Add(_activeSegment);
        }

        _nextOffset = _activeSegment.CurrentOffset;
        Volatile.Write(ref _highWatermark, _nextOffset);

        // Get the actual first message offset - find first segment with actual data
        bool foundData = false;
        LogStartOffset = 0;
        foreach (var segment in _segments)
        {
            var firstOffset = segment.GetFirstMessageOffset();
            if (firstOffset.HasValue)
            {
                LogStartOffset = firstOffset.Value;
                foundData = true;
                break;
            }
            // Segment is empty, try next one
        }

        // If no segment has data, use the active segment's current offset
        if (!foundData && _segments.Count > 0)
        {
            LogStartOffset = _activeSegment?.CurrentOffset ?? 0;
        }

        // Initialize clean bytes as current total size (assume existing data is clean)
        CleanBytes = TotalSize;
    }

    /// <summary>
    /// Append raw Kafka RecordBatch bytes to the log.
    /// Uses lock-free atomic offset claim for maximum concurrency.
    /// </summary>
    public ValueTask<long> AppendBatchAsync(byte[] recordBatch, CancellationToken cancellationToken = default)
    {
        return AppendBatchAsync(recordBatch, 0, recordBatch.Length, cancellationToken);
    }

    /// <summary>
    /// Append a slice of a Kafka RecordBatch buffer to the log.
    /// Zero-copy for ArrayPool buffers - no intermediate allocation.
    /// Uses lock-free atomic offset claim for maximum concurrency.
    /// </summary>
    public ValueTask<long> AppendBatchAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken = default)
    {
        return AppendBatchAsync(buffer, offset, length, BatchCrcMode.Recompute, cancellationToken);
    }

    /// <summary>
    /// Append a slice of a Kafka RecordBatch buffer to the log, with explicit CRC handling (#85).
    /// </summary>
    /// <param name="crcMode">
    /// <see cref="BatchCrcMode.Validate"/> checks the producer's CRC and rejects corrupt batches;
    /// <see cref="BatchCrcMode.Trusted"/> skips the pass for serializer-fresh bytes;
    /// <see cref="BatchCrcMode.Recompute"/> keeps the legacy overwrite.
    /// </param>
    public async ValueTask<long> AppendBatchAsync(byte[] buffer, int offset, int length, BatchCrcMode crcMode, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (length == 0)
        {
            return Volatile.Read(ref _nextOffset);
        }

        var recordBatch = buffer.AsSpan(offset, length);

        // === LOCK-FREE PREPARATION (outside all locks) ===
        // 1. One CRC pass over bytes 21+ (CPU-intensive): Validate compares it against the
        //    producer's, Trusted skips it entirely, Recompute overwrites the field with it.
        //    Throws before any offset is claimed or anything is written.
        var crc = RecordBatchValidator.PrepareAppendCrc(recordBatch, crcMode, _topicPartition.Topic, _topicPartition.Partition);

        // 2. Extract record count from batch header (bytes 57-60, big-endian)
        var recordCount = GetRecordCountFromBatch(recordBatch);

        // === ATOMIC OFFSET CLAIM (minimal contention) ===
        // NOTE: ReaderWriterLockSlim is thread-affine and cannot be held across await.
        // We must release the lock before any async operations.
        long baseOffset;
        ILogSegment segmentToWrite;

        _appendLock.EnterReadLock();
        try
        {
            // Check if segment roll needed (rare path)
            if (_activeSegment!.IsFull)
            {
                _appendLock.ExitReadLock();
                await RollSegmentAsync(cancellationToken);
                _appendLock.EnterReadLock();
            }

            // Atomically claim offset range - lock-free!
            baseOffset = Interlocked.Add(ref _nextOffset, recordCount) - recordCount;

            // Write offset and CRC into batch (fast, just 12 bytes) - modifies buffer in place
            WriteOffsetAndCrc(buffer.AsSpan(offset, length), baseOffset, crc, writeCrc: crcMode == BatchCrcMode.Recompute);

            // Capture segment reference before releasing lock
            segmentToWrite = _activeSegment;
        }
        finally
        {
            _appendLock.ExitReadLock();
        }

        // === SERIALIZED SEGMENT WRITE ===
        // Segment writes must be serialized for correctness
        // This is done outside the read lock since it's async
        await _segmentWriteLock.WaitAsync(cancellationToken);
        try
        {
            await segmentToWrite.AppendBatchAsync(buffer, offset, length, cancellationToken);
        }
        finally
        {
            _segmentWriteLock.Release();
        }

        // Update high watermark atomically
        UpdateHighWatermark(baseOffset + recordCount);

        // Track dirty bytes atomically
        Interlocked.Add(ref _dirtyBytes, length);

        return baseOffset;
    }

    /// <summary>
    /// Append a record batch at a specific target offset for offset-preserving geo-replication.
    /// Validates that targetOffset >= current NextOffset. If targetOffset > NextOffset,
    /// advances the offset to the target position (sparse offsets).
    /// </summary>
    public ValueTask<long> AppendBatchAtOffsetAsync(byte[] recordBatch, long targetOffset, CancellationToken cancellationToken = default)
        => AppendBatchAtOffsetAsync(recordBatch, 0, recordBatch.Length, targetOffset, BatchCrcMode.Recompute, cancellationToken);

    /// <summary>
    /// Offset-preserving append of a single batch (a slice of a split replication fetch section)
    /// with explicit CRC handling (#85). The follower-ingest split hands in exactly ONE batch, so
    /// per-batch <see cref="BatchCrcMode.Validate"/> finally works: the CRC pass covers this batch's
    /// bytes 21.. and compares against this batch's own stored CRC (#92).
    /// </summary>
    public async ValueTask<long> AppendBatchAtOffsetAsync(
        byte[] buffer, int offset, int length, long targetOffset, BatchCrcMode crcMode, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (length == 0)
            return Volatile.Read(ref _nextOffset);

        var currentNext = Volatile.Read(ref _nextOffset);
        if (targetOffset < currentNext)
            throw new InvalidOperationException(
                $"Target offset {targetOffset} is less than current NextOffset {currentNext}. Offset-preserving write requires targetOffset >= NextOffset.");

        var span = buffer.AsSpan(offset, length);
        var crc = RecordBatchValidator.PrepareAppendCrc(span, crcMode, _topicPartition.Topic, _topicPartition.Partition);
        var recordCount = GetRecordCountFromBatch(span);

        _appendLock.EnterReadLock();
        try
        {
            if (_activeSegment!.IsFull)
            {
                _appendLock.ExitReadLock();
                await RollSegmentAsync(cancellationToken);
                _appendLock.EnterReadLock();
            }

            // Set the offset to targetOffset atomically
            Interlocked.Exchange(ref _nextOffset, targetOffset + recordCount);

            // writeCrc only for Recompute; Validate/Trusted keep the batch's own producer CRC (#92).
            WriteOffsetAndCrc(buffer.AsSpan(offset, length), targetOffset, crc, writeCrc: crcMode == BatchCrcMode.Recompute);
        }
        finally
        {
            _appendLock.ExitReadLock();
        }

        await _segmentWriteLock.WaitAsync(cancellationToken);
        try
        {
            await _activeSegment!.AppendBatchAsync(buffer, offset, length, cancellationToken);
        }
        finally
        {
            _segmentWriteLock.Release();
        }

        UpdateHighWatermark(targetOffset + recordCount);
        Interlocked.Add(ref _dirtyBytes, length);

        return targetOffset;
    }

    /// <summary>
    /// Extract record count from Kafka RecordBatch header (bytes 57-60, big-endian).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetRecordCountFromBatch(ReadOnlySpan<byte> recordBatch)
    {
        return BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(57, 4));
    }

    /// <summary>
    /// Atomically update high watermark to the maximum of current and new value.
    /// Notifies any waiters that are waiting for data at or below the new watermark.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void UpdateHighWatermark(long newValue)
    {
        long current;
        do
        {
            current = Volatile.Read(ref _highWatermark);
            if (newValue <= current) return;
        } while (Interlocked.CompareExchange(ref _highWatermark, newValue, current) != current);

        // Notify waiters that new data is available
        NotifyWaiters(newValue);
    }

    /// <summary>
    /// Notify any waiters that data is now available up to the given offset.
    /// </summary>
    private void NotifyWaiters(long availableOffset)
    {
        lock (_waiterLock)
        {
            for (int i = _dataWaiters.Count - 1; i >= 0; i--)
            {
                var waiter = _dataWaiters[i];
                if (waiter.WaitingForOffset <= availableOffset)
                {
                    waiter.Tcs.TrySetResult(true);
                    _dataWaiters.RemoveAt(i);
                }
            }
        }
    }

    /// <summary>
    /// Wait for data to be available at the specified offset.
    /// Returns true if data became available, false if timeout or cancellation.
    /// </summary>
    /// <param name="offset">The offset to wait for</param>
    /// <param name="timeout">Maximum time to wait</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if data is available at or after the offset, false on timeout</returns>
    public async Task<bool> WaitForDataAsync(long offset, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        // Check if data is already available
        if (offset < Volatile.Read(ref _highWatermark))
        {
            return true;
        }

        // Create a waiter
        var waiter = new DataWaiter(offset);

        lock (_waiterLock)
        {
            // Double-check after acquiring lock
            if (offset < Volatile.Read(ref _highWatermark))
            {
                return true;
            }
            _dataWaiters.Add(waiter);
        }

        try
        {
            // Wait with timeout and cancellation
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(timeout);

            try
            {
                await waiter.Tcs.Task.WaitAsync(cts.Token);
                return true;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
        }
        finally
        {
            // Clean up waiter if still registered
            lock (_waiterLock)
            {
                _dataWaiters.Remove(waiter);
            }
        }
    }

    /// <summary>
    /// Internal class to track waiting consumers for long-polling.
    /// </summary>
    private sealed class DataWaiter
    {
        public long WaitingForOffset { get; }
        public TaskCompletionSource<bool> Tcs { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public DataWaiter(long offset)
        {
            WaitingForOffset = offset;
        }
    }

    /// <summary>
    /// Roll to a new segment. Takes exclusive write lock.
    /// NOTE: Uses blocking Wait() instead of WaitAsync() because ReaderWriterLockSlim
    /// is thread-affine and cannot be held across await boundaries.
    /// This is acceptable since segment roll is a rare operation.
    /// </summary>
    private ValueTask RollSegmentAsync(CancellationToken cancellationToken)
    {
        _appendLock.EnterWriteLock();
        try
        {
            // Double-check after acquiring write lock
            if (!_activeSegment!.IsFull) return ValueTask.CompletedTask;

            // Use blocking Wait() since we're holding a thread-affine lock
            _segmentWriteLock.Wait(cancellationToken);
            try
            {
                RollSegment();
            }
            finally
            {
                _segmentWriteLock.Release();
            }
        }
        finally
        {
            _appendLock.ExitWriteLock();
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Write the baseOffset and pre-calculated CRC into the RecordBatch header.
    /// This is very fast (just writing 12 bytes) and done inside the lock.
    /// </summary>
    /// <param name="writeCrc">
    /// False when the batch's own CRC must survive: it was either validated against these very
    /// bytes or written by the serializer moments ago. baseOffset lies outside the CRC-covered
    /// region (which starts at byte 21), so stamping it does not invalidate the checksum.
    /// </param>
    private static void WriteOffsetAndCrc(Span<byte> recordBatch, long baseOffset, uint crc, bool writeCrc)
    {
        // Write baseOffset (bytes 0-7, big-endian)
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(recordBatch[..8], baseOffset);

        if (writeCrc)
        {
            // Write pre-calculated CRC (bytes 17-20, big-endian)
            System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(recordBatch.Slice(17, 4), crc);
        }
    }

    /// <summary>
    /// Read raw RecordBatch bytes from the log starting at the given offset.
    /// Uses memory-mapped I/O for high-performance reads when available.
    /// </summary>
    public async ValueTask<List<byte[]>> ReadBatchesAsync(long startOffset, int maxBytes = 1024 * 1024, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (startOffset < LogStartOffset || startOffset >= _nextOffset)
        {
            return [];
        }

        // Capture segment info under a quick lock, then release before async I/O
        // This allows concurrent reads without blocking writes for too long
        List<(ILogSegment segment, long offset, bool isActive)> segmentsToRead;
        lock (_segmentLock)
        {
            var segment = FindSegment(startOffset);
            if (segment == null)
            {
                return [];
            }

            var segmentIndex = _segments.IndexOf(segment);
            segmentsToRead = [];

            for (int i = segmentIndex; i < _segments.Count; i++)
            {
                var currentSegment = _segments[i];
                var segmentStartOffset = (i == segmentIndex) ? startOffset : currentSegment.BaseOffset;
                var isActive = currentSegment == _activeSegment;
                segmentsToRead.Add((currentSegment, segmentStartOffset, isActive));
            }
        }

        // Perform async I/O without holding any lock
        var batches = new List<byte[]>();
        var totalBytes = 0;

        foreach (var (currentSegment, segmentStartOffset, isActive) in segmentsToRead)
        {
            if (totalBytes >= maxBytes) break;

            List<byte[]> segmentBatches;

            // For active segment, use FileStream reads because mmap can't see unflushed buffered writes.
            // For closed segments, use mmap for better performance (all data is on disk).
            if (isActive)
            {
                segmentBatches = await currentSegment.ReadBatchesAsync(segmentStartOffset, maxBytes - totalBytes, cancellationToken);
            }
            else
            {
                segmentBatches = _reader.ReadBatchesWithMmap(currentSegment, segmentStartOffset, maxBytes - totalBytes);
            }

            // Validate CRC if enabled
            if (_validateCrcOnRead)
            {
                foreach (var batch in segmentBatches)
                {
                    if (!RecordBatchValidator.ValidateCrc(batch, out var expected, out var actual))
                    {
                        var baseOffset = RecordBatchValidator.GetBaseOffset(batch);
                        var info = new CorruptedBatchInfo(
                            _topicPartition.Topic,
                            _topicPartition.Partition,
                            baseOffset,
                            expected,
                            actual,
                            batch.Length);

                        _corruptionHandler?.OnCorruptionDetected(info);

                        if (_corruptionRecoveryMode == CorruptionRecoveryMode.FailFast)
                        {
                            throw new DataCorruptionException(info);
                        }
                        // SkipAndContinue: skip this batch
                        continue;
                    }
                    batches.Add(batch);
                    totalBytes += batch.Length;
                }
            }
            else
            {
                batches.AddRange(segmentBatches);
                foreach (var b in segmentBatches)
                    totalBytes += b.Length;
            }

            if (segmentBatches.Count == 0)
            {
                break;
            }
        }

        return batches;
    }

    /// <summary>
    /// Read raw RecordBatch bytes as a single contiguous array with parallel I/O for multi-segment reads.
    /// This is optimized for high-throughput fetch operations.
    /// Returns the data and batch offsets within it for processing.
    /// </summary>
    public async ValueTask<(ReadOnlyMemory<byte> Data, List<int> BatchOffsets)> ReadBatchesContiguousAsync(long startOffset, int maxBytes = 1024 * 1024, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (startOffset < LogStartOffset || startOffset >= _nextOffset)
        {
            return (ReadOnlyMemory<byte>.Empty, []);
        }

        // Capture segment info under a quick lock, then release before async I/O
        ILogSegment? segment;
        int segmentIndex;
        int segmentCount;
        ILogSegment? activeSegment;
        List<(ILogSegment segment, long offset)>? segmentsToRead = null;

        lock (_segmentLock)
        {
            segment = FindSegment(startOffset);
            if (segment == null)
            {
                return (ReadOnlyMemory<byte>.Empty, []);
            }

            segmentIndex = _segments.IndexOf(segment);
            segmentCount = _segments.Count;
            activeSegment = _activeSegment;

            // For multi-segment reads, capture all segments needed
            if (segmentIndex < _segments.Count - 1 && segment != _activeSegment)
            {
                segmentsToRead = [];
                for (int i = segmentIndex; i < _segments.Count; i++)
                {
                    var currentSegment = _segments[i];
                    var segmentStartOffset = (i == segmentIndex) ? startOffset : currentSegment.BaseOffset;
                    segmentsToRead.Add((currentSegment, segmentStartOffset));
                }
            }
        }

        // Perform async I/O without holding any lock
        // For single segment reads (most common case), use direct path
        (ReadOnlyMemory<byte> Data, List<int> BatchOffsets) result;
        if (segmentIndex == segmentCount - 1 || segment == activeSegment)
        {
            result = await _reader.ReadSingleSegmentContiguousAsync(segment, activeSegment, startOffset, maxBytes, cancellationToken);
        }
        else
        {
            // Multi-segment: use parallel I/O
            result = await _reader.ReadMultiSegmentContiguousAsync(segmentsToRead!, activeSegment, maxBytes, cancellationToken);
        }

        // Validate CRC if enabled
        if (_validateCrcOnRead && result.Data.Length > 0)
        {
            return ValidateContiguousBatches(result.Data, result.BatchOffsets);
        }

        return result;
    }

    /// <summary>
    /// Validates CRC for batches in a contiguous buffer and returns only valid batches.
    /// </summary>
    private (ReadOnlyMemory<byte> Data, List<int> BatchOffsets) ValidateContiguousBatches(ReadOnlyMemory<byte> data, List<int> batchOffsets)
    {
        if (batchOffsets.Count == 0)
            return (data, batchOffsets);

        var validBatchOffsets = new List<int>();
        var validData = new List<byte>();

        for (int i = 0; i < batchOffsets.Count; i++)
        {
            var batchStart = batchOffsets[i];
            var batchEnd = (i + 1 < batchOffsets.Count) ? batchOffsets[i + 1] : data.Length;
            var batchLength = batchEnd - batchStart;

            if (batchLength < RecordBatchValidator.MinBatchHeaderSize)
                continue;

            var batch = data.Span.Slice(batchStart, batchLength);

            if (!RecordBatchValidator.ValidateCrc(batch, out var expected, out var actual))
            {
                var baseOffset = RecordBatchValidator.GetBaseOffset(batch);
                var info = new CorruptedBatchInfo(
                    _topicPartition.Topic,
                    _topicPartition.Partition,
                    baseOffset,
                    expected,
                    actual,
                    batchLength);

                _corruptionHandler?.OnCorruptionDetected(info);

                if (_corruptionRecoveryMode == CorruptionRecoveryMode.FailFast)
                {
                    throw new DataCorruptionException(info);
                }
                // SkipAndContinue: skip this batch
                continue;
            }

            // Valid batch - track its position in the new data array
            validBatchOffsets.Add(validData.Count);
            validData.AddRange(batch.ToArray());
        }

        return (validData.ToArray(), validBatchOffsets);
    }

    /// <summary>
    /// Truncate the log to the specified offset
    /// </summary>
    public void TruncateTo(long offset)
    {
        _appendLock.EnterWriteLock();
        try
        {
            // Find segments to remove
            var segmentsToRemove = _segments.Where(s => s.BaseOffset >= offset).ToList();

            foreach (var segment in segmentsToRemove)
            {
                segment.Dispose();
                _segments.Remove(segment);
            }

            if (_segments.Count == 0)
            {
                _activeSegment = _segmentFactory.CreateSegment(_baseDirectory, offset, createNew: true, _maxSegmentBytes);
                _segments.Add(_activeSegment);
            }
            else
            {
                _activeSegment = _segments[^1];
            }

            Volatile.Write(ref _nextOffset, offset);
            Volatile.Write(ref _highWatermark, offset);
        }
        finally
        {
            _appendLock.ExitWriteLock();
        }
    }

    private void LoadExistingSegments()
    {
        // Only load existing segments for persistent storage
        if (!_segmentFactory.IsPersistent || !Directory.Exists(_baseDirectory))
        {
            return;
        }

        var logFiles = Directory.GetFiles(_baseDirectory, "*.log")
            .Select(f => Path.GetFileNameWithoutExtension(f))
            .Where(name => long.TryParse(name, out _))
            .Select(long.Parse)
            .OrderBy(offset => offset)
            .ToList();

        foreach (var baseOffset in logFiles)
        {
            var segment = _segmentFactory.CreateSegment(_baseDirectory, baseOffset, createNew: false, _maxSegmentBytes);
            _segments.Add(segment);
        }

        _activeSegment = _segments.LastOrDefault();
    }

    private void RollSegment()
    {
        var newBaseOffset = _nextOffset;
        var newSegment = _segmentFactory.CreateSegment(_baseDirectory, newBaseOffset, createNew: true, _maxSegmentBytes);
        _segments.Add(newSegment);
        _activeSegment = newSegment;
    }

    private ILogSegment? FindSegment(long offset)
    {
        var segments = _segments;
        var count = segments.Count;
        if (count == 0) return null;

        // Fast path: most reads hit the active (last) segment
        if (segments[count - 1].BaseOffset <= offset)
            return segments[count - 1];

        // Binary search for the segment whose BaseOffset is <= offset.
        // _segments is sorted by BaseOffset (append-only, segments are created
        // with monotonically increasing offsets).
        int lo = 0, hi = count - 2; // exclude last (already checked)
        ILogSegment? result = null;

        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            if (segments[mid].BaseOffset <= offset)
            {
                result = segments[mid];
                lo = mid + 1; // might be a later segment
            }
            else
            {
                hi = mid - 1;
            }
        }

        return result;
    }

    /// <summary>
    /// Get the total size of all segments in this partition log
    /// </summary>
    public long TotalSize => _segments.Sum(s => s.Size);

    /// <summary>
    /// Calculate the dirty ratio (dirty bytes / total bytes).
    /// Returns 0 if log is empty, 1.0 if all data is dirty (never compacted).
    /// </summary>
    public double DirtyRatio
    {
        get
        {
            var total = TotalSize;
            if (total == 0) return 0;
            // Dirty ratio = dirty bytes / (clean bytes + dirty bytes)
            // If CleanBytes is 0 and DirtyBytes > 0, ratio is 1.0 (all dirty)
            var effectiveTotal = CleanBytes + DirtyBytes;
            if (effectiveTotal == 0) return 0;
            return (double)DirtyBytes / effectiveTotal;
        }
    }

    /// <summary>
    /// Reset dirty tracking after compaction completes.
    /// Sets CleanBytes to current total size and resets DirtyBytes to 0.
    /// </summary>
    /// <param name="bytesRemoved">Bytes removed during compaction (for accurate tracking)</param>
    public void ResetDirtyTracking(long bytesRemoved)
    {
        // After compaction, current size is the new "clean" baseline
        CleanBytes = TotalSize;
        Volatile.Write(ref _dirtyBytes, 0);
    }

    /// <summary>
    /// Get all segments (for retention analysis)
    /// </summary>
    public IReadOnlyList<ILogSegment> Segments => _segments.AsReadOnly();

    /// <summary>
    /// Apply retention policy and delete old segments
    /// </summary>
    /// <returns>Number of segments deleted</returns>
    public int ApplyRetentionPolicy(RetentionPolicy policy)
    {
        _appendLock.EnterWriteLock();
        try
        {
            var deletedCount = 0;
            var now = DateTime.UtcNow;
            var cutoffTime = policy.RetentionHours > 0
                ? now.AddHours(-policy.RetentionHours)
                : DateTime.MinValue;

            // Calculate total size for size-based retention
            var totalSize = TotalSize;

            // Never delete the active segment
            var segmentsToCheck = _segments.Take(_segments.Count - Math.Max(1, policy.MinSegmentsToKeep)).ToList();

            foreach (var segment in segmentsToCheck)
            {
                var shouldDelete = false;

                // Time-based retention: delete if segment is older than retention period
                if (policy.RetentionHours > 0 && segment.CreatedAt < cutoffTime)
                {
                    shouldDelete = true;
                }

                // Size-based retention: delete if total size exceeds limit
                if (policy.RetentionBytes > 0 && totalSize > policy.RetentionBytes)
                {
                    shouldDelete = true;
                }

                if (shouldDelete)
                {
                    // Update LogStartOffset before deleting
                    var nextSegmentIndex = _segments.IndexOf(segment) + 1;
                    if (nextSegmentIndex < _segments.Count)
                    {
                        var nextSegment = _segments[nextSegmentIndex];
                        LogStartOffset = nextSegment.GetFirstMessageOffset() ?? nextSegment.BaseOffset;
                    }

                    // Dispose mmap reader if it exists for this segment
                    _reader.RemoveMmapReader(segment.BaseOffset);

                    segment.Dispose();
                    segment.DeleteFiles();
                    _segments.Remove(segment);
                    totalSize -= segment.Size;
                    deletedCount++;
                }
            }

            return deletedCount;
        }
        finally
        {
            _appendLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Delete all records before the specified offset by updating LogStartOffset
    /// and optionally removing segments that are entirely before the new start.
    /// Returns the new LogStartOffset after deletion.
    /// </summary>
    /// <param name="offset">Delete all records with offset less than this value. Use -1 to delete all records up to high watermark.</param>
    /// <returns>The new LogStartOffset after deletion.</returns>
    public long DeleteRecordsToOffset(long offset)
    {
        _appendLock.EnterWriteLock();
        try
        {
            // Handle -1 meaning "delete everything up to high watermark"
            var targetOffset = offset == -1 ? Volatile.Read(ref _nextOffset) : offset;

            // Can't delete beyond what exists
            if (targetOffset > Volatile.Read(ref _nextOffset))
            {
                targetOffset = Volatile.Read(ref _nextOffset);
            }

            // Can't set LogStartOffset lower than it already is
            if (targetOffset <= LogStartOffset)
            {
                return LogStartOffset;
            }

            // Update LogStartOffset
            LogStartOffset = targetOffset;

            // Delete segments that are entirely before the new LogStartOffset
            var segmentsToDelete = _segments
                .Where(s => s != _activeSegment) // Never delete active segment
                .Where(s =>
                {
                    // Find the highest offset in this segment
                    var nextSegment = _segments.SkipWhile(seg => seg != s).Skip(1).FirstOrDefault();
                    var segmentEndOffset = nextSegment?.BaseOffset ?? Volatile.Read(ref _nextOffset);
                    return segmentEndOffset <= LogStartOffset;
                })
                .ToList();

            foreach (var segment in segmentsToDelete)
            {
                // Dispose mmap reader if it exists
                _reader.RemoveMmapReader(segment.BaseOffset);

                segment.Dispose();
                segment.DeleteFiles();
                _segments.Remove(segment);
            }

            return LogStartOffset;
        }
        finally
        {
            _appendLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Find the offset of the first batch with timestamp >= targetTimestamp
    /// Searches across all segments in chronological order
    /// </summary>
    public long? FindOffsetByTimestamp(long targetTimestamp)
    {
        _appendLock.EnterReadLock();
        try
        {
            // Search segments in order (oldest to newest)
            foreach (var segment in _segments)
            {
                var offset = segment.FindOffsetByTimestamp(targetTimestamp);
                if (offset != null)
                {
                    return offset;
                }
            }

            return null;
        }
        finally
        {
            _appendLock.ExitReadLock();
        }
    }

    public void Dispose()
    {
        if (_disposed) return;

        _appendLock.EnterWriteLock();
        try
        {
            // Dispose reader (clears mmap readers)
            _reader.Dispose();

            foreach (var segment in _segments)
            {
                segment.Dispose();
            }

            _segments.Clear();
            _activeSegment = null;
            _disposed = true;
        }
        finally
        {
            _appendLock.ExitWriteLock();
            _appendLock.Dispose();
            _segmentWriteLock.Dispose();
        }
    }
}
