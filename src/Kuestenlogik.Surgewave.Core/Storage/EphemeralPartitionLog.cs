using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// A non-persistent partition log backed by a fixed-size ring buffer.
/// Messages are only available to active consumers and are discarded when the buffer wraps around.
/// Optimized for real-time event distribution (similar to Redis Pub/Sub or NATS Core).
/// </summary>
public sealed class EphemeralPartitionLog : IPartitionLog
{
    private readonly byte[] _ringBuffer;
    private readonly RingBatchEntry[] _batchEntries;
    private int _entryHead;   // Next write position in _batchEntries (circular)
    private int _entryCount;  // Current number of valid entries
    private int _writePosition; // Current write position in _ringBuffer
    private long _nextOffset;
    private long _highWatermark;
    private long _logStartOffset;
    private readonly int _maxEntries;
    private bool _disposed;

    // Long-polling waiters
    private readonly Lock _waiterLock = new();
    private readonly List<DataWaiter> _dataWaiters = [];

    // SpinLock for short critical sections
    private SpinLock _lock = new(enableThreadOwnerTracking: false);

    public TopicPartition TopicPartition { get; }
    public long NextOffset => Volatile.Read(ref _nextOffset);
    public long HighWatermark => Volatile.Read(ref _highWatermark);
    public long LogStartOffset => Volatile.Read(ref _logStartOffset);
    public long TotalSize => _ringBuffer.Length;

    /// <summary>
    /// Creates an ephemeral partition log with a fixed-size ring buffer.
    /// </summary>
    /// <param name="topicPartition">The topic-partition this log belongs to</param>
    /// <param name="bufferBytes">Size of the ring buffer in bytes (default 64MB)</param>
    /// <param name="maxEntries">Maximum number of batch index entries (default 1M)</param>
    public EphemeralPartitionLog(TopicPartition topicPartition, long bufferBytes = 64 * 1024 * 1024, int maxEntries = 1_048_576)
    {
        TopicPartition = topicPartition;
        _ringBuffer = new byte[bufferBytes];
        _maxEntries = maxEntries;
        _batchEntries = new RingBatchEntry[maxEntries];
    }

    public ValueTask<long> AppendBatchAsync(byte[] recordBatch, CancellationToken cancellationToken = default)
    {
        return AppendBatchAsync(recordBatch, 0, recordBatch.Length, cancellationToken);
    }

    public ValueTask<long> AppendBatchAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken = default)
    {
        return AppendBatchAsync(buffer, offset, length, BatchCrcMode.Recompute, cancellationToken);
    }

    /// <inheritdoc cref="PartitionLog.AppendBatchAsync(byte[], int, int, BatchCrcMode, CancellationToken)"/>
    public ValueTask<long> AppendBatchAsync(byte[] buffer, int offset, int length, BatchCrcMode crcMode, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (length == 0)
            return new ValueTask<long>(Volatile.Read(ref _nextOffset));

        var recordBatch = buffer.AsSpan(offset, length);

        // Single CRC pass, outside the lock (#85)
        var crc = RecordBatchValidator.PrepareAppendCrc(recordBatch, crcMode, TopicPartition.Topic, TopicPartition.Partition);
        var recordCount = GetRecordCountFromBatch(recordBatch);

        long baseOffset;
        bool lockTaken = false;
        try
        {
            _lock.Enter(ref lockTaken);

            // Claim offset range
            baseOffset = _nextOffset;
            _nextOffset += recordCount;

            // Write offset + CRC into batch header
            WriteOffsetAndCrc(buffer.AsSpan(offset, length), baseOffset, crc, writeCrc: crcMode == BatchCrcMode.Recompute);

            // Write to ring buffer with wrap-around
            WriteToRingBuffer(buffer, offset, length);

            // Add batch entry to circular index
            AddBatchEntry(baseOffset, recordCount, _writePosition - length, length);
        }
        finally
        {
            if (lockTaken) _lock.Exit(false);
        }

        // Update high watermark and notify waiters (outside SpinLock)
        UpdateHighWatermark(baseOffset + recordCount);

        return new ValueTask<long>(baseOffset);
    }

    /// <summary>
    /// Append a record batch at a specific target offset (for offset-preserving geo-replication).
    /// Not supported for ephemeral logs.
    /// </summary>
    public ValueTask<long> AppendBatchAtOffsetAsync(byte[] recordBatch, long targetOffset, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("Offset-preserving writes are not supported for ephemeral partition logs.");
    }

    public ValueTask<List<byte[]>> ReadBatchesAsync(long startOffset, int maxBytes = 1048576, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var logStart = Volatile.Read(ref _logStartOffset);
        var nextOff = Volatile.Read(ref _nextOffset);

        if (startOffset < logStart || startOffset >= nextOff)
            return new ValueTask<List<byte[]>>([]);

        var batches = new List<byte[]>();
        var totalBytes = 0;

        bool lockTaken = false;
        try
        {
            _lock.Enter(ref lockTaken);

            // Find first entry with offset >= startOffset via linear scan of valid entries
            var entryIndex = FindEntryIndex(startOffset);
            if (entryIndex < 0)
                return new ValueTask<List<byte[]>>([]);

            // Read batches from ring buffer
            for (int i = 0; i < _entryCount && totalBytes < maxBytes; i++)
            {
                var idx = (_entryHead - _entryCount + i + _maxEntries) % _maxEntries;
                ref var entry = ref _batchEntries[idx];

                if (entry.BaseOffset < startOffset)
                    continue;

                var batch = ReadFromRingBuffer(entry.RingPosition, entry.Length);
                batches.Add(batch);
                totalBytes += entry.Length;
            }
        }
        finally
        {
            if (lockTaken) _lock.Exit(false);
        }

        return new ValueTask<List<byte[]>>(batches);
    }

    public ValueTask<(ReadOnlyMemory<byte> Data, List<int> BatchOffsets)> ReadBatchesContiguousAsync(long startOffset, int maxBytes = 1048576, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var logStart = Volatile.Read(ref _logStartOffset);
        var nextOff = Volatile.Read(ref _nextOffset);

        if (startOffset < logStart || startOffset >= nextOff)
            return new ValueTask<(ReadOnlyMemory<byte>, List<int>)>((ReadOnlyMemory<byte>.Empty, []));

        var batchOffsets = new List<int>();
        int totalBytes = 0;

        bool lockTaken = false;
        byte[]? result;
        try
        {
            _lock.Enter(ref lockTaken);

            // First pass: calculate total size
            var tempOffsets = new List<(int RingPos, int Length)>();
            for (int i = 0; i < _entryCount && totalBytes < maxBytes; i++)
            {
                var idx = (_entryHead - _entryCount + i + _maxEntries) % _maxEntries;
                ref var entry = ref _batchEntries[idx];

                if (entry.BaseOffset < startOffset)
                    continue;

                tempOffsets.Add((entry.RingPosition, entry.Length));
                totalBytes += entry.Length;
            }

            if (totalBytes == 0)
                return new ValueTask<(ReadOnlyMemory<byte>, List<int>)>((ReadOnlyMemory<byte>.Empty, []));

            // Second pass: copy to contiguous buffer
            result = new byte[totalBytes];
            var writePos = 0;
            foreach (var (ringPos, length) in tempOffsets)
            {
                batchOffsets.Add(writePos);
                CopyFromRingBuffer(ringPos, length, result, writePos);
                writePos += length;
            }
        }
        finally
        {
            if (lockTaken) _lock.Exit(false);
        }

        return new ValueTask<(ReadOnlyMemory<byte>, List<int>)>((result, batchOffsets));
    }

    public async Task<bool> WaitForDataAsync(long offset, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        if (offset < Volatile.Read(ref _highWatermark))
            return true;

        var waiter = new DataWaiter(offset);

        lock (_waiterLock)
        {
            if (offset < Volatile.Read(ref _highWatermark))
                return true;
            _dataWaiters.Add(waiter);
        }

        try
        {
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
            lock (_waiterLock)
            {
                _dataWaiters.Remove(waiter);
            }
        }
    }

    public long? FindOffsetByTimestamp(long targetTimestamp) => null;

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        lock (_waiterLock)
        {
            foreach (var waiter in _dataWaiters)
            {
                waiter.Tcs.TrySetCanceled();
            }
            _dataWaiters.Clear();
        }
    }

    // --- Private helpers ---

    /// <summary>
    /// Write data to the ring buffer, handling wrap-around.
    /// Advances _writePosition and evicts old entries if the buffer wraps.
    /// </summary>
    private void WriteToRingBuffer(byte[] buffer, int offset, int length)
    {
        var ringSize = _ringBuffer.Length;

        // Track which bytes will be overwritten
        var writeStart = _writePosition % ringSize;
        var writeEnd = (writeStart + length) % ringSize;

        // Copy data, handling wrap-around
        var firstCopyLen = Math.Min(length, ringSize - writeStart);
        Buffer.BlockCopy(buffer, offset, _ringBuffer, writeStart, firstCopyLen);

        if (firstCopyLen < length)
        {
            // Wrap around
            Buffer.BlockCopy(buffer, offset + firstCopyLen, _ringBuffer, 0, length - firstCopyLen);
        }

        var oldWritePos = _writePosition;
        _writePosition = (_writePosition + length) % ringSize;

        // Evict old entries that were overwritten
        EvictOverwrittenEntries(oldWritePos, length, ringSize);
    }

    /// <summary>
    /// Evict batch entries whose data has been overwritten by the ring buffer advancing.
    /// </summary>
    private void EvictOverwrittenEntries(int oldWritePos, int bytesWritten, int ringSize)
    {
        // Check if we've wrapped around the buffer (written more than remaining space from old position)
        while (_entryCount > 0)
        {
            var oldestIdx = (_entryHead - _entryCount + _maxEntries) % _maxEntries;
            ref var oldest = ref _batchEntries[oldestIdx];

            // Check if this entry's data overlaps with newly written data
            if (!IsOverwritten(oldest.RingPosition, oldest.Length, oldWritePos, bytesWritten, ringSize))
                break;

            // This entry was overwritten - advance LogStartOffset
            Volatile.Write(ref _logStartOffset, oldest.BaseOffset + oldest.RecordCount);
            _entryCount--;
        }
    }

    /// <summary>
    /// Check if a region in the ring buffer has been overwritten.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsOverwritten(int entryPos, int entryLen, int writeStart, int writeLen, int ringSize)
    {
        // Convert positions to a linear space relative to writeStart for overlap detection
        // An entry is overwritten if any part of it falls within [writeStart, writeStart+writeLen)
        var entryStartDist = ((entryPos - writeStart) % ringSize + ringSize) % ringSize;
        var entryEndDist = ((entryPos + entryLen - 1 - writeStart) % ringSize + ringSize) % ringSize;

        // If write wraps and is larger than or equal to ring, everything is overwritten
        if (writeLen >= ringSize) return true;

        // Entry start or end falls within written region
        return entryStartDist < writeLen || entryEndDist < writeLen;
    }

    /// <summary>
    /// Add a batch entry to the circular index.
    /// </summary>
    private void AddBatchEntry(long baseOffset, int recordCount, int ringPosition, int length)
    {
        // Normalize ring position
        var ringSize = _ringBuffer.Length;
        ringPosition = ((ringPosition % ringSize) + ringSize) % ringSize;

        _batchEntries[_entryHead] = new RingBatchEntry
        {
            BaseOffset = baseOffset,
            RecordCount = recordCount,
            RingPosition = ringPosition,
            Length = length
        };

        _entryHead = (_entryHead + 1) % _maxEntries;
        if (_entryCount < _maxEntries)
            _entryCount++;
        else
        {
            // Index is full - oldest entry is implicitly evicted
            var oldestIdx = (_entryHead - _maxEntries + _maxEntries) % _maxEntries;
            Volatile.Write(ref _logStartOffset, _batchEntries[oldestIdx].BaseOffset + _batchEntries[oldestIdx].RecordCount);
        }
    }

    /// <summary>
    /// Find the index in _batchEntries for the first entry with BaseOffset >= startOffset.
    /// Returns -1 if not found.
    /// </summary>
    private int FindEntryIndex(long startOffset)
    {
        // Linear scan through valid entries (oldest to newest)
        for (int i = 0; i < _entryCount; i++)
        {
            var idx = (_entryHead - _entryCount + i + _maxEntries) % _maxEntries;
            if (_batchEntries[idx].BaseOffset >= startOffset)
                return idx;
            // Also match if startOffset falls within this batch's offset range
            if (_batchEntries[idx].BaseOffset + _batchEntries[idx].RecordCount > startOffset)
                return idx;
        }
        return -1;
    }

    /// <summary>
    /// Read data from the ring buffer, handling wrap-around.
    /// </summary>
    private byte[] ReadFromRingBuffer(int position, int length)
    {
        var result = new byte[length];
        CopyFromRingBuffer(position, length, result, 0);
        return result;
    }

    /// <summary>
    /// Copy data from ring buffer to destination, handling wrap-around.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CopyFromRingBuffer(int position, int length, byte[] destination, int destOffset)
    {
        var ringSize = _ringBuffer.Length;
        var normalizedPos = position % ringSize;

        var firstCopyLen = Math.Min(length, ringSize - normalizedPos);
        Buffer.BlockCopy(_ringBuffer, normalizedPos, destination, destOffset, firstCopyLen);

        if (firstCopyLen < length)
        {
            Buffer.BlockCopy(_ringBuffer, 0, destination, destOffset + firstCopyLen, length - firstCopyLen);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetRecordCountFromBatch(ReadOnlySpan<byte> recordBatch)
    {
        return BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(57, 4));
    }

    /// <param name="writeCrc">
    /// False when the batch's own CRC must survive — see PartitionLog.WriteOffsetAndCrc.
    /// </param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteOffsetAndCrc(Span<byte> recordBatch, long baseOffset, uint crc, bool writeCrc)
    {
        BinaryPrimitives.WriteInt64BigEndian(recordBatch[..8], baseOffset);
        if (writeCrc)
        {
            BinaryPrimitives.WriteUInt32BigEndian(recordBatch.Slice(17, 4), crc);
        }
    }

    private void UpdateHighWatermark(long newValue)
    {
        long current;
        do
        {
            current = Volatile.Read(ref _highWatermark);
            if (newValue <= current) return;
        } while (Interlocked.CompareExchange(ref _highWatermark, newValue, current) != current);

        NotifyWaiters(newValue);
    }

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

    private sealed class DataWaiter
    {
        public long WaitingForOffset { get; }
        public TaskCompletionSource<bool> Tcs { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public DataWaiter(long offset) => WaitingForOffset = offset;
    }

    private struct RingBatchEntry
    {
        public long BaseOffset;
        public int RecordCount;
        public int RingPosition;
        public int Length;
    }
}
