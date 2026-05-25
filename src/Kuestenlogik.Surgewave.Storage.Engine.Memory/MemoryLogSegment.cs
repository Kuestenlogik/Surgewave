using System.Buffers.Binary;
using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Storage.Engine.Memory;

/// <summary>
/// In-memory log segment implementation.
/// Uses a growable byte array for storage with no disk I/O.
/// Ideal for testing, ephemeral workloads, and ultra-low-latency scenarios.
/// </summary>
public sealed class MemoryLogSegment : IMemoryLogSegment
{
    private byte[] _data;
    private int _writePosition;
    private readonly long _baseOffset;
    private readonly long _maxSegmentSize;
    private long _currentOffset;
    private bool _disposed;

    // Offset index: maps batch baseOffset -> buffer position (O(1) insert)
    private readonly ConcurrentDictionary<long, int> _offsetIndex = new();

    // Track offsets in insertion order for efficient range lookups
    private readonly List<long> _offsetsInOrder = new();

    // Timestamp index: maps maxTimestamp -> batch baseOffset
    private readonly Dictionary<long, long> _timestampIndex = new();

    // Track timestamps in insertion order for efficient range lookups
    private readonly List<long> _timestampsInOrder = new();

    // Write lock for mutual exclusion between concurrent writers. Reads are lock-free:
    // _data is append-only (old content never changes), _writePosition and _currentOffset
    // are published with Volatile.Write after data is committed, and _offsetIndex is a
    // ConcurrentDictionary. This eliminates ReaderWriterLockSlim overhead (~100ns per read).
    private readonly Lock _writeLock = new();

    public long BaseOffset => _baseOffset;
    public long CurrentOffset => _currentOffset;
    public long Size => _writePosition;
    public bool IsFull => Size >= _maxSegmentSize;
    public DateTime CreatedAt { get; }
    public long MaxTimestamp { get; private set; }

    public MemoryLogSegment(long baseOffset, long maxSegmentSize = ILogSegment.DefaultMaxSegmentSize)
    {
        _baseOffset = baseOffset;
        _currentOffset = baseOffset;
        _maxSegmentSize = maxSegmentSize;
        _data = new byte[Math.Min(1024 * 1024, (int)maxSegmentSize)]; // Start with 1MB or max size
        CreatedAt = DateTime.UtcNow;
    }

    public long? GetFirstMessageOffset()
    {
        if (Volatile.Read(ref _writePosition) < 8) return null;
        return BinaryPrimitives.ReadInt64BigEndian(_data.AsSpan(0, 8));
    }

    public ValueTask<(long baseOffset, int recordCount)> AppendBatchAsync(byte[] recordBatch, CancellationToken cancellationToken = default)
    {
        return AppendBatchAsync(recordBatch.AsMemory(), cancellationToken);
    }

    /// <summary>
    /// Append a raw Kafka RecordBatch using ReadOnlyMemory for zero-copy scenarios.
    /// </summary>
    public ValueTask<(long baseOffset, int recordCount)> AppendBatchAsync(ReadOnlyMemory<byte> recordBatch, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        lock (_writeLock)
        {
            // Parse batch header using Span (zero-copy)
            var (batchBaseOffset, recordCount, maxTimestamp) = ParseBatchHeader(recordBatch.Span);

            // Ensure capacity (may grow _data — old array stays valid via GC)
            EnsureCapacity(_writePosition + recordBatch.Length);

            // Record position before writing
            var position = _writePosition;

            // Copy data (single copy, no intermediate buffer)
            recordBatch.Span.CopyTo(_data.AsSpan(_writePosition));

            // Update index BEFORE publishing _writePosition — readers check index first
            _offsetIndex[batchBaseOffset] = position;
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

            // Publish new positions with memory barrier — readers use Volatile.Read
            // to see these values. Data is committed before positions are published.
            Volatile.Write(ref _writePosition, _writePosition + recordBatch.Length);
            Volatile.Write(ref _currentOffset, batchBaseOffset + recordCount);

            return ValueTask.FromResult((batchBaseOffset, recordCount));
        }
    }

    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        // No-op for memory segment - data is always "flushed"
        return ValueTask.CompletedTask;
    }

    public ValueTask<List<byte[]>> ReadBatchesAsync(long startOffset, int maxBytes, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Lock-free read: snapshot _currentOffset and _writePosition via Volatile.Read.
        // Data up to the snapshot writePosition is immutable (append-only), so no lock
        // is needed. _offsetIndex is a ConcurrentDictionary (thread-safe TryGetValue).
        var currentOffset = Volatile.Read(ref _currentOffset);
        var writePos = Volatile.Read(ref _writePosition);
        var data = _data; // capture reference — may be replaced by EnsureCapacity, but
                          // old array stays valid via GC and contains committed data

        if (startOffset < _baseOffset || startOffset >= currentOffset)
        {
            return ValueTask.FromResult<List<byte[]>>([]);
        }

        var batchOffset = FindBatchOffsetForRead(startOffset);
        if (batchOffset == null || !_offsetIndex.TryGetValue(batchOffset.Value, out var position))
        {
            return ValueTask.FromResult<List<byte[]>>([]);
        }

        // Cap writePos to data.Length to guard against reader seeing a writePos
        // published for a newer (grown) array while still holding the old reference.
        if (writePos > data.Length) writePos = data.Length;

        var batches = new List<byte[]>();
        var totalBytes = 0;

        while (position + 12 <= writePos && totalBytes < maxBytes)
        {
            var batchLength = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(position + 8, 4));
            var totalBatchSize = 12 + batchLength;

            if (position + totalBatchSize > writePos)
                break;

            if (totalBytes > 0 && totalBytes + totalBatchSize > maxBytes)
                break;

            var batchBytes = new byte[totalBatchSize];
            data.AsSpan(position, totalBatchSize).CopyTo(batchBytes);
            batches.Add(batchBytes);

            totalBytes += totalBatchSize;
            position += totalBatchSize;
        }

        return ValueTask.FromResult(batches);
    }

    public ValueTask<(ReadOnlyMemory<byte> Data, List<int> BatchOffsets)> ReadBatchesContiguousAsync(long startOffset, int maxBytes, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Lock-free contiguous read — same pattern as ReadBatchesAsync
        var currentOffset = Volatile.Read(ref _currentOffset);
        var writePos = Volatile.Read(ref _writePosition);
        var data = _data;
        if (writePos > data.Length) writePos = data.Length;

        {
            if (startOffset < _baseOffset || startOffset >= currentOffset)
            {
                return ValueTask.FromResult<(ReadOnlyMemory<byte>, List<int>)>((ReadOnlyMemory<byte>.Empty, []));
            }

            var batchOffset = FindBatchOffsetForRead(startOffset);
            if (batchOffset == null || !_offsetIndex.TryGetValue(batchOffset.Value, out var startPosition))
            {
                return ValueTask.FromResult<(ReadOnlyMemory<byte>, List<int>)>((ReadOnlyMemory<byte>.Empty, []));
            }

            var batchOffsets = new List<int>();
            var position = startPosition;
            var validBytes = 0;

            // Find all complete batches within maxBytes
            while (position + 12 <= writePos)
            {
                var batchLength = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(position + 8, 4));
                var totalBatchSize = 12 + batchLength;

                if (position + totalBatchSize > writePos)
                    break;

                // Ensure at least first batch is returned
                if (validBytes > 0 && validBytes + totalBatchSize > maxBytes)
                    break;

                batchOffsets.Add(validBytes);
                validBytes += totalBatchSize;
                position += totalBatchSize;
            }

            if (validBytes == 0)
            {
                return ValueTask.FromResult<(ReadOnlyMemory<byte>, List<int>)>((ReadOnlyMemory<byte>.Empty, []));
            }

            // Zero-copy: return a memory slice pointing directly into the captured data array.
            // Safe because: ReadOnlyMemory<byte> holds a reference preventing GC collection
            // even if EnsureCapacity replaces _data with a new array later.
            ReadOnlyMemory<byte> slice = data.AsMemory(startPosition, validBytes);

            return ValueTask.FromResult((slice, batchOffsets));
        }
    }

    public long? GetFilePositionForOffset(long startOffset)
    {
        // Lock-free: ConcurrentDictionary.TryGetValue is thread-safe
        var batchOffset = FindBatchOffsetForRead(startOffset);
        if (batchOffset == null) return null;
        return _offsetIndex.TryGetValue(batchOffset.Value, out var position) ? position : null;
    }

    public long? FindOffsetByTimestamp(long targetTimestamp)
    {
        // Lock-free: _timestampsInOrder is append-only (writers hold _writeLock),
        // and we only read up to the current count (snapshot).
        if (_timestampsInOrder.Count == 0) return null;

        var count = _timestampsInOrder.Count; // snapshot
        int left = 0, right = count - 1, result = -1;

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

    public void DeleteFiles()
    {
        // No-op for memory segment - no files to delete
    }

    /// <summary>
    /// Get a direct memory slice. Zero-copy, lock-free access to segment data.
    /// </summary>
    public ReadOnlyMemory<byte> GetMemorySlice(long position, int length)
    {
        var wp = Volatile.Read(ref _writePosition);
        var data = _data;
        if (wp > data.Length) wp = data.Length;
        if (position < 0 || position + length > wp)
            return ReadOnlyMemory<byte>.Empty;
        return new ReadOnlyMemory<byte>(data, (int)position, length);
    }

    private void EnsureCapacity(int required)
    {
        if (required <= _data.Length)
            return;

        var newSize = Math.Max(_data.Length * 2, required);
        newSize = Math.Min(newSize, (int)_maxSegmentSize);

        var newData = new byte[newSize];
        _data.AsSpan(0, _writePosition).CopyTo(newData);
        _data = newData;
    }

    private long? FindBatchOffsetForRead(long requestedOffset)
    {
        if (_offsetsInOrder.Count == 0)
            return null;

        // Binary search for largest offset <= requestedOffset
        // _offsetsInOrder is always in ascending order (offsets increase monotonically)
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
        // Clear references to help GC
        _data = [];
        _offsetIndex.Clear();
        _offsetsInOrder.Clear();
        _timestampIndex.Clear();
        _timestampsInOrder.Clear();
        // Lock-free: no RWLS to dispose
    }
}
