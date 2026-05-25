using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage;

namespace Kuestenlogik.Surgewave.Storage.Engine.Memory;

/// <summary>
/// In-memory storage engine with zero-copy read support.
/// Uses pooled buffers for efficient memory management.
/// </summary>
public sealed class MemoryStorageEngine : ISurgewaveStorageEngine
{
    private byte[] _data;
    private int _writePosition;
    private readonly long _baseOffset;
    private readonly long _maxSize;
    private long _currentOffset;
    private bool _disposed;

    private readonly ISurgewaveBufferPool _bufferPool;

    // Offset index: maps batch baseOffset -> buffer position
    private readonly Dictionary<long, int> _offsetIndex = new();
    private readonly List<long> _offsetsInOrder = new();

    // Timestamp index: maps maxTimestamp -> batch baseOffset
    private readonly Dictionary<long, long> _timestampIndex = new();
    private readonly List<long> _timestampsInOrder = new();

    // SpinLock for low-contention, short critical sections
    private SpinLock _spinLock = new(enableThreadOwnerTracking: false);

    public long BaseOffset => _baseOffset;
    public long CurrentOffset => _currentOffset;
    public long Size => _writePosition;
    public bool IsFull => Size >= _maxSize;
    public DateTime CreatedAt { get; }
    public long MaxTimestamp { get; private set; }
    public long? FirstOffset => _offsetsInOrder.Count > 0 ? _offsetsInOrder[0] : null;

    public MemoryStorageEngine(
        long baseOffset,
        long maxSize = 1024L * 1024 * 1024,
        ISurgewaveBufferPool? bufferPool = null)
    {
        _baseOffset = baseOffset;
        _currentOffset = baseOffset;
        _maxSize = maxSize;
        _bufferPool = bufferPool ?? DefaultSurgewaveBufferPool.Shared;
        _data = new byte[Math.Min(1024 * 1024, (int)maxSize)];
        CreatedAt = DateTime.UtcNow;
    }

    public ValueTask<(long baseOffset, int recordCount)> AppendAsync(
        ReadOnlySpan<byte> recordBatch,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var lockTaken = false;
        try
        {
            _spinLock.Enter(ref lockTaken);

            var (batchBaseOffset, recordCount, maxTimestamp) = ParseBatchHeader(recordBatch);

            EnsureCapacity(_writePosition + recordBatch.Length);

            var position = _writePosition;
            recordBatch.CopyTo(_data.AsSpan(_writePosition));
            _writePosition += recordBatch.Length;

            UpdateIndexes(batchBaseOffset, position, maxTimestamp, recordCount);

            return ValueTask.FromResult((batchBaseOffset, recordCount));
        }
        finally
        {
            if (lockTaken) _spinLock.Exit();
        }
    }

    public ValueTask<(long baseOffset, int recordCount)> AppendAsync(
        ISurgewaveBuffer recordBatch,
        CancellationToken cancellationToken = default)
    {
        // For memory engine, we always copy (buffer might be reused)
        return AppendAsync(recordBatch.Span, cancellationToken);
    }

    public ValueTask<IStorageReadLease> ReadAsync(
        long startOffset,
        int maxBytes,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var lockTaken = false;
        try
        {
            _spinLock.Enter(ref lockTaken);

            if (startOffset < _baseOffset || startOffset >= _currentOffset)
            {
                return ValueTask.FromResult<IStorageReadLease>(EmptyStorageReadLease.Instance);
            }

            var batchOffset = FindBatchOffsetForRead(startOffset);
            if (batchOffset == null || !_offsetIndex.TryGetValue(batchOffset.Value, out var startPosition))
            {
                return ValueTask.FromResult<IStorageReadLease>(EmptyStorageReadLease.Instance);
            }

            var batchOffsets = new List<int>();
            var position = startPosition;
            var validBytes = 0;

            while (position + 12 <= _writePosition)
            {
                var batchLength = BinaryPrimitives.ReadInt32BigEndian(_data.AsSpan(position + 8, 4));
                var totalBatchSize = 12 + batchLength;

                if (position + totalBatchSize > _writePosition)
                    break;

                if (validBytes > 0 && validBytes + totalBatchSize > maxBytes)
                    break;

                batchOffsets.Add(validBytes);
                validBytes += totalBatchSize;
                position += totalBatchSize;
            }

            if (validBytes == 0)
            {
                return ValueTask.FromResult<IStorageReadLease>(EmptyStorageReadLease.Instance);
            }

            // Zero-copy: wrap the existing memory instead of copying
            // IMPORTANT: This is safe because we hold the lock and the caller
            // must dispose the lease before we can modify the data
            var memory = new ReadOnlyMemory<byte>(_data, startPosition, validBytes);
            var buffer = _bufferPool.Wrap(memory);

            // StorageReadLease takes ownership of buffer - it will dispose it
#pragma warning disable CA2000 // Ownership transferred to StorageReadLease
            var lease = new StorageReadLease(buffer, batchOffsets);
#pragma warning restore CA2000

            return ValueTask.FromResult<IStorageReadLease>(lease);
        }
        finally
        {
            if (lockTaken) _spinLock.Exit();
        }
    }

    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        // No-op for memory storage
        return ValueTask.CompletedTask;
    }

    public long? FindOffsetByTimestamp(long targetTimestamp)
    {
        var lockTaken = false;
        try
        {
            _spinLock.Enter(ref lockTaken);

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
            if (lockTaken) _spinLock.Exit();
        }
    }

    public void DeleteStorage()
    {
        // No-op for memory storage
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _data = [];
        _offsetIndex.Clear();
        _offsetsInOrder.Clear();
        _timestampIndex.Clear();
        _timestampsInOrder.Clear();
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    private void UpdateIndexes(long batchBaseOffset, int position, long maxTimestamp, int recordCount)
    {
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

        _currentOffset = batchBaseOffset + recordCount;
    }

    private void EnsureCapacity(int required)
    {
        if (required <= _data.Length)
            return;

        var newSize = Math.Max(_data.Length * 2, required);
        newSize = Math.Min(newSize, (int)_maxSize);

        var newData = new byte[newSize];
        _data.AsSpan(0, _writePosition).CopyTo(newData);
        _data = newData;
    }

    private long? FindBatchOffsetForRead(long requestedOffset)
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

    private static (long baseOffset, int recordCount, long maxTimestamp) ParseBatchHeader(ReadOnlySpan<byte> recordBatch)
    {
        var baseOffset = BinaryPrimitives.ReadInt64BigEndian(recordBatch);
        var maxTimestamp = BinaryPrimitives.ReadInt64BigEndian(recordBatch.Slice(35));
        var recordCount = BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(57));
        return (baseOffset, recordCount, maxTimestamp);
    }
}

/// <summary>
/// Factory for creating memory storage engines.
/// </summary>
public sealed class MemoryStorageEngineFactory : ISurgewaveStorageEngineFactory
{
    private readonly ISurgewaveBufferPool _bufferPool;
    private readonly long _defaultMaxSize;

    public MemoryStorageEngineFactory(
        ISurgewaveBufferPool? bufferPool = null,
        long defaultMaxSize = 1024L * 1024 * 1024)
    {
        _bufferPool = bufferPool ?? DefaultSurgewaveBufferPool.Shared;
        _defaultMaxSize = defaultMaxSize;
    }

    public ISurgewaveStorageEngine Create(string directory, long baseOffset, long maxSize)
    {
        return new MemoryStorageEngine(baseOffset, maxSize, _bufferPool);
    }

    public ISurgewaveStorageEngine Open(string directory, long baseOffset)
    {
        // Memory storage doesn't persist, so "open" creates a new empty segment
        return new MemoryStorageEngine(baseOffset, _defaultMaxSize, _bufferPool);
    }
}
