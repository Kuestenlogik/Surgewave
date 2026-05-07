using System.Buffers.Binary;

namespace Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;

/// <summary>
/// In-memory write buffer that accumulates record batches and flushes to remote storage.
/// Thread-safe for concurrent appends via SpinLock for minimal contention.
/// </summary>
internal sealed class WriteBuffer : IDisposable
{
    private byte[] _data;
    private int _writePosition;
    private readonly long _maxSizeBytes;
    private readonly IObjectStoreProvider _storeProvider;
    private readonly string _topic;
    private readonly int _partition;

    // Offset index: maps batch baseOffset -> buffer position
    private readonly Dictionary<long, int> _offsetIndex = new();
    private readonly List<long> _offsetsInOrder = [];

    // Timestamp index
    private readonly Dictionary<long, long> _timestampIndex = new();
    private readonly List<long> _timestampsInOrder = [];

    private long _currentOffset;
    private readonly long _baseOffset;
    private long _maxTimestamp;
    private long? _firstOffset;
    private long _flushedSegmentCount;
    private bool _disposed;

    private SpinLock _spinLock = new(enableThreadOwnerTracking: false);

    /// <summary>
    /// Whether the buffer has reached its maximum size.
    /// </summary>
    public bool IsFull => _writePosition >= _maxSizeBytes;

    /// <summary>
    /// Current write offset (next offset to be written).
    /// </summary>
    public long CurrentOffset => Volatile.Read(ref _currentOffset);

    /// <summary>
    /// Current size of buffered data in bytes.
    /// </summary>
    public int CurrentSize => _writePosition;

    /// <summary>
    /// Maximum timestamp across all buffered batches.
    /// </summary>
    public long MaxTimestamp => Volatile.Read(ref _maxTimestamp);

    /// <summary>
    /// First offset in the buffer, or null if empty.
    /// </summary>
    public long? FirstOffset => _firstOffset;

    public WriteBuffer(
        long maxSizeBytes,
        IObjectStoreProvider storeProvider,
        string topic,
        int partition,
        long baseOffset)
    {
        _maxSizeBytes = maxSizeBytes;
        _storeProvider = storeProvider;
        _topic = topic;
        _partition = partition;
        _baseOffset = baseOffset;
        _currentOffset = baseOffset;
        _data = new byte[Math.Min(1024 * 1024, (int)Math.Min(maxSizeBytes, int.MaxValue))];
    }

    /// <summary>
    /// Append a record batch to the buffer.
    /// </summary>
    /// <returns>Tuple of (baseOffset, recordCount) for the appended batch.</returns>
    public (long baseOffset, int recordCount) Append(ReadOnlySpan<byte> data)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var (batchBaseOffset, recordCount, maxTimestamp) = ParseBatchHeader(data);

        var lockTaken = false;
        try
        {
            _spinLock.Enter(ref lockTaken);

            EnsureCapacity(_writePosition + data.Length);

            var position = _writePosition;
            data.CopyTo(_data.AsSpan(_writePosition));
            _writePosition += data.Length;

            // Update indexes
            _offsetIndex[batchBaseOffset] = position;
            _offsetsInOrder.Add(batchBaseOffset);

            if (maxTimestamp > 0)
            {
                _timestampIndex[maxTimestamp] = batchBaseOffset;
                _timestampsInOrder.Add(maxTimestamp);
                if (maxTimestamp > _maxTimestamp)
                {
                    Volatile.Write(ref _maxTimestamp, maxTimestamp);
                }
            }

            _firstOffset ??= batchBaseOffset;
            Volatile.Write(ref _currentOffset, batchBaseOffset + recordCount);

            return (batchBaseOffset, recordCount);
        }
        finally
        {
            if (lockTaken) _spinLock.Exit();
        }
    }

    /// <summary>
    /// Flush buffer contents to remote storage and clear the buffer.
    /// </summary>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        byte[] dataToFlush;
        int bytesToFlush;
        long flushOffset;

        var lockTaken = false;
        try
        {
            _spinLock.Enter(ref lockTaken);

            if (_writePosition == 0) return;

            bytesToFlush = _writePosition;
            dataToFlush = new byte[bytesToFlush];
            _data.AsSpan(0, bytesToFlush).CopyTo(dataToFlush);

            flushOffset = _firstOffset ?? _baseOffset;

            // Reset buffer state
            _writePosition = 0;
            _offsetIndex.Clear();
            _offsetsInOrder.Clear();
            _timestampIndex.Clear();
            _timestampsInOrder.Clear();
            _firstOffset = null;
            _flushedSegmentCount++;
        }
        finally
        {
            if (lockTaken) _spinLock.Exit();
        }

        // Upload to remote storage outside the lock
        await _storeProvider.UploadAsync(
            _topic,
            _partition,
            flushOffset,
            dataToFlush.AsMemory(),
            cancellationToken);
    }

    /// <summary>
    /// Read data from the current buffer starting at the given offset.
    /// </summary>
    /// <returns>Read-only memory containing the requested data, or empty if offset not found.</returns>
    public ReadOnlyMemory<byte> ReadFromBuffer(long startOffset, int maxBytes)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var lockTaken = false;
        try
        {
            _spinLock.Enter(ref lockTaken);

            if (_offsetsInOrder.Count == 0 || _writePosition == 0)
                return ReadOnlyMemory<byte>.Empty;

            // Find the batch containing or after the requested offset
            var batchOffset = FindBatchOffsetForRead(startOffset);
            if (batchOffset == null || !_offsetIndex.TryGetValue(batchOffset.Value, out var startPosition))
                return ReadOnlyMemory<byte>.Empty;

            var endPosition = Math.Min(startPosition + maxBytes, _writePosition);
            var length = endPosition - startPosition;

            if (length <= 0)
                return ReadOnlyMemory<byte>.Empty;

            // Copy data out to avoid holding lock dependencies
            var result = new byte[length];
            _data.AsSpan(startPosition, length).CopyTo(result);
            return result;
        }
        finally
        {
            if (lockTaken) _spinLock.Exit();
        }
    }

    /// <summary>
    /// Find the offset of the first batch with timestamp >= targetTimestamp.
    /// </summary>
    public long? FindOffsetByTimestamp(long targetTimestamp)
    {
        var lockTaken = false;
        try
        {
            _spinLock.Enter(ref lockTaken);

            if (_timestampsInOrder.Count == 0) return null;

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
                return batchOffset;

            return null;
        }
        finally
        {
            if (lockTaken) _spinLock.Exit();
        }
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

    private void EnsureCapacity(int required)
    {
        if (required <= _data.Length)
            return;

        var newSize = Math.Max(_data.Length * 2, required);
        newSize = Math.Min(newSize, (int)Math.Min(_maxSizeBytes * 2, int.MaxValue));

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
