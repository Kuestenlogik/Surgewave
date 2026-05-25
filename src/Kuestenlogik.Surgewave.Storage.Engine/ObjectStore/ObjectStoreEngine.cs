using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Storage;

namespace Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;

/// <summary>
/// Storage engine that writes directly to object storage (S3/Azure/GCP).
/// Local write buffer is flushed to remote on segment roll or flush.
/// Reads use a local LRU cache backed by remote storage.
/// Enables zero-disk stateless broker instances.
/// </summary>
public sealed class ObjectStoreEngine : ISurgewaveStorageEngine
{
    private readonly IObjectStoreProvider _storeProvider;
    private readonly ObjectStoreConfig _config;
    private readonly string _topic;
    private readonly int _partition;
    private readonly long _baseOffset;
    private readonly long _maxSize;
    private readonly ISurgewaveBufferPool _bufferPool;
    private readonly WriteBuffer _writeBuffer;
    private readonly ReadCache _readCache;

    private long _currentOffset;
    private long _totalSize;
    private long _maxTimestamp;
    private long? _firstOffset;
    private bool _disposed;

    private readonly object _stateLock = new();

    public long BaseOffset => _baseOffset;
    public long CurrentOffset => Volatile.Read(ref _currentOffset);
    public long Size => Volatile.Read(ref _totalSize);
    public bool IsFull => Size >= _maxSize;
    public DateTime CreatedAt { get; }
    public long MaxTimestamp => Volatile.Read(ref _maxTimestamp);
    public long? FirstOffset => _firstOffset;

    public ObjectStoreEngine(
        IObjectStoreProvider storeProvider,
        ObjectStoreConfig config,
        string topic,
        int partition,
        long baseOffset,
        long maxSize,
        ISurgewaveBufferPool? bufferPool = null)
    {
        _storeProvider = storeProvider ?? throw new ArgumentNullException(nameof(storeProvider));
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _topic = topic;
        _partition = partition;
        _baseOffset = baseOffset;
        _currentOffset = baseOffset;
        _maxSize = maxSize;
        _bufferPool = bufferPool ?? DefaultSurgewaveBufferPool.Shared;
        CreatedAt = DateTime.UtcNow;

        _writeBuffer = new WriteBuffer(
            config.WriteBufferSizeBytes,
            storeProvider,
            topic,
            partition,
            baseOffset);

        _readCache = new ReadCache(
            config.CacheDirectory,
            config.ReadCacheSizeBytes);
    }

    public ValueTask<(long baseOffset, int recordCount)> AppendAsync(
        ReadOnlySpan<byte> recordBatch,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var (batchBaseOffset, recordCount, maxTimestamp) = ParseBatchHeader(recordBatch);

        // Append to write buffer
        _writeBuffer.Append(recordBatch);

        // Update engine-level state
        lock (_stateLock)
        {
            _firstOffset ??= batchBaseOffset;
            Volatile.Write(ref _currentOffset, batchBaseOffset + recordCount);
            Volatile.Write(ref _totalSize, _totalSize + recordBatch.Length);

            if (maxTimestamp > _maxTimestamp)
            {
                Volatile.Write(ref _maxTimestamp, maxTimestamp);
            }
        }

        return ValueTask.FromResult((batchBaseOffset, recordCount));
    }

    public ValueTask<(long baseOffset, int recordCount)> AppendAsync(
        ISurgewaveBuffer recordBatch,
        CancellationToken cancellationToken = default)
    {
        return AppendAsync(recordBatch.Span, cancellationToken);
    }

    public async ValueTask<IStorageReadLease> ReadAsync(
        long startOffset,
        int maxBytes,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (startOffset < _baseOffset || startOffset >= CurrentOffset)
        {
            return EmptyStorageReadLease.Instance;
        }

        // Try reading from the write buffer first (hot data)
        var bufferData = _writeBuffer.ReadFromBuffer(startOffset, maxBytes);
        if (!bufferData.IsEmpty)
        {
            return CreateLease(bufferData);
        }

        // Fall back to remote storage via read cache
        var cacheKey = $"{_topic}/{_partition}/{startOffset}";
        var remoteData = await _readCache.GetOrFetchAsync(cacheKey, async () =>
        {
            return await _storeProvider.DownloadAsync(
                _topic, _partition, startOffset, cancellationToken);
        });

        if (remoteData == null || remoteData.Length == 0)
        {
            return EmptyStorageReadLease.Instance;
        }

        // Trim to maxBytes
        var length = Math.Min(remoteData.Length, maxBytes);
        return CreateLease(remoteData.AsMemory(0, length));
    }

    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _writeBuffer.FlushAsync(cancellationToken);
    }

    public long? FindOffsetByTimestamp(long targetTimestamp)
    {
        // Check write buffer first
        return _writeBuffer.FindOffsetByTimestamp(targetTimestamp);
    }

    public void DeleteStorage()
    {
        // Delete remote storage data
        _storeProvider.DeleteAsync(_topic, _partition, _baseOffset)
            .GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        // Flush remaining data synchronously on dispose
        try
        {
            _writeBuffer.FlushAsync(CancellationToken.None).GetAwaiter().GetResult();
        }
        catch
        {
            // Best effort flush on dispose
        }

        _writeBuffer.Dispose();
        _readCache.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        try
        {
            await _writeBuffer.FlushAsync(CancellationToken.None);
        }
        catch
        {
            // Best effort flush on dispose
        }

        _writeBuffer.Dispose();
        _readCache.Dispose();
    }

    private IStorageReadLease CreateLease(ReadOnlyMemory<byte> data)
    {
        if (data.IsEmpty)
            return EmptyStorageReadLease.Instance;

        // Parse batch boundaries from the data
        var batchOffsets = new List<int>();
        var position = 0;
        var span = data.Span;

        while (position + 12 <= span.Length)
        {
            var batchLength = BinaryPrimitives.ReadInt32BigEndian(span.Slice(position + 8, 4));
            var totalBatchSize = 12 + batchLength;

            if (position + totalBatchSize > span.Length)
                break;

            batchOffsets.Add(position);
            position += totalBatchSize;
        }

        if (batchOffsets.Count == 0)
            return EmptyStorageReadLease.Instance;

        var buffer = _bufferPool.Wrap(data.Slice(0, position));
#pragma warning disable CA2000 // Ownership transferred to StorageReadLease
        return new StorageReadLease(buffer, batchOffsets);
#pragma warning restore CA2000
    }

    private static (long baseOffset, int recordCount, long maxTimestamp) ParseBatchHeader(ReadOnlySpan<byte> recordBatch)
    {
        var baseOffset = BinaryPrimitives.ReadInt64BigEndian(recordBatch);
        var maxTimestamp = BinaryPrimitives.ReadInt64BigEndian(recordBatch.Slice(35));
        var recordCount = BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(57));
        return (baseOffset, recordCount, maxTimestamp);
    }
}
