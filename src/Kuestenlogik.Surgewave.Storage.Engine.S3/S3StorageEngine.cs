using System.Buffers.Binary;
using System.Collections.Concurrent;
using Amazon.S3;
using Amazon.S3.Model;
using Kuestenlogik.Surgewave.Storage;
using Kuestenlogik.Surgewave.Storage.Engine;

namespace Kuestenlogik.Surgewave.Storage.Engine.S3;

/// <summary>
/// S3-based primary storage engine for serverless/cloud-first deployments.
/// Stores message batches directly in S3 with local index caching.
/// Optimized for cloud-native workloads where durability is prioritized.
/// </summary>
public sealed class S3StorageEngine : ISurgewaveStorageEngine
{
    private readonly IAmazonS3 _s3Client;
    private readonly string _bucketName;
    private readonly string _prefix;
    private readonly long _baseOffset;
    private readonly long _maxSize;
    private readonly ISurgewaveBufferPool _bufferPool;
    private readonly int _batchFlushCount;

    // In-memory buffer for pending writes (flushed to S3 periodically)
    private readonly List<BatchRecord> _pendingBatches = new();
    private readonly object _writeLock = new();

    // Local index for fast offset lookups
    private readonly ConcurrentDictionary<long, string> _offsetToKey = new();
    private readonly List<long> _offsetsInOrder = new();

    // Timestamp index
    private readonly ConcurrentDictionary<long, long> _timestampIndex = new();
    private readonly List<long> _timestampsInOrder = new();

    // Local cache for recently read batches
    private readonly ConcurrentDictionary<string, byte[]> _batchCache = new();
    private readonly int _maxCacheSize;

    private long _currentOffset;
    private long _size;
    private long _maxTimestamp;
    private long? _firstOffset;
    private bool _disposed;

    public long BaseOffset => _baseOffset;
    public long CurrentOffset => Volatile.Read(ref _currentOffset);
    public long Size => Volatile.Read(ref _size);
    public bool IsFull => Size >= _maxSize;
    public DateTime CreatedAt { get; }
    public long MaxTimestamp => Volatile.Read(ref _maxTimestamp);
    public long? FirstOffset => _firstOffset;

    private sealed record BatchRecord(long Offset, long Timestamp, int RecordCount, byte[] Data);

    public S3StorageEngine(
        IAmazonS3 s3Client,
        string bucketName,
        string prefix,
        long baseOffset,
        long maxSize,
        bool createNew,
        ISurgewaveBufferPool? bufferPool = null,
        int batchFlushCount = 100,
        int maxCacheSize = 1000)
    {
        _s3Client = s3Client;
        _bucketName = bucketName;
        _prefix = prefix.TrimEnd('/');
        _baseOffset = baseOffset;
        _currentOffset = baseOffset;
        _maxSize = maxSize;
        _bufferPool = bufferPool ?? DefaultSurgewaveBufferPool.Shared;
        _batchFlushCount = batchFlushCount;
        _maxCacheSize = maxCacheSize;
        CreatedAt = DateTime.UtcNow;

        if (!createNew)
        {
            LoadIndexFromS3().GetAwaiter().GetResult();
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
            var record = new BatchRecord(batchBaseOffset, maxTimestamp, recordCount, recordBatch.ToArray());
            _pendingBatches.Add(record);

            // Update indexes
            var s3Key = GetS3Key(batchBaseOffset);
            _offsetToKey[batchBaseOffset] = s3Key;
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
            Volatile.Write(ref _size, _size + recordBatch.Length);

            // Flush to S3 when batch count reached
            if (_pendingBatches.Count >= _batchFlushCount)
            {
                FlushToS3Async(cancellationToken).GetAwaiter().GetResult();
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

        var batchOffset = FindBatchOffsetForRead(startOffset);
        if (batchOffset == null)
        {
            return EmptyStorageReadLease.Instance;
        }

        var batchOffsets = new List<int>();
        var batches = new List<byte[]>();
        var totalBytes = 0;

        // Find starting index
        int startIdx;
        lock (_writeLock)
        {
            startIdx = _offsetsInOrder.BinarySearch(batchOffset.Value);
            if (startIdx < 0) startIdx = ~startIdx;
        }

        for (int i = startIdx; i < _offsetsInOrder.Count; i++)
        {
            long offset;
            lock (_writeLock)
            {
                if (i >= _offsetsInOrder.Count) break;
                offset = _offsetsInOrder[i];
            }

            byte[]? data = null;

            // Check pending batches first
            lock (_writeLock)
            {
                var pendingIdx = _pendingBatches.FindIndex(b => b.Offset == offset);
                if (pendingIdx >= 0)
                {
                    data = _pendingBatches[pendingIdx].Data;
                }
            }

            // If not in pending, fetch from S3 (with cache)
            if (data == null)
            {
                data = await FetchBatchFromS3Async(offset, cancellationToken);
            }

            if (data == null) continue;

            if (totalBytes > 0 && totalBytes + data.Length > maxBytes)
                break;

            batchOffsets.Add(totalBytes);
            batches.Add(data);
            totalBytes += data.Length;
        }

        if (totalBytes == 0)
        {
            return EmptyStorageReadLease.Instance;
        }

        var combined = new byte[totalBytes];
        var position = 0;
        foreach (var batch in batches)
        {
            batch.CopyTo(combined, position);
            position += batch.Length;
        }

        var buffer = _bufferPool.Wrap(combined);
        var lease = new StorageReadLease(buffer, batchOffsets);

        return lease;
    }

    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await FlushToS3Async(cancellationToken);
    }

    public long? FindOffsetByTimestamp(long targetTimestamp)
    {
        lock (_writeLock)
        {
            if (_timestampsInOrder.Count == 0) return null;

            var idx = _timestampsInOrder.BinarySearch(targetTimestamp);
            if (idx < 0) idx = ~idx;
            if (idx >= _timestampsInOrder.Count) return null;

            return _timestampIndex.GetValueOrDefault(_timestampsInOrder[idx]);
        }
    }

    public void DeleteStorage()
    {
        // Delete all objects with prefix
        var listRequest = new ListObjectsV2Request
        {
            BucketName = _bucketName,
            Prefix = $"{_prefix}/{_baseOffset:D20}/"
        };

        var response = _s3Client.ListObjectsV2Async(listRequest).GetAwaiter().GetResult();
        foreach (var obj in response.S3Objects)
        {
            _s3Client.DeleteObjectAsync(_bucketName, obj.Key).GetAwaiter().GetResult();
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        lock (_writeLock)
        {
            if (_pendingBatches.Count > 0)
            {
                FlushToS3Async(CancellationToken.None).GetAwaiter().GetResult();
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        await FlushToS3Async(CancellationToken.None);
    }

    private async Task FlushToS3Async(CancellationToken cancellationToken)
    {
        List<BatchRecord> toFlush;
        lock (_writeLock)
        {
            if (_pendingBatches.Count == 0) return;
            toFlush = [.. _pendingBatches];
            _pendingBatches.Clear();
        }

        // Upload each batch to S3
        var uploadTasks = toFlush.Select(async batch =>
        {
            var key = GetS3Key(batch.Offset);
            using var stream = new MemoryStream(batch.Data);
            var request = new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = key,
                InputStream = stream,
                ContentType = "application/octet-stream"
            };

            // Add metadata
            request.Metadata.Add("offset", batch.Offset.ToString());
            request.Metadata.Add("timestamp", batch.Timestamp.ToString());
            request.Metadata.Add("record-count", batch.RecordCount.ToString());

            await _s3Client.PutObjectAsync(request, cancellationToken);

            // Add to cache
            AddToCache(key, batch.Data);
        });

        await Task.WhenAll(uploadTasks);

        // Save index
        await SaveIndexToS3Async(cancellationToken);
    }

    private async Task<byte[]?> FetchBatchFromS3Async(long offset, CancellationToken cancellationToken)
    {
        if (!_offsetToKey.TryGetValue(offset, out var key))
            return null;

        // Check cache first
        if (_batchCache.TryGetValue(key, out var cached))
            return cached;

        try
        {
            var response = await _s3Client.GetObjectAsync(_bucketName, key, cancellationToken);
            using var ms = new MemoryStream();
            await response.ResponseStream.CopyToAsync(ms, cancellationToken);
            var data = ms.ToArray();

            AddToCache(key, data);
            return data;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    private void AddToCache(string key, byte[] data)
    {
        // Simple LRU-ish eviction: remove oldest entries when over limit
        while (_batchCache.Count >= _maxCacheSize)
        {
            var keyToRemove = _batchCache.Keys.FirstOrDefault();
            if (keyToRemove != null)
            {
                _batchCache.TryRemove(keyToRemove, out _);
            }
        }

        _batchCache[key] = data;
    }

    private async Task LoadIndexFromS3()
    {
        var indexKey = $"{_prefix}/{_baseOffset:D20}/_index.json";
        try
        {
            var response = await _s3Client.GetObjectAsync(_bucketName, indexKey);
            using var reader = new StreamReader(response.ResponseStream);
            var json = await reader.ReadToEndAsync();
            var index = System.Text.Json.JsonSerializer.Deserialize<S3Index>(json);

            if (index != null)
            {
                foreach (var entry in index.Offsets)
                {
                    _offsetToKey[entry.Offset] = entry.Key;
                    _offsetsInOrder.Add(entry.Offset);

                    if (entry.Timestamp > 0)
                    {
                        _timestampIndex[entry.Timestamp] = entry.Offset;
                        _timestampsInOrder.Add(entry.Timestamp);
                        if (entry.Timestamp > _maxTimestamp)
                        {
                            _maxTimestamp = entry.Timestamp;
                        }
                    }

                    _firstOffset ??= entry.Offset;
                    _currentOffset = entry.Offset + entry.RecordCount;
                    _size += entry.Size;
                }
            }
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            // No index yet, starting fresh
        }
    }

    private async Task SaveIndexToS3Async(CancellationToken cancellationToken)
    {
        var index = new S3Index
        {
            Offsets = _offsetsInOrder.Select(o =>
            {
                _offsetToKey.TryGetValue(o, out var key);
                _timestampIndex.TryGetValue(o, out var ts);
                return new S3IndexEntry
                {
                    Offset = o,
                    Key = key ?? GetS3Key(o),
                    Timestamp = ts,
                    RecordCount = 0, // Will be populated from batch
                    Size = 0
                };
            }).ToList()
        };

        var json = System.Text.Json.JsonSerializer.Serialize(index);
        var indexKey = $"{_prefix}/{_baseOffset:D20}/_index.json";

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        var request = new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = indexKey,
            InputStream = stream,
            ContentType = "application/json"
        };

        await _s3Client.PutObjectAsync(request, cancellationToken);
    }

    private string GetS3Key(long offset) => $"{_prefix}/{_baseOffset:D20}/{offset:D20}.batch";

    private long? FindBatchOffsetForRead(long requestedOffset)
    {
        lock (_writeLock)
        {
            if (_offsetsInOrder.Count == 0) return null;

            var idx = _offsetsInOrder.BinarySearch(requestedOffset);
            if (idx < 0)
            {
                idx = ~idx - 1;
                if (idx < 0) idx = 0;
            }

            return idx < _offsetsInOrder.Count ? _offsetsInOrder[idx] : null;
        }
    }

    private static (long baseOffset, int recordCount, long maxTimestamp) ParseBatchHeader(ReadOnlySpan<byte> recordBatch)
    {
        var baseOffset = BinaryPrimitives.ReadInt64BigEndian(recordBatch);
        var maxTimestamp = BinaryPrimitives.ReadInt64BigEndian(recordBatch.Slice(35));
        var recordCount = BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(57));
        return (baseOffset, recordCount, maxTimestamp);
    }

    private sealed class S3Index
    {
        public List<S3IndexEntry> Offsets { get; set; } = [];
    }

    private sealed class S3IndexEntry
    {
        public long Offset { get; set; }
        public string Key { get; set; } = "";
        public long Timestamp { get; set; }
        public int RecordCount { get; set; }
        public long Size { get; set; }
    }
}
