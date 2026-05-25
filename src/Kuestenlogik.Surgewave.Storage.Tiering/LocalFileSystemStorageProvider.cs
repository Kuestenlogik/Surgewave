using System.Text.Json;

namespace Kuestenlogik.Surgewave.Storage.Tiering;

/// <summary>
/// Local filesystem implementation of remote storage provider.
/// Useful for testing and development without cloud dependencies.
/// </summary>
public sealed class LocalFileSystemStorageProvider : IRemoteStorageProvider
{
    private readonly string _basePath;
    private readonly SemaphoreSlim _semaphore = new(1, 1);

    public LocalFileSystemStorageProvider(string basePath)
    {
        _basePath = basePath;
        Directory.CreateDirectory(_basePath);
    }

    private string GetSegmentDirectory(string topic, int partition) =>
        Path.Combine(_basePath, topic, $"partition-{partition}");

    private string GetLogPath(string topic, int partition, long baseOffset) =>
        Path.Combine(GetSegmentDirectory(topic, partition), $"{baseOffset:D20}.log");

    private string GetIndexPath(string topic, int partition, long baseOffset) =>
        Path.Combine(GetSegmentDirectory(topic, partition), $"{baseOffset:D20}.index");

    private string GetTimeIndexPath(string topic, int partition, long baseOffset) =>
        Path.Combine(GetSegmentDirectory(topic, partition), $"{baseOffset:D20}.timeindex");

    private string GetMetadataPath(string topic, int partition, long baseOffset) =>
        Path.Combine(GetSegmentDirectory(topic, partition), $"{baseOffset:D20}.meta.json");

    public async Task UploadSegmentAsync(
        string topic,
        int partition,
        long baseOffset,
        ReadOnlyMemory<byte> logData,
        ReadOnlyMemory<byte> indexData,
        ReadOnlyMemory<byte> timeIndexData,
        CancellationToken cancellationToken = default)
    {
        var segmentDir = GetSegmentDirectory(topic, partition);
        Directory.CreateDirectory(segmentDir);

        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            // Write segment files
            await File.WriteAllBytesAsync(GetLogPath(topic, partition, baseOffset), logData.ToArray(), cancellationToken);
            await File.WriteAllBytesAsync(GetIndexPath(topic, partition, baseOffset), indexData.ToArray(), cancellationToken);
            await File.WriteAllBytesAsync(GetTimeIndexPath(topic, partition, baseOffset), timeIndexData.ToArray(), cancellationToken);

            // Write metadata
            var metadata = new SegmentMetadata
            {
                Topic = topic,
                Partition = partition,
                BaseOffset = baseOffset,
                Size = logData.Length,
                CreatedAt = DateTimeOffset.UtcNow,
                UploadedAt = DateTimeOffset.UtcNow
            };

            var metadataJson = JsonSerializer.Serialize(metadata);
            await File.WriteAllTextAsync(GetMetadataPath(topic, partition, baseOffset), metadataJson, cancellationToken);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public async Task<(byte[] LogData, byte[] IndexData, byte[] TimeIndexData)> DownloadSegmentAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default)
    {
        var logPath = GetLogPath(topic, partition, baseOffset);
        var indexPath = GetIndexPath(topic, partition, baseOffset);
        var timeIndexPath = GetTimeIndexPath(topic, partition, baseOffset);

        if (!File.Exists(logPath))
        {
            throw new FileNotFoundException($"Remote segment not found: {topic}/{partition}/{baseOffset}");
        }

        var logData = await File.ReadAllBytesAsync(logPath, cancellationToken);
        var indexData = File.Exists(indexPath) ? await File.ReadAllBytesAsync(indexPath, cancellationToken) : [];
        var timeIndexData = File.Exists(timeIndexPath) ? await File.ReadAllBytesAsync(timeIndexPath, cancellationToken) : [];

        return (logData, indexData, timeIndexData);
    }

    public Task DeleteSegmentAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default)
    {
        var logPath = GetLogPath(topic, partition, baseOffset);
        var indexPath = GetIndexPath(topic, partition, baseOffset);
        var timeIndexPath = GetTimeIndexPath(topic, partition, baseOffset);
        var metadataPath = GetMetadataPath(topic, partition, baseOffset);

        if (File.Exists(logPath)) File.Delete(logPath);
        if (File.Exists(indexPath)) File.Delete(indexPath);
        if (File.Exists(timeIndexPath)) File.Delete(timeIndexPath);
        if (File.Exists(metadataPath)) File.Delete(metadataPath);

        return Task.CompletedTask;
    }

    public async Task<IReadOnlyList<RemoteSegmentInfo>> ListSegmentsAsync(
        string topic,
        int partition,
        CancellationToken cancellationToken = default)
    {
        var segmentDir = GetSegmentDirectory(topic, partition);
        if (!Directory.Exists(segmentDir))
        {
            return [];
        }

        var segments = new List<RemoteSegmentInfo>();

        foreach (var metaFile in Directory.GetFiles(segmentDir, "*.meta.json"))
        {
            var json = await File.ReadAllTextAsync(metaFile, cancellationToken);
            var metadata = JsonSerializer.Deserialize<SegmentMetadata>(json);
            if (metadata != null)
            {
                segments.Add(new RemoteSegmentInfo(
                    metadata.Topic,
                    metadata.Partition,
                    metadata.BaseOffset,
                    metadata.Size,
                    metadata.CreatedAt,
                    metadata.UploadedAt));
            }
        }

        return segments.OrderBy(s => s.BaseOffset).ToList();
    }

    public Task<bool> SegmentExistsAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default)
    {
        var logPath = GetLogPath(topic, partition, baseOffset);
        return Task.FromResult(File.Exists(logPath));
    }

    public async Task<RemoteSegmentInfo?> GetSegmentInfoAsync(
        string topic,
        int partition,
        long baseOffset,
        CancellationToken cancellationToken = default)
    {
        var metadataPath = GetMetadataPath(topic, partition, baseOffset);
        if (!File.Exists(metadataPath))
        {
            return null;
        }

        var json = await File.ReadAllTextAsync(metadataPath, cancellationToken);
        var metadata = JsonSerializer.Deserialize<SegmentMetadata>(json);
        if (metadata == null)
        {
            return null;
        }

        return new RemoteSegmentInfo(
            metadata.Topic,
            metadata.Partition,
            metadata.BaseOffset,
            metadata.Size,
            metadata.CreatedAt,
            metadata.UploadedAt);
    }

    public async Task<Stream> FetchLogSegmentAsync(
        string topic,
        int partition,
        long baseOffset,
        int startPosition,
        int? endPosition = null,
        CancellationToken cancellationToken = default)
    {
        var logPath = GetLogPath(topic, partition, baseOffset);
        if (!File.Exists(logPath))
        {
            throw new FileNotFoundException($"Remote segment not found: {topic}/{partition}/{baseOffset}");
        }

        var fileStream = new FileStream(logPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
        var fileLength = fileStream.Length;

        // Validate positions
        if (startPosition >= fileLength)
        {
            fileStream.Dispose();
            return new MemoryStream([]);
        }

        // Calculate how much to read
        var actualEnd = endPosition.HasValue ? Math.Min(endPosition.Value, (int)fileLength) : (int)fileLength;
        var bytesToRead = actualEnd - startPosition;

        if (bytesToRead <= 0)
        {
            fileStream.Dispose();
            return new MemoryStream([]);
        }

        // Seek and read the range
        fileStream.Seek(startPosition, SeekOrigin.Begin);
        var buffer = new byte[bytesToRead];
        var bytesRead = await fileStream.ReadAsync(buffer.AsMemory(0, bytesToRead), cancellationToken);
        fileStream.Dispose();

        return new MemoryStream(buffer, 0, bytesRead);
    }

    public Task<Stream> FetchIndexAsync(
        string topic,
        int partition,
        long baseOffset,
        RemoteIndexType indexType,
        CancellationToken cancellationToken = default)
    {
        var indexPath = indexType switch
        {
            RemoteIndexType.Offset => GetIndexPath(topic, partition, baseOffset),
            RemoteIndexType.Timestamp => GetTimeIndexPath(topic, partition, baseOffset),
            RemoteIndexType.Transaction => Path.Combine(GetSegmentDirectory(topic, partition), $"{baseOffset:D20}.txnindex"),
            RemoteIndexType.ProducerSnapshot => Path.Combine(GetSegmentDirectory(topic, partition), $"{baseOffset:D20}.snapshot"),
            RemoteIndexType.LeaderEpoch => Path.Combine(GetSegmentDirectory(topic, partition), $"{baseOffset:D20}.leader-epoch"),
            _ => throw new ArgumentOutOfRangeException(nameof(indexType))
        };

        if (!File.Exists(indexPath))
        {
            return Task.FromResult<Stream>(new MemoryStream([]));
        }

        // Return a read-only file stream
        Stream stream = new FileStream(indexPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
        return Task.FromResult(stream);
    }

    public async Task<CustomMetadata?> CopyLogSegmentDataAsync(
        Guid segmentId,
        string topic,
        int partition,
        long baseOffset,
        LogSegmentData segmentData,
        CancellationToken cancellationToken = default)
    {
        var segmentDir = GetSegmentDirectory(topic, partition);
        Directory.CreateDirectory(segmentDir);

        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            // Copy all segment files
            if (File.Exists(segmentData.LogPath))
            {
                File.Copy(segmentData.LogPath, GetLogPath(topic, partition, baseOffset), overwrite: true);
            }
            if (File.Exists(segmentData.OffsetIndexPath))
            {
                File.Copy(segmentData.OffsetIndexPath, GetIndexPath(topic, partition, baseOffset), overwrite: true);
            }
            if (File.Exists(segmentData.TimeIndexPath))
            {
                File.Copy(segmentData.TimeIndexPath, GetTimeIndexPath(topic, partition, baseOffset), overwrite: true);
            }
            if (segmentData.TransactionIndexPath != null && File.Exists(segmentData.TransactionIndexPath))
            {
                File.Copy(segmentData.TransactionIndexPath, Path.Combine(segmentDir, $"{baseOffset:D20}.txnindex"), overwrite: true);
            }
            if (segmentData.ProducerSnapshotPath != null && File.Exists(segmentData.ProducerSnapshotPath))
            {
                File.Copy(segmentData.ProducerSnapshotPath, Path.Combine(segmentDir, $"{baseOffset:D20}.snapshot"), overwrite: true);
            }
            if (segmentData.LeaderEpochIndex != null && segmentData.LeaderEpochIndex.Length > 0)
            {
                await File.WriteAllBytesAsync(Path.Combine(segmentDir, $"{baseOffset:D20}.leader-epoch"), segmentData.LeaderEpochIndex, cancellationToken);
            }

            // Write metadata with segment ID for tracking
            var logSize = File.Exists(segmentData.LogPath) ? new FileInfo(segmentData.LogPath).Length : 0;
            var metadata = new SegmentMetadata
            {
                Topic = topic,
                Partition = partition,
                BaseOffset = baseOffset,
                Size = logSize,
                CreatedAt = DateTimeOffset.UtcNow,
                UploadedAt = DateTimeOffset.UtcNow
            };

            var metadataJson = JsonSerializer.Serialize(metadata);
            await File.WriteAllTextAsync(GetMetadataPath(topic, partition, baseOffset), metadataJson, cancellationToken);

            // Return segment ID as custom metadata for idempotency tracking
            return new CustomMetadata(segmentId.ToByteArray());
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public ValueTask DisposeAsync()
    {
        _semaphore.Dispose();
        return ValueTask.CompletedTask;
    }

    private sealed class SegmentMetadata
    {
        public string Topic { get; set; } = "";
        public int Partition { get; set; }
        public long BaseOffset { get; set; }
        public long Size { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset UploadedAt { get; set; }
    }
}
