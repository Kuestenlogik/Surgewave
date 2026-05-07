using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Storage.Tiering;

/// <summary>
/// Factory for creating remote storage providers.
/// Register custom providers using RegisterProvider.
/// </summary>
public static class RemoteStorageProviderFactory
{
    private static readonly Dictionary<string, Func<TieredStorageConfig, IRemoteStorageProvider>> s_providers = new(StringComparer.OrdinalIgnoreCase)
    {
        ["local"] = config => new LocalFileSystemStorageProvider(config.LocalPath)
    };

    /// <summary>
    /// Register a custom storage provider factory.
    /// </summary>
    /// <param name="providerName">Provider name (e.g., "azure", "s3", "gcp")</param>
    /// <param name="factory">Factory function to create the provider</param>
    public static void RegisterProvider(string providerName, Func<TieredStorageConfig, IRemoteStorageProvider> factory)
    {
        s_providers[providerName] = factory;
    }

    /// <summary>
    /// Create a storage provider based on configuration.
    /// </summary>
    public static IRemoteStorageProvider Create(TieredStorageConfig config)
    {
        if (s_providers.TryGetValue(config.Provider, out var factory))
        {
            return factory(config);
        }

        throw new ArgumentException($"Unknown storage provider: '{config.Provider}'. Available providers: {string.Join(", ", s_providers.Keys)}");
    }
}

/// <summary>
/// Manages tiered storage for log segments.
/// Handles uploading segments to remote storage, downloading when needed,
/// and managing the local cache.
/// </summary>
public sealed class TieredStorageManager : IAsyncDisposable
{
    private readonly TieredStorageConfig _config;
    private readonly IRemoteStorageProvider _remoteStorage;
    private readonly string _dataDirectory;
    private readonly string _cacheDirectory;
    private readonly ConcurrentDictionary<TopicPartition, RemoteLogMetadata> _metadata = new();
    private readonly SemaphoreSlim _cacheSemaphore = new(1, 1);
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _backgroundTask;
    private long _currentCacheSize;

    /// <summary>
    /// Create a TieredStorageManager with a custom storage provider.
    /// </summary>
    public TieredStorageManager(TieredStorageConfig config, string dataDirectory, IRemoteStorageProvider remoteStorage)
    {
        _config = config;
        _dataDirectory = dataDirectory;
        _cacheDirectory = Path.GetFullPath(config.LocalCachePath);
        Directory.CreateDirectory(_cacheDirectory);

        _remoteStorage = remoteStorage;

        // Start background tiering task
        _backgroundTask = Task.Run(BackgroundTieringLoopAsync);
    }

    /// <summary>
    /// Create a TieredStorageManager using the provider factory.
    /// </summary>
#pragma warning disable CA2000 // Dispose objects before losing scope - factory creates provider that is stored and disposed with manager
    public TieredStorageManager(TieredStorageConfig config, string dataDirectory)
        : this(config, dataDirectory, RemoteStorageProviderFactory.Create(config))
    {
    }
#pragma warning restore CA2000

    /// <summary>
    /// Get or create metadata tracker for a topic-partition
    /// </summary>
    private RemoteLogMetadata GetMetadata(TopicPartition tp)
    {
        return _metadata.GetOrAdd(tp, _ =>
        {
            var metadataPath = Path.Combine(_dataDirectory, tp.Topic, $"partition-{tp.Partition}", ".remote-metadata.json");
            return new RemoteLogMetadata(metadataPath);
        });
    }

    /// <summary>
    /// Upload a segment to remote storage
    /// </summary>
    public async Task UploadSegmentAsync(
        TopicPartition tp,
        IFileLogSegment segment,
        CancellationToken cancellationToken = default)
    {
        var baseOffset = segment.BaseOffset;
        var segmentDir = Path.Combine(_dataDirectory, tp.Topic, $"partition-{tp.Partition}");

        var logPath = Path.Combine(segmentDir, $"{baseOffset:D20}.log");
        var indexPath = Path.Combine(segmentDir, $"{baseOffset:D20}.index");
        var timeIndexPath = Path.Combine(segmentDir, $"{baseOffset:D20}.timeindex");

        if (!File.Exists(logPath))
        {
            return;
        }

        var logData = await File.ReadAllBytesAsync(logPath, cancellationToken);
        var indexData = File.Exists(indexPath) ? await File.ReadAllBytesAsync(indexPath, cancellationToken) : [];
        var timeIndexData = File.Exists(timeIndexPath) ? await File.ReadAllBytesAsync(timeIndexPath, cancellationToken) : [];

        await _remoteStorage.UploadSegmentAsync(
            tp.Topic,
            tp.Partition,
            baseOffset,
            logData,
            indexData,
            timeIndexData,
            cancellationToken);

        var metadata = GetMetadata(tp);
        metadata.MarkUploaded(baseOffset, logData.Length, DateTimeOffset.UtcNow);
    }

    /// <summary>
    /// Check if a segment exists in remote storage
    /// </summary>
    public bool IsSegmentRemote(TopicPartition tp, long baseOffset)
    {
        var metadata = GetMetadata(tp);
        return metadata.IsRemote(baseOffset);
    }

    /// <summary>
    /// Check if a segment is remote-only (no local copy)
    /// </summary>
    public bool IsSegmentRemoteOnly(TopicPartition tp, long baseOffset)
    {
        var metadata = GetMetadata(tp);
        return metadata.IsRemoteOnly(baseOffset);
    }

    /// <summary>
    /// Download a segment from remote storage to local cache.
    /// Returns the path to the cached segment files.
    /// </summary>
    public async Task<string> DownloadSegmentToCacheAsync(
        TopicPartition tp,
        long baseOffset,
        CancellationToken cancellationToken = default)
    {
        var metadata = GetMetadata(tp);

        // Check if already cached
        var existingCachePath = metadata.GetCachePath(baseOffset);
        if (existingCachePath != null && Directory.Exists(existingCachePath))
        {
            return existingCachePath;
        }

        // Create cache directory for this segment
        var cacheDir = Path.Combine(_cacheDirectory, tp.Topic, $"partition-{tp.Partition}", baseOffset.ToString("D20"));
        Directory.CreateDirectory(cacheDir);

        // Download from remote
        var (logData, indexData, timeIndexData) = await _remoteStorage.DownloadSegmentAsync(
            tp.Topic,
            tp.Partition,
            baseOffset,
            cancellationToken);

        // Write to cache
        await File.WriteAllBytesAsync(Path.Combine(cacheDir, $"{baseOffset:D20}.log"), logData, cancellationToken);
        if (indexData.Length > 0)
        {
            await File.WriteAllBytesAsync(Path.Combine(cacheDir, $"{baseOffset:D20}.index"), indexData, cancellationToken);
        }
        if (timeIndexData.Length > 0)
        {
            await File.WriteAllBytesAsync(Path.Combine(cacheDir, $"{baseOffset:D20}.timeindex"), timeIndexData, cancellationToken);
        }

        // Update metadata
        metadata.MarkCached(baseOffset, cacheDir);

        // Update cache size and evict if needed
        Interlocked.Add(ref _currentCacheSize, logData.Length + indexData.Length + timeIndexData.Length);
        await EvictCacheIfNeededAsync(cancellationToken);

        return cacheDir;
    }

    /// <summary>
    /// Get the path to segment files (local, cached, or download if needed)
    /// </summary>
    public async Task<string> GetSegmentPathAsync(
        TopicPartition tp,
        long baseOffset,
        CancellationToken cancellationToken = default)
    {
        var metadata = GetMetadata(tp);

        // Check if segment is local
        var localPath = Path.Combine(_dataDirectory, tp.Topic, $"partition-{tp.Partition}");
        var localLogPath = Path.Combine(localPath, $"{baseOffset:D20}.log");
        if (File.Exists(localLogPath))
        {
            return localPath;
        }

        // Check if cached
        var cachePath = metadata.GetCachePath(baseOffset);
        if (cachePath != null && Directory.Exists(cachePath))
        {
            return cachePath;
        }

        // Need to download from remote
        if (metadata.IsRemote(baseOffset))
        {
            return await DownloadSegmentToCacheAsync(tp, baseOffset, cancellationToken);
        }

        throw new FileNotFoundException($"Segment not found locally or remotely: {tp}/{baseOffset}");
    }

    /// <summary>
    /// Delete local copy of a segment after it's been uploaded
    /// </summary>
    public void DeleteLocalSegment(TopicPartition tp, long baseOffset)
    {
        var segmentDir = Path.Combine(_dataDirectory, tp.Topic, $"partition-{tp.Partition}");
        var logPath = Path.Combine(segmentDir, $"{baseOffset:D20}.log");
        var indexPath = Path.Combine(segmentDir, $"{baseOffset:D20}.index");
        var timeIndexPath = Path.Combine(segmentDir, $"{baseOffset:D20}.timeindex");

        if (File.Exists(logPath)) File.Delete(logPath);
        if (File.Exists(indexPath)) File.Delete(indexPath);
        if (File.Exists(timeIndexPath)) File.Delete(timeIndexPath);

        var metadata = GetMetadata(tp);
        metadata.MarkRemoteOnly(baseOffset);
    }

    /// <summary>
    /// Apply remote retention policy and delete old segments
    /// </summary>
    public async Task ApplyRemoteRetentionAsync(CancellationToken cancellationToken = default)
    {
        if (_config.RemoteRetentionHours < 0)
        {
            return; // Indefinite retention (negative value)
        }

        var cutoff = DateTimeOffset.UtcNow.AddHours(-_config.RemoteRetentionHours);

        foreach (var (tp, metadata) in _metadata)
        {
            var segments = metadata.GetAllSegments();
            foreach (var segment in segments)
            {
                if (segment.UploadedAt < cutoff)
                {
                    await _remoteStorage.DeleteSegmentAsync(tp.Topic, tp.Partition, segment.BaseOffset, cancellationToken);
                    metadata.Remove(segment.BaseOffset);
                }
            }
        }
    }

    /// <summary>
    /// Tier eligible segments for a partition
    /// </summary>
    public async Task TierSegmentsAsync(
        TopicPartition tp,
        IReadOnlyList<IFileLogSegment> segments,
        IFileLogSegment? activeSegment,
        CancellationToken cancellationToken = default)
    {
        var now = DateTimeOffset.UtcNow;
        var tieringLagCutoff = now.AddHours(-_config.TieringLagHours);
        var localRetentionCutoff = _config.LocalRetentionHours > 0
            ? now.AddHours(-_config.LocalRetentionHours)
            : DateTimeOffset.MinValue;

        var metadata = GetMetadata(tp);

        foreach (var segment in segments)
        {
            // Skip active segment
            if (segment == activeSegment)
            {
                continue;
            }

            // Skip if too small
            if (segment.Size < _config.MinSegmentSizeBytes)
            {
                continue;
            }

            // Skip if too recent
            if (segment.CreatedAt > tieringLagCutoff)
            {
                continue;
            }

            // Upload if not already remote
            if (!metadata.IsRemote(segment.BaseOffset))
            {
                await UploadSegmentAsync(tp, segment, cancellationToken);
            }

            // Delete local copy if past local retention and configured to do so
            if (_config.DeleteAfterUpload &&
                metadata.IsRemote(segment.BaseOffset) &&
                !metadata.IsRemoteOnly(segment.BaseOffset) &&
                segment.CreatedAt < localRetentionCutoff)
            {
                DeleteLocalSegment(tp, segment.BaseOffset);
            }
        }
    }

    /// <summary>
    /// Evict old entries from cache if size limit exceeded
    /// </summary>
    private async Task EvictCacheIfNeededAsync(CancellationToken cancellationToken)
    {
        if (_currentCacheSize <= _config.LocalCacheSizeBytes)
        {
            return;
        }

        await _cacheSemaphore.WaitAsync(cancellationToken);
        try
        {
            // Collect all cached segments with their timestamps
            var cachedSegments = new List<(TopicPartition Tp, long BaseOffset, DateTimeOffset CachedAt, string Path, long Size)>();

            foreach (var (tp, metadata) in _metadata)
            {
                foreach (var segment in metadata.GetAllSegments())
                {
                    if (segment.CachePath != null && segment.CachedAt != null && Directory.Exists(segment.CachePath))
                    {
                        var size = Directory.GetFiles(segment.CachePath).Sum(f => new FileInfo(f).Length);
                        cachedSegments.Add((tp, segment.BaseOffset, segment.CachedAt.Value, segment.CachePath, size));
                    }
                }
            }

            // Sort by cached time (oldest first) for LRU eviction
            cachedSegments = cachedSegments.OrderBy(s => s.CachedAt).ToList();

            // Evict until under limit
            foreach (var (tp, baseOffset, _, path, size) in cachedSegments)
            {
                if (_currentCacheSize <= _config.LocalCacheSizeBytes * 0.8) // Evict to 80% of limit
                {
                    break;
                }

                try
                {
                    Directory.Delete(path, recursive: true);
                    var metadata = GetMetadata(tp);
                    metadata.ClearCacheEntry(baseOffset);
                    Interlocked.Add(ref _currentCacheSize, -size);
                }
                catch
                {
                    // Ignore deletion errors
                }
            }
        }
        finally
        {
            _cacheSemaphore.Release();
        }
    }

    private async Task BackgroundTieringLoopAsync()
    {
        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(_config.TieringIntervalSeconds), _cts.Token);

                // Apply remote retention
                await ApplyRemoteRetentionAsync(_cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch
            {
                // Log error and continue
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try
        {
            await _backgroundTask;
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        await _remoteStorage.DisposeAsync();
        _cacheSemaphore.Dispose();
        _cts.Dispose();
    }
}
