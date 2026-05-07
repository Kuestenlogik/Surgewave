using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Tiering;
using Microsoft.Win32.SafeHandles;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Tiering.Tests;

/// <summary>
/// Minimal fake IFileLogSegment for testing TieredStorageManager.
/// </summary>
sealed class FakeFileLogSegment : IFileLogSegment
{
    public long BaseOffset { get; init; }
    public long CurrentOffset => BaseOffset;
    public long Size { get; init; } = 100;
    public bool IsFull => false;
    public DateTime CreatedAt { get; init; } = DateTime.UtcNow.AddHours(-2);
    public long MaxTimestamp => 0;
    public string LogFilePath => $"/fake/{BaseOffset:D20}.log";
    public SafeFileHandle SafeFileHandle => throw new NotImplementedException();

    public void Dispose() { }
    public void DeleteFiles() { }
    public long? GetFirstMessageOffset() => null;
    public long? GetFilePositionForOffset(long startOffset) => null;
    public long? FindOffsetByTimestamp(long targetTimestamp) => null;

    public ValueTask<(long baseOffset, int recordCount)> AppendBatchAsync(byte[] recordBatch, CancellationToken ct = default)
        => ValueTask.FromResult((BaseOffset, 0));

    public ValueTask FlushAsync(CancellationToken ct = default) => ValueTask.CompletedTask;

    public ValueTask<List<byte[]>> ReadBatchesAsync(long startOffset, int maxBytes, CancellationToken ct = default)
        => ValueTask.FromResult(new List<byte[]>());

    public ValueTask<(ReadOnlyMemory<byte> Data, List<int> BatchOffsets)> ReadBatchesContiguousAsync(
        long startOffset, int maxBytes, CancellationToken ct = default)
        => ValueTask.FromResult((ReadOnlyMemory<byte>.Empty, new List<int>()));
}

/// <summary>
/// Tests for TieredStorageManager upload, download, caching, and retention.
/// </summary>
public class TieredStorageManagerTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _dataDir;
    private readonly string _cacheDir;

    public TieredStorageManagerTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"surgewave-tsm-{Guid.NewGuid():N}");
        _dataDir = Path.Combine(_testDir, "data");
        _cacheDir = Path.Combine(_testDir, "cache");
        Directory.CreateDirectory(_dataDir);
        Directory.CreateDirectory(_cacheDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_testDir, recursive: true); } catch { }
    }

    private TieredStorageConfig MakeConfig(int tieringLagHours = 0, int localRetentionHours = -1) =>
        new TieredStorageConfig
        {
            Enabled = true,
            Provider = "local",
            LocalCachePath = _cacheDir,
            TieringLagHours = tieringLagHours,
            LocalRetentionHours = localRetentionHours,
            MinSegmentSizeBytes = 0, // allow all sizes
            DeleteAfterUpload = false,
            TieringIntervalSeconds = 3600, // Don't run background loop frequently
            RemoteRetentionHours = -1
        };

    private static byte[] MakeLogData(int size = 64)
    {
        var data = new byte[size];
        new Random(42).NextBytes(data);
        return data;
    }

    private void CreateSegmentFiles(string topic, int partition, long baseOffset, byte[]? logData = null)
    {
        var segDir = Path.Combine(_dataDir, topic, $"partition-{partition}");
        Directory.CreateDirectory(segDir);
        var logPath = Path.Combine(segDir, $"{baseOffset:D20}.log");
        File.WriteAllBytes(logPath, logData ?? MakeLogData());
    }

    private static FakeFileLogSegment MakeFakeSegment(
        long baseOffset,
        long size = 100,
        DateTime? createdAt = null) => new FakeFileLogSegment
    {
        BaseOffset = baseOffset,
        Size = size,
        CreatedAt = createdAt ?? DateTime.UtcNow.AddHours(-2)
    };

    [Fact]
    public async Task IsSegmentRemote_BeforeUpload_ReturnsFalse()
    {
        var tp = new TopicPartition { Topic = "t", Partition = 0 };
        var config = MakeConfig();
        var provider = new LocalFileSystemStorageProvider(Path.Combine(_testDir, "remote"));

        await using var manager = new TieredStorageManager(config, _dataDir, provider);

        Assert.False(manager.IsSegmentRemote(tp, 0));
    }

    [Fact]
    public async Task UploadSegment_MarksSegmentAsRemote()
    {
        var tp = new TopicPartition { Topic = "upload-test", Partition = 0 };
        var config = MakeConfig();
        var remoteDir = Path.Combine(_testDir, "remote");
        var provider = new LocalFileSystemStorageProvider(remoteDir);

        CreateSegmentFiles(tp.Topic, tp.Partition, baseOffset: 0);
        var segment = MakeFakeSegment(baseOffset: 0);

        await using var manager = new TieredStorageManager(config, _dataDir, provider);
        await manager.UploadSegmentAsync(tp, segment);

        Assert.True(manager.IsSegmentRemote(tp, 0));
    }

    [Fact]
    public async Task UploadSegment_MissingLogFile_DoesNotThrow()
    {
        var tp = new TopicPartition { Topic = "missing-log", Partition = 0 };
        var config = MakeConfig();
        var provider = new LocalFileSystemStorageProvider(Path.Combine(_testDir, "remote"));

        // No files created on disk
        var segment = MakeFakeSegment(baseOffset: 0);

        await using var manager = new TieredStorageManager(config, _dataDir, provider);
        // Should not throw - just skips if log file not found
        await manager.UploadSegmentAsync(tp, segment);
    }

    [Fact]
    public async Task IsSegmentRemoteOnly_BeforeMarkRemoteOnly_ReturnsFalse()
    {
        var tp = new TopicPartition { Topic = "t", Partition = 0 };
        var config = MakeConfig();
        var provider = new LocalFileSystemStorageProvider(Path.Combine(_testDir, "remote"));

        await using var manager = new TieredStorageManager(config, _dataDir, provider);

        Assert.False(manager.IsSegmentRemoteOnly(tp, 0));
    }

    [Fact]
    public async Task DeleteLocalSegment_RemovesLocalFiles()
    {
        var tp = new TopicPartition { Topic = "del-test", Partition = 0 };
        var segDir = Path.Combine(_dataDir, tp.Topic, $"partition-{tp.Partition}");
        Directory.CreateDirectory(segDir);

        var logFile = Path.Combine(segDir, "00000000000000000000.log");
        var indexFile = Path.Combine(segDir, "00000000000000000000.index");
        File.WriteAllBytes(logFile, [1, 2, 3]);
        File.WriteAllBytes(indexFile, [4, 5]);

        var config = MakeConfig();
        var provider = new LocalFileSystemStorageProvider(Path.Combine(_testDir, "remote"));
        await using var manager = new TieredStorageManager(config, _dataDir, provider);

        manager.DeleteLocalSegment(tp, 0);

        Assert.False(File.Exists(logFile));
        Assert.False(File.Exists(indexFile));
    }

    [Fact]
    public async Task DownloadSegmentToCache_AfterUpload_ReturnsCachePath()
    {
        var tp = new TopicPartition { Topic = "dl-test", Partition = 0 };
        var remoteDir = Path.Combine(_testDir, "remote");
        var provider = new LocalFileSystemStorageProvider(remoteDir);

        CreateSegmentFiles(tp.Topic, tp.Partition, 0, MakeLogData(128));
        var segment = MakeFakeSegment(0);

        var config = MakeConfig();
        await using var manager = new TieredStorageManager(config, _dataDir, provider);
        await manager.UploadSegmentAsync(tp, segment);

        var cachePath = await manager.DownloadSegmentToCacheAsync(tp, 0);

        Assert.NotNull(cachePath);
        Assert.True(Directory.Exists(cachePath));

        // Check that log file was cached
        var cachedLog = Path.Combine(cachePath, "00000000000000000000.log");
        Assert.True(File.Exists(cachedLog));
    }

    [Fact]
    public async Task DownloadSegmentToCache_AlreadyCached_ReturnsSamePath()
    {
        var tp = new TopicPartition { Topic = "dl-cached", Partition = 0 };
        var remoteDir = Path.Combine(_testDir, "remote");
        var provider = new LocalFileSystemStorageProvider(remoteDir);

        CreateSegmentFiles(tp.Topic, tp.Partition, 0);
        var segment = MakeFakeSegment(0);

        var config = MakeConfig();
        await using var manager = new TieredStorageManager(config, _dataDir, provider);
        await manager.UploadSegmentAsync(tp, segment);

        var path1 = await manager.DownloadSegmentToCacheAsync(tp, 0);
        var path2 = await manager.DownloadSegmentToCacheAsync(tp, 0);

        Assert.Equal(path1, path2);
    }

    [Fact]
    public async Task GetSegmentPath_LocalFileExists_ReturnsLocalPath()
    {
        var tp = new TopicPartition { Topic = "local-seg", Partition = 0 };
        CreateSegmentFiles(tp.Topic, tp.Partition, 0);

        var config = MakeConfig();
        var provider = new LocalFileSystemStorageProvider(Path.Combine(_testDir, "remote"));
        await using var manager = new TieredStorageManager(config, _dataDir, provider);

        var path = await manager.GetSegmentPathAsync(tp, 0);

        Assert.NotNull(path);
        Assert.Contains(tp.Topic, path);
    }

    [Fact]
    public async Task GetSegmentPath_NotFoundAnywhere_ThrowsFileNotFound()
    {
        var tp = new TopicPartition { Topic = "nowhere", Partition = 0 };
        var config = MakeConfig();
        var provider = new LocalFileSystemStorageProvider(Path.Combine(_testDir, "remote"));
        await using var manager = new TieredStorageManager(config, _dataDir, provider);

        await Assert.ThrowsAsync<FileNotFoundException>(
            () => manager.GetSegmentPathAsync(tp, 0));
    }

    [Fact]
    public async Task ApplyRemoteRetention_NegativeHours_DoesNotDelete()
    {
        var tp = new TopicPartition { Topic = "retain", Partition = 0 };
        var remoteDir = Path.Combine(_testDir, "remote");
        var provider = new LocalFileSystemStorageProvider(remoteDir);

        CreateSegmentFiles(tp.Topic, tp.Partition, 0);
        var segment = MakeFakeSegment(0);

        var config = new TieredStorageConfig
        {
            Enabled = true,
            LocalCachePath = _cacheDir,
            RemoteRetentionHours = -1, // Indefinite
            TieringIntervalSeconds = 3600,
            MinSegmentSizeBytes = 0,
            DeleteAfterUpload = false
        };

        await using var manager = new TieredStorageManager(config, _dataDir, provider);
        await manager.UploadSegmentAsync(tp, segment);
        await manager.ApplyRemoteRetentionAsync();

        // Should still be remote with indefinite retention
        Assert.True(manager.IsSegmentRemote(tp, 0));
    }

    [Fact]
    public async Task ApplyRemoteRetention_ZeroHours_DeletesOldSegments()
    {
        var tp = new TopicPartition { Topic = "expire-test", Partition = 0 };
        var remoteDir = Path.Combine(_testDir, "remote");
        var provider = new LocalFileSystemStorageProvider(remoteDir);

        CreateSegmentFiles(tp.Topic, tp.Partition, 0);
        var segment = MakeFakeSegment(0);

        // Retention of 0 hours means expired immediately
        var config = new TieredStorageConfig
        {
            Enabled = true,
            LocalCachePath = _cacheDir,
            RemoteRetentionHours = 0,
            TieringIntervalSeconds = 3600,
            MinSegmentSizeBytes = 0,
            DeleteAfterUpload = false
        };

        await using var manager = new TieredStorageManager(config, _dataDir, provider);
        await manager.UploadSegmentAsync(tp, segment);

        // Wait to ensure the upload time is past the cutoff
        await Task.Delay(10);
        await manager.ApplyRemoteRetentionAsync();

        // After applying 0-hour retention, segment should be removed
        Assert.False(manager.IsSegmentRemote(tp, 0));
    }

    [Fact]
    public async Task TierSegments_SkipsActiveSegment()
    {
        var tp = new TopicPartition { Topic = "tier-active", Partition = 0 };
        var remoteDir = Path.Combine(_testDir, "remote");
        var provider = new LocalFileSystemStorageProvider(remoteDir);

        var active = MakeFakeSegment(0, createdAt: DateTime.UtcNow.AddHours(-2));
        var config = MakeConfig(tieringLagHours: 0);
        await using var manager = new TieredStorageManager(config, _dataDir, provider);

        // Pass active segment as activeSegment - should be skipped
        await manager.TierSegmentsAsync(tp, [active], activeSegment: active);

        Assert.False(manager.IsSegmentRemote(tp, 0));
    }

    [Fact]
    public async Task TierSegments_SkipsTooSmallSegments()
    {
        var tp = new TopicPartition { Topic = "tier-small", Partition = 0 };
        var remoteDir = Path.Combine(_testDir, "remote");
        var provider = new LocalFileSystemStorageProvider(remoteDir);

        var config = new TieredStorageConfig
        {
            Enabled = true,
            LocalCachePath = _cacheDir,
            MinSegmentSizeBytes = 1_000_000, // 1 MB
            TieringLagHours = 0,
            TieringIntervalSeconds = 3600,
            DeleteAfterUpload = false
        };

        var smallSeg = MakeFakeSegment(0, size: 100); // Much smaller than min
        await using var manager = new TieredStorageManager(config, _dataDir, provider);

        await manager.TierSegmentsAsync(tp, [smallSeg], activeSegment: null);

        Assert.False(manager.IsSegmentRemote(tp, 0));
    }

    [Fact]
    public async Task TierSegments_SkipsTooRecentSegments()
    {
        var tp = new TopicPartition { Topic = "tier-recent", Partition = 0 };
        var remoteDir = Path.Combine(_testDir, "remote");
        var provider = new LocalFileSystemStorageProvider(remoteDir);

        var config = new TieredStorageConfig
        {
            Enabled = true,
            LocalCachePath = _cacheDir,
            MinSegmentSizeBytes = 0,
            TieringLagHours = 24, // 24 hour lag - recent segment won't be tiered
            TieringIntervalSeconds = 3600,
            DeleteAfterUpload = false
        };

        var recentSeg = MakeFakeSegment(0, createdAt: DateTime.UtcNow.AddMinutes(-5));
        await using var manager = new TieredStorageManager(config, _dataDir, provider);

        await manager.TierSegmentsAsync(tp, [recentSeg], activeSegment: null);

        Assert.False(manager.IsSegmentRemote(tp, 0));
    }

    [Fact]
    public async Task Dispose_CompletesGracefully()
    {
        var config = MakeConfig();
        var provider = new LocalFileSystemStorageProvider(Path.Combine(_testDir, "remote"));
        var manager = new TieredStorageManager(config, _dataDir, provider);

        // Should not throw
        await manager.DisposeAsync();
    }

    [Fact]
    public async Task TierSegments_EligibleSegment_GetsUploaded()
    {
        var tp = new TopicPartition { Topic = "tier-eligible", Partition = 0 };
        var remoteDir = Path.Combine(_testDir, "remote");
        var provider = new LocalFileSystemStorageProvider(remoteDir);

        // Create segment files on disk
        CreateSegmentFiles(tp.Topic, tp.Partition, 0, MakeLogData(1024));

        var config = new TieredStorageConfig
        {
            Enabled = true,
            LocalCachePath = _cacheDir,
            MinSegmentSizeBytes = 0,      // No minimum size
            TieringLagHours = 0,          // No lag requirement
            TieringIntervalSeconds = 3600,
            DeleteAfterUpload = false
        };

        var oldSeg = MakeFakeSegment(0, size: 2048, createdAt: DateTime.UtcNow.AddHours(-5));
        await using var manager = new TieredStorageManager(config, _dataDir, provider);

        await manager.TierSegmentsAsync(tp, [oldSeg], activeSegment: null);

        Assert.True(manager.IsSegmentRemote(tp, 0));
    }

    [Fact]
    public async Task MultipleTopicPartitions_AreTrackedIndependently()
    {
        var tp1 = new TopicPartition { Topic = "topic1", Partition = 0 };
        var tp2 = new TopicPartition { Topic = "topic2", Partition = 0 };
        var remoteDir = Path.Combine(_testDir, "remote");
        var provider = new LocalFileSystemStorageProvider(remoteDir);

        CreateSegmentFiles(tp1.Topic, tp1.Partition, 0, MakeLogData(64));
        CreateSegmentFiles(tp2.Topic, tp2.Partition, 0, MakeLogData(64));

        var config = MakeConfig();
        await using var manager = new TieredStorageManager(config, _dataDir, provider);

        await manager.UploadSegmentAsync(tp1, MakeFakeSegment(0));

        Assert.True(manager.IsSegmentRemote(tp1, 0));
        Assert.False(manager.IsSegmentRemote(tp2, 0));
    }
}
