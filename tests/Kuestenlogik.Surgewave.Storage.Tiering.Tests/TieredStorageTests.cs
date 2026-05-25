using Kuestenlogik.Surgewave.Storage.Tiering;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Tiering.Tests;

/// <summary>
/// Tests for tiered storage functionality
/// </summary>
public sealed class TieredStorageTests : IDisposable
{
    private readonly string _testDir;

    public TieredStorageTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"surgewave-tiered-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDir))
            {
                Directory.Delete(_testDir, recursive: true);
            }
        }
        catch
        {
            // Ignore cleanup errors
        }
    }

    #region LocalFileSystemStorageProvider Tests

    [Fact]
    public async Task LocalProvider_UploadAndDownload_RoundTrips()
    {
        // Arrange
        var storagePath = Path.Combine(_testDir, "remote");
        await using var provider = new LocalFileSystemStorageProvider(storagePath);

        var logData = new byte[] { 1, 2, 3, 4, 5 };
        var indexData = new byte[] { 10, 20, 30 };
        var timeIndexData = new byte[] { 100, 200 };

        // Act
        await provider.UploadSegmentAsync("test-topic", 0, 0, logData, indexData, timeIndexData);
        var (downloadedLog, downloadedIndex, downloadedTimeIndex) = await provider.DownloadSegmentAsync("test-topic", 0, 0);

        // Assert
        Assert.Equal(logData, downloadedLog);
        Assert.Equal(indexData, downloadedIndex);
        Assert.Equal(timeIndexData, downloadedTimeIndex);
    }

    [Fact]
    public async Task LocalProvider_SegmentExists_ReturnsTrueAfterUpload()
    {
        // Arrange
        var storagePath = Path.Combine(_testDir, "remote");
        await using var provider = new LocalFileSystemStorageProvider(storagePath);

        // Act & Assert - before upload
        Assert.False(await provider.SegmentExistsAsync("test-topic", 0, 0));

        // Upload
        await provider.UploadSegmentAsync("test-topic", 0, 0, new byte[] { 1 }, Array.Empty<byte>(), Array.Empty<byte>());

        // Assert - after upload
        Assert.True(await provider.SegmentExistsAsync("test-topic", 0, 0));
    }

    [Fact]
    public async Task LocalProvider_Delete_RemovesSegment()
    {
        // Arrange
        var storagePath = Path.Combine(_testDir, "remote");
        await using var provider = new LocalFileSystemStorageProvider(storagePath);
        await provider.UploadSegmentAsync("test-topic", 0, 0, new byte[] { 1 }, Array.Empty<byte>(), Array.Empty<byte>());

        // Act
        await provider.DeleteSegmentAsync("test-topic", 0, 0);

        // Assert
        Assert.False(await provider.SegmentExistsAsync("test-topic", 0, 0));
    }

    [Fact]
    public async Task LocalProvider_ListSegments_ReturnsAllSegments()
    {
        // Arrange
        var storagePath = Path.Combine(_testDir, "remote");
        await using var provider = new LocalFileSystemStorageProvider(storagePath);

        await provider.UploadSegmentAsync("test-topic", 0, 0, new byte[] { 1 }, Array.Empty<byte>(), Array.Empty<byte>());
        await provider.UploadSegmentAsync("test-topic", 0, 100, new byte[] { 2, 3 }, Array.Empty<byte>(), Array.Empty<byte>());
        await provider.UploadSegmentAsync("test-topic", 0, 200, new byte[] { 4, 5, 6 }, Array.Empty<byte>(), Array.Empty<byte>());

        // Act
        var segments = await provider.ListSegmentsAsync("test-topic", 0);

        // Assert
        Assert.Equal(3, segments.Count);
        Assert.Equal(0, segments[0].BaseOffset);
        Assert.Equal(100, segments[1].BaseOffset);
        Assert.Equal(200, segments[2].BaseOffset);
    }

    [Fact]
    public async Task LocalProvider_GetSegmentInfo_ReturnsMetadata()
    {
        // Arrange
        var storagePath = Path.Combine(_testDir, "remote");
        await using var provider = new LocalFileSystemStorageProvider(storagePath);
        var logData = new byte[1024];
        new Random(42).NextBytes(logData);

        await provider.UploadSegmentAsync("test-topic", 0, 500, logData, Array.Empty<byte>(), Array.Empty<byte>());

        // Act
        var info = await provider.GetSegmentInfoAsync("test-topic", 0, 500);

        // Assert
        Assert.NotNull(info);
        Assert.Equal("test-topic", info.Topic);
        Assert.Equal(0, info.Partition);
        Assert.Equal(500, info.BaseOffset);
        Assert.Equal(1024, info.Size);
    }

    [Fact]
    public async Task LocalProvider_MultiplePartitions_IsolatedCorrectly()
    {
        // Arrange
        var storagePath = Path.Combine(_testDir, "remote");
        await using var provider = new LocalFileSystemStorageProvider(storagePath);

        await provider.UploadSegmentAsync("test-topic", 0, 0, new byte[] { 1 }, Array.Empty<byte>(), Array.Empty<byte>());
        await provider.UploadSegmentAsync("test-topic", 1, 0, new byte[] { 2 }, Array.Empty<byte>(), Array.Empty<byte>());
        await provider.UploadSegmentAsync("test-topic", 2, 0, new byte[] { 3 }, Array.Empty<byte>(), Array.Empty<byte>());

        // Act
        var partition0 = await provider.ListSegmentsAsync("test-topic", 0);
        var partition1 = await provider.ListSegmentsAsync("test-topic", 1);
        var partition2 = await provider.ListSegmentsAsync("test-topic", 2);

        // Assert
        Assert.Single(partition0);
        Assert.Single(partition1);
        Assert.Single(partition2);
    }

    #endregion

    #region RemoteLogMetadata Tests

    [Fact]
    public void Metadata_MarkUploaded_TracksSegment()
    {
        // Arrange
        var metadataPath = Path.Combine(_testDir, "metadata.json");
        using var metadata = new RemoteLogMetadata(metadataPath);

        // Act
        metadata.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);

        // Assert
        Assert.True(metadata.IsRemote(0));
        Assert.False(metadata.IsRemoteOnly(0));
    }

    [Fact]
    public void Metadata_MarkRemoteOnly_UpdatesState()
    {
        // Arrange
        var metadataPath = Path.Combine(_testDir, "metadata.json");
        using var metadata = new RemoteLogMetadata(metadataPath);
        metadata.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);

        // Act
        metadata.MarkRemoteOnly(0);

        // Assert
        Assert.True(metadata.IsRemote(0));
        Assert.True(metadata.IsRemoteOnly(0));
    }

    [Fact]
    public void Metadata_MarkCached_SetsCachePath()
    {
        // Arrange
        var metadataPath = Path.Combine(_testDir, "metadata.json");
        using var metadata = new RemoteLogMetadata(metadataPath);
        metadata.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);

        // Act
        metadata.MarkCached(0, "/cache/path");

        // Assert
        Assert.Equal("/cache/path", metadata.GetCachePath(0));
    }

    [Fact]
    public void Metadata_ClearCacheEntry_RemovesCachePath()
    {
        // Arrange
        var metadataPath = Path.Combine(_testDir, "metadata.json");
        using var metadata = new RemoteLogMetadata(metadataPath);
        metadata.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);
        metadata.MarkCached(0, "/cache/path");

        // Act
        metadata.ClearCacheEntry(0);

        // Assert
        Assert.Null(metadata.GetCachePath(0));
    }

    [Fact]
    public void Metadata_Remove_DeletesSegment()
    {
        // Arrange
        var metadataPath = Path.Combine(_testDir, "metadata.json");
        using var metadata = new RemoteLogMetadata(metadataPath);
        metadata.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);

        // Act
        metadata.Remove(0);

        // Assert
        Assert.False(metadata.IsRemote(0));
    }

    [Fact]
    public void Metadata_GetAllSegments_ReturnsOrderedList()
    {
        // Arrange
        var metadataPath = Path.Combine(_testDir, "metadata.json");
        using var metadata = new RemoteLogMetadata(metadataPath);
        metadata.MarkUploaded(200, 1024, DateTimeOffset.UtcNow);
        metadata.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);
        metadata.MarkUploaded(100, 1024, DateTimeOffset.UtcNow);

        // Act
        var segments = metadata.GetAllSegments();

        // Assert
        Assert.Equal(3, segments.Count);
        Assert.Equal(0, segments[0].BaseOffset);
        Assert.Equal(100, segments[1].BaseOffset);
        Assert.Equal(200, segments[2].BaseOffset);
    }

    [Fact]
    public void Metadata_GetRemoteOnlySegments_FiltersCorrectly()
    {
        // Arrange
        var metadataPath = Path.Combine(_testDir, "metadata.json");
        using var metadata = new RemoteLogMetadata(metadataPath);
        metadata.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);
        metadata.MarkUploaded(100, 1024, DateTimeOffset.UtcNow);
        metadata.MarkRemoteOnly(100);

        // Act
        var remoteOnly = metadata.GetRemoteOnlySegments();

        // Assert
        Assert.Single(remoteOnly);
        Assert.Equal(100, remoteOnly[0].BaseOffset);
    }

    [Fact]
    public void Metadata_FindSegmentContaining_FindsCorrectSegment()
    {
        // Arrange
        var metadataPath = Path.Combine(_testDir, "metadata.json");
        using var metadata = new RemoteLogMetadata(metadataPath);
        metadata.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);
        metadata.MarkUploaded(100, 1024, DateTimeOffset.UtcNow);
        metadata.MarkUploaded(200, 1024, DateTimeOffset.UtcNow);

        // Act & Assert
        Assert.Equal(0, metadata.FindSegmentContaining(50)?.BaseOffset);
        Assert.Equal(100, metadata.FindSegmentContaining(150)?.BaseOffset);
        Assert.Equal(200, metadata.FindSegmentContaining(250)?.BaseOffset);
    }

    [Fact]
    public void Metadata_Persistence_SurvivesReload()
    {
        // Arrange
        var metadataPath = Path.Combine(_testDir, "metadata.json");

        // Create and populate metadata
        using (var metadata = new RemoteLogMetadata(metadataPath))
        {
            metadata.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);
            metadata.MarkUploaded(100, 2048, DateTimeOffset.UtcNow);
            metadata.MarkRemoteOnly(100);
        }

        // Act - reload metadata
        using var reloaded = new RemoteLogMetadata(metadataPath);
        var segments = reloaded.GetAllSegments();

        // Assert
        Assert.Equal(2, segments.Count);
        Assert.False(reloaded.IsRemoteOnly(0));
        Assert.True(reloaded.IsRemoteOnly(100));
    }

    #endregion

    #region Kafka-compatible API Tests (KIP-405)

    [Fact]
    public async Task LocalProvider_FetchLogSegment_ReturnsRangeData()
    {
        // Arrange
        var storagePath = Path.Combine(_testDir, "remote");
        await using var provider = new LocalFileSystemStorageProvider(storagePath);
        var logData = "0123456789ABCDEF"u8.ToArray();

        await provider.UploadSegmentAsync("test-topic", 0, 0, logData, Array.Empty<byte>(), Array.Empty<byte>());

        // Act - fetch bytes 5-10
        await using var stream = await provider.FetchLogSegmentAsync("test-topic", 0, 0, 5, 10);
        using var reader = new MemoryStream();
        await stream.CopyToAsync(reader);
        var result = reader.ToArray();

        // Assert
        Assert.Equal("56789"u8.ToArray(), result);
    }

    [Fact]
    public async Task LocalProvider_FetchLogSegment_WithoutEnd_ReturnsTillEnd()
    {
        // Arrange
        var storagePath = Path.Combine(_testDir, "remote");
        await using var provider = new LocalFileSystemStorageProvider(storagePath);
        var logData = "HelloWorld"u8.ToArray();

        await provider.UploadSegmentAsync("test-topic", 0, 0, logData, Array.Empty<byte>(), Array.Empty<byte>());

        // Act - fetch from position 5 to end
        await using var stream = await provider.FetchLogSegmentAsync("test-topic", 0, 0, 5);
        using var reader = new MemoryStream();
        await stream.CopyToAsync(reader);
        var result = reader.ToArray();

        // Assert
        Assert.Equal("World"u8.ToArray(), result);
    }

    [Fact]
    public async Task LocalProvider_FetchIndex_ReturnsOffsetIndex()
    {
        // Arrange
        var storagePath = Path.Combine(_testDir, "remote");
        await using var provider = new LocalFileSystemStorageProvider(storagePath);
        var indexData = new byte[] { 0, 0, 0, 0, 0, 0, 0, 100 };

        await provider.UploadSegmentAsync("test-topic", 0, 0, new byte[] { 1 }, indexData, Array.Empty<byte>());

        // Act
        await using var stream = await provider.FetchIndexAsync("test-topic", 0, 0, RemoteIndexType.Offset);
        using var reader = new MemoryStream();
        await stream.CopyToAsync(reader);
        var result = reader.ToArray();

        // Assert
        Assert.Equal(indexData, result);
    }

    [Fact]
    public async Task LocalProvider_FetchIndex_ReturnsTimestampIndex()
    {
        // Arrange
        var storagePath = Path.Combine(_testDir, "remote");
        await using var provider = new LocalFileSystemStorageProvider(storagePath);
        var timeIndexData = new byte[] { 0, 0, 1, 0, 0, 0, 0, 50 };

        await provider.UploadSegmentAsync("test-topic", 0, 0, new byte[] { 1 }, Array.Empty<byte>(), timeIndexData);

        // Act
        await using var stream = await provider.FetchIndexAsync("test-topic", 0, 0, RemoteIndexType.Timestamp);
        using var reader = new MemoryStream();
        await stream.CopyToAsync(reader);
        var result = reader.ToArray();

        // Assert
        Assert.Equal(timeIndexData, result);
    }

    [Fact]
    public async Task LocalProvider_FetchIndex_MissingIndex_ReturnsEmpty()
    {
        // Arrange
        var storagePath = Path.Combine(_testDir, "remote");
        await using var provider = new LocalFileSystemStorageProvider(storagePath);

        await provider.UploadSegmentAsync("test-topic", 0, 0, new byte[] { 1 }, Array.Empty<byte>(), Array.Empty<byte>());

        // Act
        await using var stream = await provider.FetchIndexAsync("test-topic", 0, 0, RemoteIndexType.Transaction);
        using var reader = new MemoryStream();
        await stream.CopyToAsync(reader);
        var result = reader.ToArray();

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public async Task LocalProvider_CopyLogSegmentData_UploadsAllIndexTypes()
    {
        // Arrange
        var storagePath = Path.Combine(_testDir, "remote");
        await using var provider = new LocalFileSystemStorageProvider(storagePath);
        var segmentId = Guid.NewGuid();

        // Create test files
        var sourceDir = Path.Combine(_testDir, "source");
        Directory.CreateDirectory(sourceDir);

        var logPath = Path.Combine(sourceDir, "test.log");
        var indexPath = Path.Combine(sourceDir, "test.index");
        var timeIndexPath = Path.Combine(sourceDir, "test.timeindex");

        await File.WriteAllBytesAsync(logPath, [1, 2, 3, 4, 5]);
        await File.WriteAllBytesAsync(indexPath, [10, 20]);
        await File.WriteAllBytesAsync(timeIndexPath, [30, 40]);

        var segmentData = new LogSegmentData
        {
            LogPath = logPath,
            OffsetIndexPath = indexPath,
            TimeIndexPath = timeIndexPath,
            LeaderEpochIndex = [50, 60, 70]
        };

        // Act
        var customMetadata = await provider.CopyLogSegmentDataAsync(
            segmentId, "test-topic", 0, 0, segmentData);

        // Assert
        Assert.NotNull(customMetadata);
        Assert.Equal(segmentId.ToByteArray(), customMetadata.Data);
        Assert.True(await provider.SegmentExistsAsync("test-topic", 0, 0));

        // Verify leader epoch index was uploaded
        await using var leaderEpochStream = await provider.FetchIndexAsync(
            "test-topic", 0, 0, RemoteIndexType.LeaderEpoch);
        using var reader = new MemoryStream();
        await leaderEpochStream.CopyToAsync(reader);
        Assert.Equal([50, 60, 70], reader.ToArray());
    }

    [Fact]
    public void RemoteLogSegmentState_ValidTransitions()
    {
        // CopySegmentStarted can transition to CopySegmentFinished or DeleteSegmentStarted
        var startedTransitions = RemoteLogSegmentState.CopySegmentStarted.ValidTransitions();
        Assert.Contains(RemoteLogSegmentState.CopySegmentFinished, startedTransitions);
        Assert.Contains(RemoteLogSegmentState.DeleteSegmentStarted, startedTransitions);

        // CopySegmentFinished can only transition to DeleteSegmentStarted
        var finishedTransitions = RemoteLogSegmentState.CopySegmentFinished.ValidTransitions();
        Assert.Single(finishedTransitions);
        Assert.Contains(RemoteLogSegmentState.DeleteSegmentStarted, finishedTransitions);

        // DeleteSegmentStarted can only transition to DeleteSegmentFinished
        var deleteStartedTransitions = RemoteLogSegmentState.DeleteSegmentStarted.ValidTransitions();
        Assert.Single(deleteStartedTransitions);
        Assert.Contains(RemoteLogSegmentState.DeleteSegmentFinished, deleteStartedTransitions);

        // DeleteSegmentFinished is terminal
        var terminalTransitions = RemoteLogSegmentState.DeleteSegmentFinished.ValidTransitions();
        Assert.Empty(terminalTransitions);
    }

    [Fact]
    public void RemoteLogSegmentState_IsReadable()
    {
        Assert.False(RemoteLogSegmentState.CopySegmentStarted.IsReadable());
        Assert.True(RemoteLogSegmentState.CopySegmentFinished.IsReadable());
        Assert.False(RemoteLogSegmentState.DeleteSegmentStarted.IsReadable());
        Assert.False(RemoteLogSegmentState.DeleteSegmentFinished.IsReadable());
    }

    [Fact]
    public void CustomMetadata_EnforcesMaxSize()
    {
        // Valid size
        var validData = new byte[CustomMetadata.MaxSize];
        var metadata = new CustomMetadata(validData);
        Assert.Equal(validData.Length, metadata.Data.Length);

        // Exceeds max size
        var oversizedData = new byte[CustomMetadata.MaxSize + 1];
        Assert.Throws<ArgumentException>(() => new CustomMetadata(oversizedData));
    }

    #endregion

    #region TieredStorageConfig Tests

    [Fact]
    public void Config_Defaults_AreReasonable()
    {
        var config = new TieredStorageConfig();

        Assert.False(config.Enabled);
        Assert.Equal("local", config.Provider);
        Assert.Equal(24, config.LocalRetentionHours);
        Assert.Equal(-1, config.RemoteRetentionHours);
        Assert.Equal(1, config.TieringLagHours);
        Assert.Equal(1024 * 1024, config.MinSegmentSizeBytes);
        Assert.Equal(1024L * 1024 * 1024, config.LocalCacheSizeBytes);
        Assert.True(config.DeleteAfterUpload);
        Assert.Equal(300, config.TieringIntervalSeconds);
    }

    #endregion
}
