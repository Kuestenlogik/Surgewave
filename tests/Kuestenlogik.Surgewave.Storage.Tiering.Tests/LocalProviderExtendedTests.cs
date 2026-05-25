using Kuestenlogik.Surgewave.Storage.Tiering;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Tiering.Tests;

/// <summary>
/// Extended tests for LocalFileSystemStorageProvider edge cases and behavior.
/// </summary>
public class LocalProviderExtendedTests : IDisposable
{
    private readonly string _testDir;

    public LocalProviderExtendedTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"surgewave-local-provider-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_testDir, recursive: true); } catch { }
    }

    private string StoragePath => Path.Combine(_testDir, "storage");

    [Fact]
    public async Task Upload_CreatesDirectoryStructure()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);
        await provider.UploadSegmentAsync("my-topic", 3, 0, new byte[] { 1, 2, 3 }, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);

        var expectedDir = Path.Combine(StoragePath, "my-topic", "partition-3");
        Assert.True(Directory.Exists(expectedDir));
    }

    [Fact]
    public async Task Upload_WithEmptyIndexFiles_StillWorks()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);
        await provider.UploadSegmentAsync("t", 0, 0, new byte[] { 42 }, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);

        var downloaded = await provider.DownloadSegmentAsync("t", 0, 0);
        Assert.Equal(new byte[] { 42 }, downloaded.LogData);
        Assert.Empty(downloaded.IndexData);
        Assert.Empty(downloaded.TimeIndexData);
    }

    [Fact]
    public async Task Download_MissingSegment_ThrowsFileNotFound()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);

        await Assert.ThrowsAsync<FileNotFoundException>(
            () => provider.DownloadSegmentAsync("missing-topic", 0, 0));
    }

    [Fact]
    public async Task Delete_IsIdempotent_NoThrowForMissing()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);

        // Should not throw even if segment does not exist
        await provider.DeleteSegmentAsync("nonexistent-topic", 0, 0);
    }

    [Fact]
    public async Task ListSegments_EmptyPartition_ReturnsEmpty()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);

        var segments = await provider.ListSegmentsAsync("empty-topic", 0);
        Assert.Empty(segments);
    }

    [Fact]
    public async Task ListSegments_NonExistentPartition_ReturnsEmpty()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);

        var segments = await provider.ListSegmentsAsync("no-such-topic", 99);
        Assert.Empty(segments);
    }

    [Fact]
    public async Task GetSegmentInfo_MissingSegment_ReturnsNull()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);

        var info = await provider.GetSegmentInfoAsync("no-topic", 0, 0);
        Assert.Null(info);
    }

    [Fact]
    public async Task GetSegmentInfo_ExistingSegment_HasCorrectMetadata()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);
        var data = new byte[512];
        new Random(42).NextBytes(data);

        await provider.UploadSegmentAsync("meta-test", 1, 100, data, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);

        var info = await provider.GetSegmentInfoAsync("meta-test", 1, 100);
        Assert.NotNull(info);
        Assert.Equal("meta-test", info.Topic);
        Assert.Equal(1, info.Partition);
        Assert.Equal(100, info.BaseOffset);
        Assert.Equal(512, info.Size);
        Assert.True(info.UploadedAt <= DateTimeOffset.UtcNow);
    }

    [Fact]
    public async Task FetchLogSegment_MissingSegment_ThrowsFileNotFound()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);

        await Assert.ThrowsAsync<FileNotFoundException>(
            () => provider.FetchLogSegmentAsync("missing", 0, 0, 0));
    }

    [Fact]
    public async Task FetchLogSegment_StartBeyondEnd_ReturnsEmpty()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);
        await provider.UploadSegmentAsync("t", 0, 0, new byte[] { 1, 2, 3 }, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);

        await using var stream = await provider.FetchLogSegmentAsync("t", 0, 0, startPosition: 100);
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms);
        Assert.Empty(ms.ToArray());
    }

    [Fact]
    public async Task FetchLogSegment_FullRange_ReturnsAllBytes()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);
        var data = new byte[] { 10, 20, 30, 40, 50 };
        await provider.UploadSegmentAsync("t", 0, 0, data, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);

        await using var stream = await provider.FetchLogSegmentAsync("t", 0, 0, 0);
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms);
        Assert.Equal(data, ms.ToArray());
    }

    [Fact]
    public async Task FetchLogSegment_WithEndPosition_ClampsToFileLength()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);
        var data = new byte[] { 1, 2, 3, 4, 5 };
        await provider.UploadSegmentAsync("t", 0, 0, data, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);

        // End position beyond file length should be clamped
        await using var stream = await provider.FetchLogSegmentAsync("t", 0, 0, 0, endPosition: 1000);
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms);
        Assert.Equal(data, ms.ToArray());
    }

    [Fact]
    public async Task FetchIndex_MissingIndexType_ReturnsEmpty()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);
        await provider.UploadSegmentAsync("t", 0, 0, new byte[] { 1 }, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);

        await using var stream = await provider.FetchIndexAsync("t", 0, 0, RemoteIndexType.ProducerSnapshot);
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms);
        Assert.Empty(ms.ToArray());
    }

    [Fact]
    public async Task FetchIndex_LeaderEpoch_ReturnsEmptyWhenNotUploaded()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);
        await provider.UploadSegmentAsync("t", 0, 0, new byte[] { 1 }, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);

        await using var stream = await provider.FetchIndexAsync("t", 0, 0, RemoteIndexType.LeaderEpoch);
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms);
        Assert.Empty(ms.ToArray());
    }

    [Fact]
    public async Task CopyLogSegmentData_WithMissingOptionalFiles_Succeeds()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);
        var segId = Guid.NewGuid();
        var sourceDir = Path.Combine(_testDir, "src");
        Directory.CreateDirectory(sourceDir);

        var logPath = Path.Combine(sourceDir, "test.log");
        var indexPath = Path.Combine(sourceDir, "test.index");
        var timeIndexPath = Path.Combine(sourceDir, "test.timeindex");

        await File.WriteAllBytesAsync(logPath, [1, 2, 3]);
        await File.WriteAllBytesAsync(indexPath, [10, 20]);
        await File.WriteAllBytesAsync(timeIndexPath, [30, 40]);

        var segData = new LogSegmentData
        {
            LogPath = logPath,
            OffsetIndexPath = indexPath,
            TimeIndexPath = timeIndexPath
            // No TransactionIndexPath, ProducerSnapshotPath, LeaderEpochIndex
        };

        var meta = await provider.CopyLogSegmentDataAsync(segId, "copy-test", 0, 0, segData);

        Assert.NotNull(meta);
        Assert.Equal(segId.ToByteArray(), meta!.Data);
        Assert.True(await provider.SegmentExistsAsync("copy-test", 0, 0));
    }

    [Fact]
    public async Task CopyLogSegmentData_WithNonExistentLogFile_DoesNotThrow()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);
        var segId = Guid.NewGuid();

        var segData = new LogSegmentData
        {
            LogPath = "/non/existent/path.log",
            OffsetIndexPath = "/non/existent/path.index",
            TimeIndexPath = "/non/existent/path.timeindex"
        };

        // Should not throw, just skip missing files
        var meta = await provider.CopyLogSegmentDataAsync(segId, "no-file-test", 0, 0, segData);
        Assert.NotNull(meta);
    }

    [Fact]
    public async Task SegmentExists_MultipleTopics_AreIndependent()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);

        await provider.UploadSegmentAsync("topic-a", 0, 0, new byte[] { 1 }, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);

        Assert.True(await provider.SegmentExistsAsync("topic-a", 0, 0));
        Assert.False(await provider.SegmentExistsAsync("topic-b", 0, 0));
        Assert.False(await provider.SegmentExistsAsync("topic-a", 1, 0));
    }

    [Fact]
    public async Task Upload_Overwrite_UpdatesData()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);
        await provider.UploadSegmentAsync("t", 0, 0, new byte[] { 1, 2, 3 }, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);
        await provider.UploadSegmentAsync("t", 0, 0, new byte[] { 9, 8, 7 }, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty); // Overwrite

        var downloaded = await provider.DownloadSegmentAsync("t", 0, 0);
        Assert.Equal(new byte[] { 9, 8, 7 }, downloaded.LogData);
    }

    [Fact]
    public async Task ListSegments_OrderedByBaseOffset()
    {
        await using var provider = new LocalFileSystemStorageProvider(StoragePath);
        await provider.UploadSegmentAsync("ordered", 0, 300, new byte[] { 1 }, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);
        await provider.UploadSegmentAsync("ordered", 0, 100, new byte[] { 2 }, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);
        await provider.UploadSegmentAsync("ordered", 0, 200, new byte[] { 3 }, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);

        var segments = await provider.ListSegmentsAsync("ordered", 0);

        Assert.Equal(3, segments.Count);
        Assert.Equal(100, segments[0].BaseOffset);
        Assert.Equal(200, segments[1].BaseOffset);
        Assert.Equal(300, segments[2].BaseOffset);
    }
}
