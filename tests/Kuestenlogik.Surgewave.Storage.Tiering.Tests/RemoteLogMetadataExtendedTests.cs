using Kuestenlogik.Surgewave.Storage.Tiering;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Tiering.Tests;

/// <summary>
/// Extended tests for RemoteLogMetadata state machine, persistence and query operations.
/// </summary>
public class RemoteLogMetadataExtendedTests : IDisposable
{
    private readonly string _testDir;

    public RemoteLogMetadataExtendedTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"surgewave-meta-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_testDir, recursive: true); } catch { }
    }

    private string MetaPath(string name = "meta.json") =>
        Path.Combine(_testDir, name);

    [Fact]
    public void NewMetadata_IsEmpty()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        Assert.Empty(meta.GetAllSegments());
        Assert.Null(meta.FindSegmentContaining(0));
    }

    [Fact]
    public void MarkUploaded_NewSegment_CreatesEntry()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        meta.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);

        Assert.True(meta.IsRemote(0));
        Assert.False(meta.IsRemoteOnly(0));
        Assert.Single(meta.GetAllSegments());
    }

    [Fact]
    public void MarkUploaded_UpdatesExistingEntry()
    {
        using var meta = new RemoteLogMetadata(MetaPath());

        // Start copy first
        meta.StartCopy(0, 99, 512, 1000, brokerId: 1);
        meta.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);

        var segments = meta.GetAllSegments();
        Assert.Single(segments);
        Assert.Equal(1024, segments[0].Size);
        Assert.Equal(RemoteLogSegmentState.CopySegmentFinished, segments[0].State);
    }

    [Fact]
    public void StartCopy_ReturnsSegmentId_AndCreatesEntry()
    {
        using var meta = new RemoteLogMetadata(MetaPath());

        var segmentId = meta.StartCopy(0, 99, 512, 1000, brokerId: 1);

        Assert.NotEqual(Guid.Empty, segmentId);
        Assert.True(meta.IsRemote(0));
        var segments = meta.GetAllSegments();
        Assert.Single(segments);
        Assert.Equal(RemoteLogSegmentState.CopySegmentStarted, segments[0].State);
    }

    [Fact]
    public void StartCopy_WithLeaderEpochs_StoresEpochData()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        var epochs = new Dictionary<int, long> { [0] = 0, [1] = 50 };

        meta.StartCopy(0, 99, 512, 1000, brokerId: 1, leaderEpochs: epochs);

        var segments = meta.GetAllSegments();
        Assert.Single(segments);
        Assert.Equal(2, segments[0].SegmentLeaderEpochs.Count);
    }

    [Fact]
    public void StartDelete_FromFinishedState_Succeeds()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        meta.StartCopy(0, 99, 512, 1000, brokerId: 1);
        meta.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);

        var result = meta.StartDelete(0);

        Assert.True(result);
        var segments = meta.GetAllSegments();
        Assert.Equal(RemoteLogSegmentState.DeleteSegmentStarted, segments[0].State);
    }

    [Fact]
    public void StartDelete_NonExistentSegment_ReturnsFalse()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        var result = meta.StartDelete(999);
        Assert.False(result);
    }

    [Fact]
    public void FinishDelete_FromStartedState_Succeeds()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        meta.StartCopy(0, 99, 512, 1000, brokerId: 1);
        meta.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);
        meta.StartDelete(0);

        var result = meta.FinishDelete(0);

        Assert.True(result);
        var segments = meta.GetAllSegments();
        Assert.Equal(RemoteLogSegmentState.DeleteSegmentFinished, segments[0].State);
    }

    [Fact]
    public void FinishDelete_NonExistentSegment_ReturnsFalse()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        var result = meta.FinishDelete(999);
        Assert.False(result);
    }

    [Fact]
    public void MarkRemoteOnly_NonExistentSegment_IsNoOp()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        // Should not throw
        meta.MarkRemoteOnly(999);
        Assert.False(meta.IsRemoteOnly(999));
    }

    [Fact]
    public void MarkCached_NonExistentSegment_IsNoOp()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        // Should not throw
        meta.MarkCached(999, "/some/path");
        Assert.Null(meta.GetCachePath(999));
    }

    [Fact]
    public void ClearCacheEntry_NonExistentSegment_IsNoOp()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        // Should not throw
        meta.ClearCacheEntry(999);
    }

    [Fact]
    public void Remove_NonExistentSegment_IsNoOp()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        // Should not throw
        meta.Remove(999);
        Assert.False(meta.IsRemote(999));
    }

    [Fact]
    public void GetCachePath_NotCached_ReturnsNull()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        meta.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);
        Assert.Null(meta.GetCachePath(0));
    }

    [Fact]
    public void GetCachePath_NonExistent_ReturnsNull()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        Assert.Null(meta.GetCachePath(999));
    }

    [Fact]
    public void FindSegmentContaining_NoSegments_ReturnsNull()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        Assert.Null(meta.FindSegmentContaining(0));
    }

    [Fact]
    public void FindSegmentContaining_MultipleSegments_FindsCorrectOne()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        meta.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);
        meta.MarkUploaded(100, 1024, DateTimeOffset.UtcNow);
        meta.MarkUploaded(200, 1024, DateTimeOffset.UtcNow);

        var found = meta.FindSegmentContaining(150);
        Assert.NotNull(found);
        Assert.Equal(100, found.BaseOffset);
    }

    [Fact]
    public void FindSegmentContaining_BeforeFirstSegment_ReturnsNull()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        meta.MarkUploaded(100, 1024, DateTimeOffset.UtcNow);

        // Offset 50 is before base offset 100, so no segment contains it
        var found = meta.FindSegmentContaining(50);
        Assert.Null(found);
    }

    [Fact]
    public void GetRemoteOnlySegments_EmptyWhenNoneRemoteOnly()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        meta.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);
        meta.MarkUploaded(100, 1024, DateTimeOffset.UtcNow);

        var remoteOnly = meta.GetRemoteOnlySegments();
        Assert.Empty(remoteOnly);
    }

    [Fact]
    public void GetRemoteOnlySegments_ReturnsOnlyRemoteOnlyOnes()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        meta.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);
        meta.MarkUploaded(100, 1024, DateTimeOffset.UtcNow);
        meta.MarkUploaded(200, 1024, DateTimeOffset.UtcNow);
        meta.MarkRemoteOnly(100);
        meta.MarkRemoteOnly(200);

        var remoteOnly = meta.GetRemoteOnlySegments();
        Assert.Equal(2, remoteOnly.Count);
        Assert.All(remoteOnly, s => Assert.True(s.IsRemoteOnly));
    }

    [Fact]
    public void Persistence_FullLifecycle_SurvivesReload()
    {
        var path = MetaPath("lifecycle.json");

        using (var meta = new RemoteLogMetadata(path))
        {
            meta.StartCopy(0, 99, 512, 1000, brokerId: 1);
            meta.MarkUploaded(0, 1024, DateTimeOffset.UtcNow);
            meta.MarkRemoteOnly(0);
            meta.MarkCached(0, "/cache/seg0");

            meta.MarkUploaded(100, 2048, DateTimeOffset.UtcNow);
        }

        using var reloaded = new RemoteLogMetadata(path);
        var segments = reloaded.GetAllSegments();
        Assert.Equal(2, segments.Count);
        Assert.True(reloaded.IsRemoteOnly(0));
        Assert.False(reloaded.IsRemoteOnly(100));
    }

    [Fact]
    public void Persistence_CorruptFile_IsIgnored()
    {
        var path = MetaPath("corrupt.json");
        File.WriteAllText(path, "NOT VALID JSON { corrupted }");

        // Should not throw
        using var meta = new RemoteLogMetadata(path);
        Assert.Empty(meta.GetAllSegments());
    }

    [Fact]
    public void IsRemote_NonExistentOffset_ReturnsFalse()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        Assert.False(meta.IsRemote(12345));
    }

    [Fact]
    public void IsRemoteOnly_NonExistentOffset_ReturnsFalse()
    {
        using var meta = new RemoteLogMetadata(MetaPath());
        Assert.False(meta.IsRemoteOnly(12345));
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var meta = new RemoteLogMetadata(MetaPath());
        meta.Dispose();
        meta.Dispose(); // Should not throw
    }
}
