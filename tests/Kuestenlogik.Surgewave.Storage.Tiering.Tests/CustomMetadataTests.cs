using Kuestenlogik.Surgewave.Storage.Tiering;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Tiering.Tests;

/// <summary>
/// Tests for CustomMetadata, LogSegmentData, and RemoteSegmentInfo.
/// </summary>
public class CustomMetadataTests
{
    [Fact]
    public void CustomMetadata_ExactMaxSize_IsValid()
    {
        var data = new byte[CustomMetadata.MaxSize];
        var meta = new CustomMetadata(data);
        Assert.Equal(CustomMetadata.MaxSize, meta.Data.Length);
    }

    [Fact]
    public void CustomMetadata_EmptyData_IsValid()
    {
        var meta = new CustomMetadata([]);
        Assert.Empty(meta.Data);
    }

    [Fact]
    public void CustomMetadata_OverMaxSize_ThrowsArgumentException()
    {
        var data = new byte[CustomMetadata.MaxSize + 1];
        Assert.ThrowsAny<ArgumentException>(() => new CustomMetadata(data));
    }

    [Fact]
    public void CustomMetadata_MaxSizeConstant_Is128()
    {
        Assert.Equal(128, CustomMetadata.MaxSize);
    }

    [Fact]
    public void CustomMetadata_DataProperty_ReturnsOriginalData()
    {
        var data = new byte[] { 1, 2, 3, 4, 5 };
        var meta = new CustomMetadata(data);
        Assert.Equal(data, meta.Data);
    }

    [Fact]
    public void CustomMetadata_GuidBytes_FitsInMaxSize()
    {
        // Guid.ToByteArray() returns 16 bytes, well within 128
        var guidBytes = Guid.NewGuid().ToByteArray();
        var meta = new CustomMetadata(guidBytes);
        Assert.Equal(16, meta.Data.Length);
    }

    [Fact]
    public void LogSegmentData_AllProperties_CanBeSet()
    {
        var data = new LogSegmentData
        {
            LogPath = "/path/to/segment.log",
            OffsetIndexPath = "/path/to/segment.index",
            TimeIndexPath = "/path/to/segment.timeindex",
            TransactionIndexPath = "/path/to/segment.txnindex",
            ProducerSnapshotPath = "/path/to/segment.snapshot",
            LeaderEpochIndex = [1, 2, 3]
        };

        Assert.Equal("/path/to/segment.log", data.LogPath);
        Assert.Equal("/path/to/segment.index", data.OffsetIndexPath);
        Assert.Equal("/path/to/segment.timeindex", data.TimeIndexPath);
        Assert.Equal("/path/to/segment.txnindex", data.TransactionIndexPath);
        Assert.Equal("/path/to/segment.snapshot", data.ProducerSnapshotPath);
        Assert.Equal(new byte[] { 1, 2, 3 }, data.LeaderEpochIndex);
    }

    [Fact]
    public void LogSegmentData_OptionalProperties_DefaultToNull()
    {
        var data = new LogSegmentData
        {
            LogPath = "/log",
            OffsetIndexPath = "/index",
            TimeIndexPath = "/timeindex"
        };

        Assert.Null(data.TransactionIndexPath);
        Assert.Null(data.ProducerSnapshotPath);
        Assert.Null(data.LeaderEpochIndex);
    }

    [Fact]
    public void RemoteSegmentInfo_Properties_AreCorrect()
    {
        var createdAt = DateTimeOffset.UtcNow.AddHours(-1);
        var uploadedAt = DateTimeOffset.UtcNow;

        var info = new RemoteSegmentInfo(
            Topic: "my-topic",
            Partition: 3,
            BaseOffset: 1000,
            Size: 2048,
            CreatedAt: createdAt,
            UploadedAt: uploadedAt);

        Assert.Equal("my-topic", info.Topic);
        Assert.Equal(3, info.Partition);
        Assert.Equal(1000, info.BaseOffset);
        Assert.Equal(2048, info.Size);
        Assert.Equal(createdAt, info.CreatedAt);
        Assert.Equal(uploadedAt, info.UploadedAt);
    }

    [Fact]
    public void RemoteSegmentInfo_IsRecordType_SupportsEquality()
    {
        var now = DateTimeOffset.UtcNow;
        var a = new RemoteSegmentInfo("t", 0, 0, 100, now, now);
        var b = new RemoteSegmentInfo("t", 0, 0, 100, now, now);
        Assert.Equal(a, b);
    }

    [Fact]
    public void RemoteSegmentInfo_DifferentOffset_IsNotEqual()
    {
        var now = DateTimeOffset.UtcNow;
        var a = new RemoteSegmentInfo("t", 0, 0, 100, now, now);
        var b = new RemoteSegmentInfo("t", 0, 100, 100, now, now);
        Assert.NotEqual(a, b);
    }
}
