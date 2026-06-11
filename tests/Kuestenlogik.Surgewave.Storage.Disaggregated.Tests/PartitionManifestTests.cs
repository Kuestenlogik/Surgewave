using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Storage.Disaggregated;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Tests;

public sealed class PartitionManifestTests
{
    private static readonly TopicPartition Partition = new() { Topic = "orders", Partition = 0 };

    [Fact]
    public void Empty_manifest_has_version_zero_and_no_offsets()
    {
        var m = PartitionManifest.Empty(Partition);
        Assert.Equal(0, m.Version);
        Assert.Empty(m.Objects);
        Assert.Null(m.FirstOffset);
        Assert.Null(m.LastOffset);
    }

    [Fact]
    public void Append_bumps_version_and_records_object()
    {
        var ref0 = new StreamObjectRef("orders/0/000.so", 0, 99, 1024, DateTime.UtcNow);
        var next = PartitionManifest.Empty(Partition).AppendObject(ref0);

        Assert.Equal(1, next.Version);
        Assert.Single(next.Objects);
        Assert.Equal(0, next.FirstOffset);
        Assert.Equal(99, next.LastOffset);
    }

    [Fact]
    public void Append_with_overlapping_range_throws()
    {
        var first = new StreamObjectRef("orders/0/000.so", 0, 99, 1024, DateTime.UtcNow);
        var overlapping = new StreamObjectRef("orders/0/001.so", 50, 149, 1024, DateTime.UtcNow);
        var m = PartitionManifest.Empty(Partition).AppendObject(first);

        Assert.Throws<InvalidOperationException>(() => m.AppendObject(overlapping));
    }

    [Fact]
    public void Append_with_contiguous_range_is_allowed()
    {
        var first = new StreamObjectRef("orders/0/000.so", 0, 99, 1024, DateTime.UtcNow);
        var contiguous = new StreamObjectRef("orders/0/001.so", 100, 199, 1024, DateTime.UtcNow);
        var m = PartitionManifest.Empty(Partition)
            .AppendObject(first)
            .AppendObject(contiguous);

        Assert.Equal(2, m.Version);
        Assert.Equal(0, m.FirstOffset);
        Assert.Equal(199, m.LastOffset);
    }

    [Fact]
    public void Locate_returns_object_containing_offset()
    {
        var m = PartitionManifest.Empty(Partition)
            .AppendObject(new StreamObjectRef("a", 0, 99, 1024, DateTime.UtcNow))
            .AppendObject(new StreamObjectRef("b", 100, 199, 1024, DateTime.UtcNow))
            .AppendObject(new StreamObjectRef("c", 200, 299, 1024, DateTime.UtcNow));

        Assert.Equal("a", m.Locate(0)?.ObjectKey);
        Assert.Equal("a", m.Locate(99)?.ObjectKey);
        Assert.Equal("b", m.Locate(150)?.ObjectKey);
        Assert.Equal("c", m.Locate(299)?.ObjectKey);
        Assert.Null(m.Locate(300));
    }

    [Fact]
    public void StreamObjectRef_RecordCount_is_inclusive_range()
    {
        var r = new StreamObjectRef("k", 100, 199, 0, DateTime.UtcNow);
        Assert.Equal(100, r.RecordCount);
    }
}
