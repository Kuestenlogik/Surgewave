using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Producer ids are unique in the cluster and over restarts (#223). Every broker counted from 1 at every start, so
/// producers on two brokers, or before and after a restart, could share an id and a partition leader mixed their
/// sequences.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ProducerIdBlocksTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "surgewave-producer-ids-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void ARestartedBrokerNeverHandsOutAnIdItHandedOutBefore()
    {
        var before = new ProducerIdBlocks(brokerId: 1, Path.Combine(_dir, "b1"));
        var first = Enumerable.Range(0, ProducerIdBlocks.BlockSize + 5).Select(_ => before.Next()).ToList();

        var after = new ProducerIdBlocks(brokerId: 1, Path.Combine(_dir, "b1"));
        var second = Enumerable.Range(0, 10).Select(_ => after.Next()).ToList();

        Assert.Equal(first.Count, first.Distinct().Count());
        Assert.True(second.Min() > first.Max(), $"after the restart {second.Min()}, before it up to {first.Max()}");
    }

    [Fact]
    public void TwoBrokersNeverHandOutTheSameId()
    {
        var one = new ProducerIdBlocks(brokerId: 1, Path.Combine(_dir, "b1"));
        var two = new ProducerIdBlocks(brokerId: 2, Path.Combine(_dir, "b2"));

        var ids = Enumerable.Range(0, 3000).SelectMany(_ => new[] { one.Next(), two.Next() }).ToList();

        Assert.Equal(ids.Count, ids.Distinct().Count());
        Assert.All(ids, id => Assert.True(id > 0));
    }

    [Fact]
    public void TheBlockIsOnDiskBeforeItsFirstIdIsUsed()
    {
        var blocks = new ProducerIdBlocks(brokerId: 3, _dir);
        var id = blocks.Next();

        var reserved = File.ReadAllBytes(Path.Combine(_dir, ".metadata", "producer-id-block"));
        var end = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(reserved);
        Assert.True((3L << 40) + end > id, $"reserved up to {end} for id {id}");
    }
}
