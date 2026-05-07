using System.Text;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests;

/// <summary>
/// Tests for partition strategies (RoundRobin, ByKey, Random, Sticky, Custom)
/// and ConsumerProtocolCodec.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class PartitionerTests
{
    #region RoundRobin Strategy

    [Fact]
    public void RoundRobin_CyclesThroughPartitions()
    {
        var strategy = Partitioner.RoundRobin;
        const int partitionCount = 3;

        var partitions = new HashSet<int>();
        for (int i = 0; i < 30; i++)
        {
            var p = strategy.SelectPartition(null, partitionCount);
            Assert.InRange(p, 0, partitionCount - 1);
            partitions.Add(p);
        }

        // After enough iterations all partitions should be used
        Assert.Equal(partitionCount, partitions.Count);
    }

    [Fact]
    public void RoundRobin_ReturnsValidPartition()
    {
        var strategy = Partitioner.RoundRobin;

        for (int i = 0; i < 100; i++)
        {
            var partition = strategy.SelectPartition(null, 5);
            Assert.InRange(partition, 0, 4);
        }
    }

    #endregion

    #region ByKey Strategy

    [Fact]
    public void ByKey_SameKey_SamePartition()
    {
        var strategy = Partitioner.ByKey;
        var key = Encoding.UTF8.GetBytes("order-123");

        var first = strategy.SelectPartition(key, 10);
        for (int i = 0; i < 100; i++)
        {
            Assert.Equal(first, strategy.SelectPartition(key, 10));
        }
    }

    [Fact]
    public void ByKey_DifferentKeys_MayDiffer()
    {
        var strategy = Partitioner.ByKey;
        var partitions = new HashSet<int>();

        for (int i = 0; i < 100; i++)
        {
            var key = Encoding.UTF8.GetBytes($"key-{i}");
            partitions.Add(strategy.SelectPartition(key, 10));
        }

        // With 100 different keys and 10 partitions, at least 2 should be used
        Assert.True(partitions.Count >= 2);
    }

    [Fact]
    public void ByKey_NullKey_FallsBackToRoundRobin()
    {
        var strategy = Partitioner.ByKey;
        var partition = strategy.SelectPartition(null, 5);
        Assert.InRange(partition, 0, 4);
    }

    [Fact]
    public void ByKey_EmptyKey_FallsBackToRoundRobin()
    {
        var strategy = Partitioner.ByKey;
        var partition = strategy.SelectPartition([], 5);
        Assert.InRange(partition, 0, 4);
    }

    [Fact]
    public void ByKey_ReturnsValidPartition()
    {
        var strategy = Partitioner.ByKey;
        var key = Encoding.UTF8.GetBytes("test-key");
        var partition = strategy.SelectPartition(key, 3);
        Assert.InRange(partition, 0, 2);
    }

    #endregion

    #region Random Strategy

    [Fact]
    public void Random_ReturnsValidPartition()
    {
        var strategy = Partitioner.Random;

        for (int i = 0; i < 100; i++)
        {
            var partition = strategy.SelectPartition(null, 5);
            Assert.InRange(partition, 0, 4);
        }
    }

    [Fact]
    public void Random_DistributesAcrossPartitions()
    {
        var strategy = Partitioner.Random;
        var partitions = new HashSet<int>();

        for (int i = 0; i < 1000; i++)
        {
            partitions.Add(strategy.SelectPartition(null, 5));
        }

        // With 1000 iterations across 5 partitions, all should be hit
        Assert.Equal(5, partitions.Count);
    }

    #endregion

    #region Custom Strategy

    [Fact]
    public void Custom_UsesProvidedFunction()
    {
        var strategy = Partitioner.Custom((_, count) => count - 1);
        Assert.Equal(4, strategy.SelectPartition(null, 5));
        Assert.Equal(9, strategy.SelectPartition(null, 10));
    }

    [Fact]
    public void Custom_WithKeyBasedLogic()
    {
        var strategy = Partitioner.Custom((key, count) =>
        {
            if (key == null) return 0;
            return key[0] % count;
        });

        var partition = strategy.SelectPartition(new byte[] { 7 }, 3);
        Assert.Equal(7 % 3, partition);
    }

    #endregion

    #region PartitionStrategyExtensions

    [Fact]
    public void SelectPartition_StringKey_UsesUtf8()
    {
        var strategy = Partitioner.ByKey;
        var partition = strategy.SelectPartition("test-key", 10);
        Assert.InRange(partition, 0, 9);
    }

    [Fact]
    public void SelectPartition_NullStringKey()
    {
        var strategy = Partitioner.ByKey;
        var partition = strategy.SelectPartition((string?)null, 5);
        Assert.InRange(partition, 0, 4);
    }

    #endregion

    #region ConsumerProtocolCodec Tests

    [Fact]
    public void ConsumerProtocolCodec_BuildAndParseAssignment_RoundTrip()
    {
        var partitions = new List<(string Topic, int Partition)>
        {
            ("topic-a", 0),
            ("topic-a", 1),
            ("topic-b", 0),
            ("topic-b", 1),
            ("topic-b", 2)
        };

        var bytes = ConsumerProtocolCodec.BuildAssignment(partitions);
        var parsed = ConsumerProtocolCodec.ParseAssignment(bytes);

        Assert.Equal(5, parsed.Count);
        Assert.Contains(("topic-a", 0), parsed);
        Assert.Contains(("topic-a", 1), parsed);
        Assert.Contains(("topic-b", 0), parsed);
        Assert.Contains(("topic-b", 1), parsed);
        Assert.Contains(("topic-b", 2), parsed);
    }

    [Fact]
    public void ConsumerProtocolCodec_EmptyAssignment()
    {
        var bytes = ConsumerProtocolCodec.BuildAssignment([]);
        var parsed = ConsumerProtocolCodec.ParseAssignment(bytes);
        Assert.Empty(parsed);
    }

    [Fact]
    public void ConsumerProtocolCodec_ParseAssignment_ShortBuffer_ReturnsEmpty()
    {
        // Buffer too short to contain valid assignment
        var parsed = ConsumerProtocolCodec.ParseAssignment([0, 0]);
        Assert.Empty(parsed);
    }

    [Fact]
    public void ConsumerProtocolCodec_BuildConsumerMetadata_SingleTopic()
    {
        var metadata = ConsumerProtocolCodec.BuildConsumerMetadata(["my-topic"]);
        Assert.NotNull(metadata);
        Assert.True(metadata.Length > 0);
    }

    [Fact]
    public void ConsumerProtocolCodec_BuildConsumerMetadata_MultipleTopics()
    {
        var metadata = ConsumerProtocolCodec.BuildConsumerMetadata(["topic-a", "topic-b", "topic-c"]);
        Assert.NotNull(metadata);
        Assert.True(metadata.Length > 0);
    }

    [Fact]
    public void ConsumerProtocolCodec_SinglePartition_RoundTrip()
    {
        var partitions = new List<(string Topic, int Partition)> { ("orders", 0) };
        var bytes = ConsumerProtocolCodec.BuildAssignment(partitions);
        var parsed = ConsumerProtocolCodec.ParseAssignment(bytes);

        Assert.Single(parsed);
        Assert.Equal(("orders", 0), parsed[0]);
    }

    #endregion
}
