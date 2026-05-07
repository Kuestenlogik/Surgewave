using System.Text;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Partitioning;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests;

/// <summary>
/// Tests for <see cref="PriorityPartitioner"/>, <see cref="MessagePriority"/>,
/// <see cref="MessagePriorityExtensions"/>, and <see cref="PriorityConsumerConfig"/>.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class PriorityPartitionerTests
{
    // ──────────────────────────────────────────────────────────────────────────
    // MessagePriority enum values
    // ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public void MessagePriority_EnumValues_AreCorrect()
    {
        Assert.Equal(0, (int)MessagePriority.High);
        Assert.Equal(1, (int)MessagePriority.Normal);
        Assert.Equal(2, (int)MessagePriority.Low);
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Header extraction
    // ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public void GetPriority_NullHeaders_ReturnsNormal()
    {
        Dictionary<string, byte[]>? headers = null;
        Assert.Equal(MessagePriority.Normal, headers.GetPriority());
    }

    [Fact]
    public void GetPriority_EmptyHeaders_ReturnsNormal()
    {
        var headers = new Dictionary<string, byte[]>();
        Assert.Equal(MessagePriority.Normal, headers.GetPriority());
    }

    [Theory]
    [InlineData("high",   MessagePriority.High)]
    [InlineData("normal", MessagePriority.Normal)]
    [InlineData("low",    MessagePriority.Low)]
    public void GetPriority_KnownHeaderValue_ReturnsPriority(string value, MessagePriority expected)
    {
        var headers = new Dictionary<string, byte[]>
        {
            [MessagePriorityExtensions.HeaderKey] = Encoding.UTF8.GetBytes(value)
        };
        Assert.Equal(expected, headers.GetPriority());
    }

    [Fact]
    public void GetPriority_UnknownHeaderValue_ReturnsNormal()
    {
        var headers = new Dictionary<string, byte[]>
        {
            [MessagePriorityExtensions.HeaderKey] = Encoding.UTF8.GetBytes("critical")
        };
        Assert.Equal(MessagePriority.Normal, headers.GetPriority());
    }

    [Fact]
    public void WithPriority_SetsHeaderOnNewDictionary()
    {
        var headers = ((Dictionary<string, byte[]>?)null).WithPriority(MessagePriority.High);

        Assert.True(headers.ContainsKey(MessagePriorityExtensions.HeaderKey));
        Assert.Equal(MessagePriority.High, headers.GetPriority());
    }

    [Fact]
    public void WithPriority_MergesExistingHeaders()
    {
        var existing = new Dictionary<string, byte[]> { ["x-custom"] = [1] };
        var headers  = existing.WithPriority(MessagePriority.Low);

        Assert.True(headers.ContainsKey("x-custom"));
        Assert.Equal(MessagePriority.Low, headers.GetPriority());
    }

    [Fact]
    public void WithPriority_DoesNotMutateOriginal()
    {
        var existing = new Dictionary<string, byte[]>();
        _ = existing.WithPriority(MessagePriority.High);
        Assert.Empty(existing);
    }

    // ──────────────────────────────────────────────────────────────────────────
    // PriorityPartitioner – partition range routing
    // ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public void PriorityPartitioner_DefaultOptions_ThreePartitions()
    {
        var partitioner = new PriorityPartitioner();
        Assert.Equal(3, partitioner.TotalPartitions);
    }

    [Fact]
    public void PriorityPartitioner_HighPriority_RoutesToFirstRange()
    {
        var partitioner = new PriorityPartitioner(new PriorityPartitionerOptions { PartitionsPerPriority = 2 });

        for (int i = 0; i < 20; i++)
        {
            var partition = partitioner.SelectPartitionForPriority(null, MessagePriority.High);
            Assert.InRange(partition, 0, 1); // partitions 0-1
        }
    }

    [Fact]
    public void PriorityPartitioner_NormalPriority_RoutesToMiddleRange()
    {
        var partitioner = new PriorityPartitioner(new PriorityPartitionerOptions { PartitionsPerPriority = 2 });

        for (int i = 0; i < 20; i++)
        {
            var partition = partitioner.SelectPartitionForPriority(null, MessagePriority.Normal);
            Assert.InRange(partition, 2, 3); // partitions 2-3
        }
    }

    [Fact]
    public void PriorityPartitioner_LowPriority_RoutesToLastRange()
    {
        var partitioner = new PriorityPartitioner(new PriorityPartitionerOptions { PartitionsPerPriority = 2 });

        for (int i = 0; i < 20; i++)
        {
            var partition = partitioner.SelectPartitionForPriority(null, MessagePriority.Low);
            Assert.InRange(partition, 4, 5); // partitions 4-5
        }
    }

    [Fact]
    public void PriorityPartitioner_SelectPartition_WithoutHeaders_DefaultsToNormal()
    {
        // IPartitionStrategy overload – no header context → Normal range
        var partitioner = new PriorityPartitioner(new PriorityPartitionerOptions { PartitionsPerPriority = 3 });

        for (int i = 0; i < 20; i++)
        {
            var partition = partitioner.SelectPartition(null, 9);
            // Normal range = partitions 3-5
            Assert.InRange(partition, 3, 5);
        }
    }

    [Fact]
    public void PriorityPartitioner_SelectPartition_WithHeaders_UsesHeaderPriority()
    {
        var partitioner = new PriorityPartitioner(new PriorityPartitionerOptions { PartitionsPerPriority = 1 });

        var highHeaders = ((Dictionary<string, byte[]>?)null).WithPriority(MessagePriority.High);
        Assert.Equal(0, partitioner.SelectPartition(null, highHeaders, 3));

        var normalHeaders = ((Dictionary<string, byte[]>?)null).WithPriority(MessagePriority.Normal);
        Assert.Equal(1, partitioner.SelectPartition(null, normalHeaders, 3));

        var lowHeaders = ((Dictionary<string, byte[]>?)null).WithPriority(MessagePriority.Low);
        Assert.Equal(2, partitioner.SelectPartition(null, lowHeaders, 3));
    }

    [Fact]
    public void PriorityPartitioner_PartitionsPerPriority_ConfiguresRangeStart()
    {
        const int p = 4;
        var partitioner = new PriorityPartitioner(new PriorityPartitionerOptions { PartitionsPerPriority = p });

        Assert.Equal(0,      partitioner.GetPartitionRangeStart(MessagePriority.High));
        Assert.Equal(p,      partitioner.GetPartitionRangeStart(MessagePriority.Normal));
        Assert.Equal(p * 2,  partitioner.GetPartitionRangeStart(MessagePriority.Low));
        Assert.Equal(p * 3,  partitioner.TotalPartitions);
    }

    [Fact]
    public void PriorityPartitioner_ZeroPartitionsPerPriority_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new PriorityPartitioner(new PriorityPartitionerOptions { PartitionsPerPriority = 0 }));
    }

    // ──────────────────────────────────────────────────────────────────────────
    // PriorityConsumerConfig
    // ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public void PriorityConsumerConfig_DefaultWeights()
    {
        var config = new PriorityConsumerConfig();

        Assert.Equal(PriorityConsumerConfig.DefaultHighWeight,   config.HighWeight);
        Assert.Equal(PriorityConsumerConfig.DefaultNormalWeight, config.NormalWeight);
        Assert.Equal(PriorityConsumerConfig.DefaultLowWeight,    config.LowWeight);
    }

    [Fact]
    public void PriorityConsumerConfig_BuildPollSchedule_RespectsWeights()
    {
        var config = new PriorityConsumerConfig
        {
            HighWeight   = 3,
            NormalWeight = 2,
            LowWeight    = 1
        };

        var schedule = config.BuildPollSchedule().ToList();

        Assert.Equal(6, schedule.Count);
        Assert.Equal(3, schedule.Count(p => p == MessagePriority.High));
        Assert.Equal(2, schedule.Count(p => p == MessagePriority.Normal));
        Assert.Equal(1, schedule.Count(p => p == MessagePriority.Low));
    }

    [Fact]
    public void PriorityConsumerConfig_BuildPollSchedule_OrderIsHighFirst()
    {
        var config   = new PriorityConsumerConfig { HighWeight = 2, NormalWeight = 1, LowWeight = 1 };
        var schedule = config.BuildPollSchedule().ToList();

        // High entries appear before Normal and Low entries
        var firstNormal = schedule.IndexOf(MessagePriority.Normal);
        var firstLow    = schedule.IndexOf(MessagePriority.Low);
        Assert.True(firstNormal > 1);  // after the 2 High entries
        Assert.True(firstLow > firstNormal);
    }

    [Fact]
    public void PriorityConsumerConfig_GetPartitionsForPriority_ReturnsCorrectIndices()
    {
        var config = new PriorityConsumerConfig { PartitionsPerPriority = 3 };

        var high   = config.GetPartitionsForPriority(MessagePriority.High).ToList();
        var normal = config.GetPartitionsForPriority(MessagePriority.Normal).ToList();
        var low    = config.GetPartitionsForPriority(MessagePriority.Low).ToList();

        Assert.Equal([0, 1, 2], high);
        Assert.Equal([3, 4, 5], normal);
        Assert.Equal([6, 7, 8], low);
    }

    [Fact]
    public void PriorityConsumerConfig_TotalPartitions_IsThreeTimesPerPriority()
    {
        var config = new PriorityConsumerConfig { PartitionsPerPriority = 5 };
        Assert.Equal(15, config.TotalPartitions);
    }

    [Fact]
    public void PriorityConsumerConfig_Validate_InvalidWeight_ReturnsErrors()
    {
        var config = new PriorityConsumerConfig { HighWeight = 0 };
        var errors = config.Validate();
        Assert.NotEmpty(errors);
        Assert.Contains(errors, e => e.Contains(nameof(PriorityConsumerConfig.HighWeight)));
    }

    [Fact]
    public void PriorityConsumerConfig_Validate_ValidConfig_ReturnsNoErrors()
    {
        var config = new PriorityConsumerConfig
        {
            HighWeight         = 5,
            NormalWeight       = 3,
            LowWeight          = 1,
            PartitionsPerPriority = 4
        };
        Assert.Empty(config.Validate());
    }
}
